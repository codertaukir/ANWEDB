//! Streaming BLOB writer: exact-fit 4 KB overflow pages, one page buffered at a time
//! (flat memory), every page WAL-logged (full body redo + all-zero undo, which recovery
//! turns into a free page for losers), plus a 4-byte `next_page_id` patch record linking
//! the previous page. Rollback is the caller's job: use `BlobFinished::pages` / `abandon`.

use std::io::{self, Write};
use std::sync::Arc;

use mitrdb_common::{Error, INVALID_LSN, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};

use crate::buffer_pool::BufferPoolManager;
use crate::crc32c::Crc32cHasher;
use crate::overflow::{NODE_KIND_OVERFLOW, OVERFLOW_CHUNK_CAPACITY, OverflowDescriptorV2};
use crate::page::PAGE_TYPE_OVERFLOW;
use crate::wal::{LogRecord, WalManager};

const BODY_LEN: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
const BODY_HEADER_LEN: usize = 8;
const NEXT_OFFSET: usize = PAGE_HEADER_SIZE + 4;
const _: () = assert!(OVERFLOW_CHUNK_CAPACITY == BODY_LEN - BODY_HEADER_LEN);

pub struct BlobFinished {
    pub descriptor: OverflowDescriptorV2,
    /// Every page allocated for this blob, in chain order (4 bytes each in memory).
    pub pages: Vec<PageId>,
    pub last_lsn: Lsn,
}

pub struct BlobWriter {
    bpm: Arc<BufferPoolManager>,
    wal: Option<Arc<WalManager>>,
    txn_id: TxnId,
    last_lsn: Lsn,
    buf: Vec<u8>,
    pages: Vec<PageId>,
    crc: Crc32cHasher,
    total: u64,
    max_len: u64,
    failed: bool,
}

fn to_io(e: Error) -> io::Error {
    io::Error::other(e.to_string())
}

impl BlobWriter {
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        wal: Option<Arc<WalManager>>,
        txn_id: TxnId,
        start_lsn: Lsn,
        max_len: u64,
    ) -> Self {
        Self {
            bpm,
            wal,
            txn_id,
            last_lsn: start_lsn,
            buf: Vec::with_capacity(OVERFLOW_CHUNK_CAPACITY),
            pages: Vec::new(),
            crc: Crc32cHasher::new(),
            total: 0,
            max_len,
            failed: false,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.total
    }

    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn
    }

    pub fn pages(&self) -> &[PageId] {
        &self.pages
    }

    /// Give up: returns every allocated page so the caller can free them.
    pub fn abandon(self) -> Vec<PageId> {
        self.pages
    }

    /// Flush the last partial page. An empty blob returns head 0, len 0, crc 0.
    pub fn finish(mut self) -> Result<BlobFinished> {
        if self.failed {
            return Err(Error::Io("blob writer already failed".to_string()));
        }
        if !self.buf.is_empty() {
            self.flush_page()?;
        }
        let descriptor = OverflowDescriptorV2 {
            head_page_id: self.pages.first().copied().map_or(0, u64::from),
            total_len: self.total,
            payload_crc32c: self.crc.finalize(),
        };
        Ok(BlobFinished {
            descriptor,
            pages: self.pages,
            last_lsn: self.last_lsn,
        })
    }

    fn log(&mut self, page_id: PageId, offset: u16, redo: Vec<u8>, undo: Vec<u8>) -> Result<Lsn> {
        let Some(wal) = &self.wal else {
            return Ok(INVALID_LSN);
        };
        let rec = LogRecord::new_update(
            INVALID_LSN,
            self.txn_id,
            self.last_lsn,
            page_id,
            offset,
            redo,
            undo,
        );
        let lsn = wal.append_record(rec)?;
        self.last_lsn = lsn;
        Ok(lsn)
    }

    fn flush_page(&mut self) -> Result<()> {
        let (pid, _) = self.bpm.new_page(PAGE_TYPE_OVERFLOW)?;
        if pid == 0 {
            let _ = self.bpm.unpin_page(pid, false);
            return Err(Error::Io(
                "page id 0 is reserved as the chain terminator".to_string(),
            ));
        }
        // Track first: from here on rollback must be able to free this page.
        let prev = self.pages.last().copied();
        self.pages.push(pid);

        let mut body = vec![0u8; BODY_LEN];
        body[0] = NODE_KIND_OVERFLOW;
        body[2..4].copy_from_slice(&(self.buf.len() as u16).to_le_bytes());
        // next_page_id stays 0 until the following page is allocated (patched below)
        body[BODY_HEADER_LEN..BODY_HEADER_LEN + self.buf.len()].copy_from_slice(&self.buf);

        let logged = self.log(
            pid,
            PAGE_HEADER_SIZE as u16,
            body.clone(),
            vec![0u8; BODY_LEN],
        );
        let lsn = match logged {
            Ok(l) => l,
            Err(e) => {
                let _ = self.bpm.unpin_page(pid, false);
                return Err(e);
            }
        };
        let applied = self.bpm.update_page_data(pid, |p| {
            p.data[PAGE_HEADER_SIZE..].copy_from_slice(&body);
            if lsn != INVALID_LSN {
                p.set_page_lsn(lsn);
            } else {
                p.update_checksum();
            }
            Ok(())
        });
        let _ = self.bpm.unpin_page(pid, applied.is_ok());
        applied?;

        if let Some(prev_pid) = prev {
            self.patch_next(prev_pid, pid)?;
        }
        self.buf.clear();
        Ok(())
    }

    fn patch_next(&mut self, prev_pid: PageId, next_pid: PageId) -> Result<()> {
        let lsn = self.log(
            prev_pid,
            NEXT_OFFSET as u16,
            next_pid.to_le_bytes().to_vec(),
            0u32.to_le_bytes().to_vec(),
        )?;
        self.bpm.update_page_data(prev_pid, |p| {
            p.data[NEXT_OFFSET..NEXT_OFFSET + 4].copy_from_slice(&next_pid.to_le_bytes());
            if lsn != INVALID_LSN {
                p.set_page_lsn(lsn);
            } else {
                p.update_checksum();
            }
            Ok(())
        })
    }
}

impl Write for BlobWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("blob writer already failed"));
        }
        if data.is_empty() {
            return Ok(0);
        }
        if (data.len() as u64) > self.max_len.saturating_sub(self.total) {
            self.failed = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "blob exceeds configured max length",
            ));
        }
        let mut rest = data;
        while !rest.is_empty() {
            let take = (OVERFLOW_CHUNK_CAPACITY - self.buf.len()).min(rest.len());
            self.buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.buf.len() == OVERFLOW_CHUNK_CAPACITY {
                if let Err(e) = self.flush_page() {
                    self.failed = true;
                    return Err(to_io(e));
                }
            }
        }
        self.crc.update(data);
        self.total += data.len() as u64;
        Ok(data.len())
    }

    /// Does not force the partial page out; `finish` does that.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc32c;
    use crate::disk::DiskManager;
    use crate::page::PAGE_TYPE_RAW;

    fn env(name: &str, pool: usize) -> (std::path::PathBuf, Arc<BufferPoolManager>) {
        let dir = std::env::temp_dir().join(format!("mitrdb_blob_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk, pool));
        // page 0 is reserved in a real database (0 = end of chain)
        let (p0, _) = bpm.new_page(PAGE_TYPE_RAW).unwrap();
        bpm.unpin_page(p0, true).unwrap();
        assert_eq!(p0, 0);
        (dir, bpm)
    }

    /// Follow the chain page by page (test oracle; BlobReader replaces this in A3).
    fn walk(bpm: &BufferPoolManager, head: u64) -> (Vec<u8>, usize) {
        let mut out = Vec::new();
        let mut pid = head as PageId;
        let mut pages = 0;
        while pid != 0 {
            let page = bpm.fetch_page(pid).unwrap();
            bpm.unpin_page(pid, false).unwrap();
            let body = &page.data[PAGE_HEADER_SIZE..];
            assert_eq!(body[0], NODE_KIND_OVERFLOW);
            let len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;
            assert!(len > 0 && len <= OVERFLOW_CHUNK_CAPACITY);
            out.extend_from_slice(&body[BODY_HEADER_LEN..BODY_HEADER_LEN + len]);
            pid = u32::from_le_bytes(body[4..8].try_into().unwrap());
            pages += 1;
            assert!(pages < 1_000_000, "chain loop");
        }
        (out, pages)
    }

    fn pattern(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn exact_fit_page_count_and_crc() {
        let (dir, bpm) = env("exact", 64);
        let payload = pattern(10_000, 7);
        let mut w = BlobWriter::new(bpm.clone(), None, 1, INVALID_LSN, u64::MAX);
        for c in payload.chunks(777) {
            w.write_all(c).unwrap();
        }
        let fin = w.finish().unwrap();
        let want_pages = payload.len().div_ceil(OVERFLOW_CHUNK_CAPACITY);
        assert_eq!(fin.pages.len(), want_pages);
        assert_eq!(fin.descriptor.total_len, 10_000);
        assert_eq!(fin.descriptor.payload_crc32c, crc32c::checksum(&payload));
        assert_eq!(fin.descriptor.head_page_id, u64::from(fin.pages[0]));
        let (got, n) = walk(&bpm, fin.descriptor.head_page_id);
        assert_eq!(n, want_pages);
        assert_eq!(got, payload);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn exact_multiple_has_no_trailing_empty_page() {
        let (dir, bpm) = env("multiple", 64);
        let payload = pattern(2 * OVERFLOW_CHUNK_CAPACITY, 3);
        let mut w = BlobWriter::new(bpm.clone(), None, 1, INVALID_LSN, u64::MAX);
        w.write_all(&payload).unwrap();
        let fin = w.finish().unwrap();
        assert_eq!(fin.pages.len(), 2);
        let (got, n) = walk(&bpm, fin.descriptor.head_page_id);
        assert_eq!((n, got), (2, payload));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn empty_blob_has_no_pages() {
        let (dir, bpm) = env("empty", 8);
        let w = BlobWriter::new(bpm, None, 1, INVALID_LSN, u64::MAX);
        let fin = w.finish().unwrap();
        assert!(fin.pages.is_empty());
        assert_eq!(fin.descriptor.head_page_id, 0);
        assert_eq!(fin.descriptor.total_len, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn max_len_enforced_and_writer_stays_failed() {
        let (dir, bpm) = env("maxlen", 8);
        let mut w = BlobWriter::new(bpm, None, 1, INVALID_LSN, 100);
        w.write_all(&[1u8; 100]).unwrap();
        assert!(w.write(&[1u8]).is_err());
        assert!(w.write(&[1u8]).is_err());
        assert!(w.finish().is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn four_mb_through_tiny_pool_in_64k_chunks() {
        // pool of 32 frames (128 KB) << 4 MB payload: proves pages are written through,
        // not accumulated in memory.
        let (dir, bpm) = env("4mb", 32);
        let payload = pattern(4 * 1024 * 1024, 11);
        let mut w = BlobWriter::new(bpm.clone(), None, 1, INVALID_LSN, u64::MAX);
        for c in payload.chunks(64 * 1024) {
            w.write_all(c).unwrap();
        }
        let fin = w.finish().unwrap();
        assert_eq!(
            fin.pages.len(),
            payload.len().div_ceil(OVERFLOW_CHUNK_CAPACITY)
        );
        assert_eq!(fin.descriptor.payload_crc32c, crc32c::checksum(&payload));
        let (got, _) = walk(&bpm, fin.descriptor.head_page_id);
        assert!(got == payload, "payload mismatch");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn wal_logged_chain_advances_lsn() {
        let (dir, bpm) = env("wal", 64);
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal")).unwrap());
        let payload = pattern(100_000, 5);
        let mut w = BlobWriter::new(bpm.clone(), Some(wal), 7, INVALID_LSN, u64::MAX);
        w.write_all(&payload).unwrap();
        let fin = w.finish().unwrap();
        assert_ne!(fin.last_lsn, INVALID_LSN);
        let (got, n) = walk(&bpm, fin.descriptor.head_page_id);
        assert_eq!(n, fin.pages.len());
        assert_eq!(got, payload);
        let _ = std::fs::remove_dir_all(dir);
    }
}
