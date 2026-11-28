//! Streaming BLOB writer: exact-fit 4 KB overflow pages, one page buffered at a time
//! (flat memory), every page WAL-logged (full body redo + all-zero undo, which recovery
//! turns into a free page for losers), plus a 4-byte `next_page_id` patch record linking
//! the previous page. Rollback is the caller's job: use `BlobFinished::pages` / `abandon`.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use mitrdb_common::{Error, INVALID_LSN, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};

use crate::buffer_pool::BufferPoolManager;
use crate::crc32c::Crc32cHasher;
use crate::overflow::{NODE_KIND_OVERFLOW, OVERFLOW_CHUNK_CAPACITY, OverflowDescriptorV2};
use crate::page::{PAGE_FLAG_FREE, PAGE_TYPE_OVERFLOW};
use crate::wal::{LogRecord, WalManager};

/// Default streaming BLOB max: 128 MiB. Configurable via Server::set_max_blob_bytes.
pub const DEFAULT_MAX_BLOB_BYTES: u64 = 128 * 1024 * 1024;

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
    start_lsn: Lsn,
    tracker: Option<Arc<Mutex<Vec<PageId>>>>,
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
            start_lsn,
            tracker: None,
        }
    }

    /// Share the list of allocated pages with the transaction, so rollback / abort can
    /// free them even if the writer is dropped mid-stream.
    pub fn with_tracker(mut self, tracker: Arc<Mutex<Vec<PageId>>>) -> Self {
        self.tracker = Some(tracker);
        self
    }

    pub fn start_lsn(&self) -> Lsn {
        self.start_lsn
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

    /// Like `finish` but keeps the writer (and its `last_lsn`) readable afterwards, even
    /// when the last page fails. Call it once.
    pub fn finish_descriptor(&mut self) -> Result<OverflowDescriptorV2> {
        if self.failed {
            return Err(Error::Io("blob writer already failed".to_string()));
        }
        if !self.buf.is_empty() {
            if let Err(e) = self.flush_page() {
                self.failed = true;
                return Err(e);
            }
        }
        let crc = std::mem::take(&mut self.crc).finalize();
        Ok(OverflowDescriptorV2 {
            head_page_id: self.pages.first().copied().map_or(0, u64::from),
            total_len: self.total,
            payload_crc32c: crc,
        })
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
        if let Some(t) = &self.tracker {
            t.lock().unwrap_or_else(|e| e.into_inner()).push(pid);
        }

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

/// Mark blob pages free and stage them for reuse (same effect as `stage_free_overflow_chain`,
/// but from an explicit page list: no chain walk, no hop limit, works for half-written chains).
pub fn free_blob_pages(bpm: &BufferPoolManager, txn_id: TxnId, pages: &[PageId]) -> Result<usize> {
    let mut freed = 0;
    for &pid in pages {
        if pid == 0 {
            continue;
        }
        if bpm.fetch_page(pid).is_ok() {
            let _ = bpm.update_page_data(pid, |page| {
                page.set_page_type(crate::page::PAGE_TYPE_RAW);
                page.set_flags(crate::page::PAGE_FLAG_FREE);
                page.data[PAGE_HEADER_SIZE] = crate::btree::NODE_KIND_FREE;
                page.update_checksum();
                Ok(())
            });
            let _ = bpm.unpin_page(pid, true);
        }
        bpm.stage_free_page(txn_id, pid)?;
        freed += 1;
    }
    Ok(freed)
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
                io::ErrorKind::StorageFull,
                "OUT_OF_SPACE: blob exceeds configured max length",
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

const CAP64: u64 = OVERFLOW_CHUNK_CAPACITY as u64;
/// Every 64th page id of the chain is remembered so a seek walks at most 63 hops.
const STRIDE: u64 = 64;
/// Upper bound for one `read_range` call (the caller chunks big reads, e.g. 64 KB frames).
pub const MAX_READ_RANGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
struct Cursor {
    idx: u64,
    pid: PageId,
    next: PageId,
}

fn corrupt(msg: String) -> Error {
    Error::PageCorrupted(msg)
}

fn to_io_read(e: Error) -> io::Error {
    match e {
        Error::PageCorrupted(m) => io::Error::new(io::ErrorKind::InvalidData, m),
        other => io::Error::other(other.to_string()),
    }
}

/// Streaming reader over an overflow chain written by `BlobWriter` (or the old chain
/// writer: same page layout). No chain length limit; memory is one page plus a small
/// sparse index. Sequential reads from offset 0 verify the end-to-end CRC32C at EOF.
pub struct BlobReader {
    bpm: Arc<BufferPoolManager>,
    desc: OverflowDescriptorV2,
    pos: u64,
    cur: Option<Cursor>,
    stride: Vec<PageId>,
    crc: Crc32cHasher,
    crc_pos: u64,
    crc_valid: bool,
}

impl BlobReader {
    pub fn open(bpm: Arc<BufferPoolManager>, desc: OverflowDescriptorV2) -> Result<Self> {
        if desc.total_len == 0 {
            if desc.head_page_id != 0 {
                return Err(corrupt("empty blob with a head page".to_string()));
            }
        } else if desc.head_page_id == 0 || desc.head_page_id > u64::from(u32::MAX) {
            return Err(corrupt(format!(
                "invalid blob head page {}",
                desc.head_page_id
            )));
        }
        let stride = if desc.total_len > 0 {
            vec![desc.head_page_id as PageId]
        } else {
            Vec::new()
        };
        Ok(Self {
            bpm,
            desc,
            pos: 0,
            cur: None,
            stride,
            crc: Crc32cHasher::new(),
            crc_pos: 0,
            crc_valid: true,
        })
    }

    pub fn len(&self) -> u64 {
        self.desc.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.desc.total_len == 0
    }

    pub fn position(&self) -> u64 {
        self.pos
    }

    /// HTTP-style range read. `len` is clamped to the blob end; `offset > len()` is an error.
    pub fn read_range(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len > MAX_READ_RANGE_BYTES {
            return Err(Error::Io(format!(
                "range read of {len} bytes exceeds {MAX_READ_RANGE_BYTES}"
            )));
        }
        let total = self.desc.total_len;
        if offset > total {
            return Err(Error::Io("range offset beyond blob end".to_string()));
        }
        let n = (len as u64).min(total - offset) as usize;
        self.set_pos(offset);
        let mut buf = vec![0u8; n];
        let got = self.read_inner(&mut buf)?;
        buf.truncate(got);
        Ok(buf)
    }

    /// Stream the whole blob once (64 KB buffer) and check every page plus the CRC32C.
    pub fn verify(&mut self) -> Result<()> {
        self.set_pos(0);
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            if self.read_inner(&mut buf)? == 0 {
                break;
            }
        }
        if self.crc_valid {
            // only reachable if nothing was ever read (empty blob handled in read_inner)
            return Err(corrupt("blob CRC32C was not checked".to_string()));
        }
        Ok(())
    }

    fn page_count(&self) -> u64 {
        self.desc.total_len.div_ceil(CAP64)
    }

    fn expected_len(&self, idx: u64) -> usize {
        (self.desc.total_len - idx * CAP64).min(CAP64) as usize
    }

    fn set_pos(&mut self, new: u64) {
        if new == 0 {
            self.crc = Crc32cHasher::new();
            self.crc_pos = 0;
            self.crc_valid = true;
        } else if new != self.crc_pos {
            self.crc_valid = false;
        }
        self.pos = new;
    }

    fn note_stride(&mut self, idx: u64, pid: PageId) {
        if pid != 0 && idx.is_multiple_of(STRIDE) && (idx / STRIDE) as usize == self.stride.len() {
            self.stride.push(pid);
        }
    }

    /// Load chain page `idx`, validate it, run `f` on its payload, return `f`'s result and `next`.
    fn load<R>(&self, pid: PageId, idx: u64, f: impl FnOnce(&[u8]) -> R) -> Result<(R, PageId)> {
        let want = self.expected_len(idx);
        let last = idx + 1 == self.page_count();
        self.bpm.with_page(pid, |page| {
            if page.page_type() != PAGE_TYPE_OVERFLOW || (page.flags() & PAGE_FLAG_FREE) != 0 {
                return Err(corrupt(format!("blob page {pid} is not an overflow page")));
            }
            let body = &page.data[PAGE_HEADER_SIZE..];
            if body[0] != NODE_KIND_OVERFLOW {
                return Err(corrupt(format!("blob page {pid} has a wrong node kind")));
            }
            let len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;
            if len != want {
                return Err(corrupt(format!(
                    "blob page {pid} (index {idx}) holds {len} bytes, expected {want}"
                )));
            }
            let next = u32::from_le_bytes(body[4..8].try_into().unwrap());
            if last != (next == 0) {
                return Err(corrupt(format!(
                    "blob page {pid} (index {idx}) has a wrong next pointer"
                )));
            }
            Ok((f(&body[BODY_HEADER_LEN..BODY_HEADER_LEN + len]), next))
        })
    }

    fn locate(&mut self, idx: u64) -> Result<PageId> {
        if let Some(c) = self.cur {
            if c.idx == idx {
                return Ok(c.pid);
            }
            if c.idx + 1 == idx {
                return Ok(c.next);
            }
        }
        let k = ((idx / STRIDE) as usize).min(self.stride.len() - 1);
        let mut i = k as u64 * STRIDE;
        let mut pid = self.stride[k];
        if let Some(c) = self.cur
            && c.idx <= idx
            && c.idx >= i
        {
            i = c.idx;
            pid = c.pid;
        }
        while i < idx {
            let ((), next) = self.load(pid, i, |_| ())?;
            i += 1;
            pid = next;
            self.note_stride(i, pid);
        }
        Ok(pid)
    }

    fn read_inner(&mut self, out: &mut [u8]) -> Result<usize> {
        let total = self.desc.total_len;
        let start = self.pos;
        let mut done = 0usize;
        while done < out.len() && self.pos < total {
            let idx = self.pos / CAP64;
            let off = (self.pos % CAP64) as usize;
            let pid = self.locate(idx)?;
            let take = (out.len() - done).min(self.expected_len(idx) - off);
            let dst = &mut out[done..done + take];
            let ((), next) = self.load(pid, idx, |chunk| {
                dst.copy_from_slice(&chunk[off..off + take]);
            })?;
            self.cur = Some(Cursor { idx, pid, next });
            self.note_stride(idx + 1, next);
            done += take;
            self.pos += take as u64;
        }
        if done > 0 {
            if self.crc_valid && self.crc_pos == start {
                self.crc.update(&out[..done]);
                self.crc_pos += done as u64;
            } else {
                self.crc_valid = false;
            }
        }
        if self.pos == total && self.crc_valid && self.crc_pos == total {
            let h = std::mem::take(&mut self.crc);
            self.crc_valid = false;
            if h.finalize() != self.desc.payload_crc32c {
                return Err(corrupt("blob CRC32C mismatch".to_string()));
            }
        }
        Ok(done)
    }
}

impl Read for BlobReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.read_inner(out).map_err(to_io_read)
    }
}

impl Seek for BlobReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let total = i128::from(self.desc.total_len);
        let new = match from {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::End(d) => total + i128::from(d),
            SeekFrom::Current(d) => i128::from(self.pos) + i128::from(d),
        };
        if new < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start of the blob",
            ));
        }
        let new = u64::try_from(new)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "seek position too large"))?;
        self.set_pos(new);
        Ok(new)
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

#[cfg(test)]
mod reader_tests {
    use super::*;
    use crate::crc32c;
    use crate::disk::DiskManager;
    use crate::page::PAGE_TYPE_RAW;

    fn env(name: &str, pool: usize) -> (std::path::PathBuf, Arc<BufferPoolManager>) {
        let dir = std::env::temp_dir().join(format!("mitrdb_rd_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk, pool));
        // page 0 is reserved in a real database (0 = end of chain)
        let (p0, _) = bpm.new_page(PAGE_TYPE_RAW).unwrap();
        bpm.unpin_page(p0, true).unwrap();
        assert_eq!(p0, 0);
        (dir, bpm)
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

    fn make(bpm: &Arc<BufferPoolManager>, payload: &[u8]) -> BlobFinished {
        let mut w = BlobWriter::new(bpm.clone(), None, 1, INVALID_LSN, u64::MAX);
        for c in payload.chunks(64 * 1024) {
            w.write_all(c).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn sequential_read_roundtrip_verifies_crc() {
        let (dir, bpm) = env("seq", 32);
        let payload = pattern(1_000_000, 21);
        let fin = make(&bpm, &payload);
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        assert_eq!(r.len(), 1_000_000);
        let mut got = Vec::new();
        let mut buf = [0u8; 1000];
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        assert!(got == payload, "payload mismatch");
        assert_eq!(fin.descriptor.payload_crc32c, crc32c::checksum(&got));
        r.verify().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn random_range_reads_match_payload() {
        let (dir, bpm) = env("range", 32);
        let payload = pattern(2 * 1024 * 1024, 33);
        let fin = make(&bpm, &payload);
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..200 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let off = (x % payload.len() as u64) as usize;
            let len = ((x >> 32) % 70_000) as usize + 1;
            let want = &payload[off..(off + len).min(payload.len())];
            let got = r.read_range(off as u64, len).unwrap();
            assert!(got == want, "range {off}+{len}");
        }
        // page boundaries
        for k in 1..6usize {
            let off = k * OVERFLOW_CHUNK_CAPACITY;
            let got = r.read_range(off as u64 - 3, 10).unwrap();
            assert_eq!(got, &payload[off - 3..off + 7]);
        }
        // clamped at the end, empty at the end, error beyond it, size cap
        let tail = r.read_range(payload.len() as u64 - 5, 100).unwrap();
        assert_eq!(tail, &payload[payload.len() - 5..]);
        assert!(r.read_range(payload.len() as u64, 10).unwrap().is_empty());
        assert!(r.read_range(payload.len() as u64 + 1, 10).is_err());
        assert!(r.read_range(0, MAX_READ_RANGE_BYTES + 1).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn seek_variants() {
        let (dir, bpm) = env("seek", 32);
        let payload = pattern(50_000, 9);
        let fin = make(&bpm, &payload);
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        let mut b = [0u8; 32];
        assert_eq!(r.seek(SeekFrom::End(-10)).unwrap(), 49_990);
        assert_eq!(r.read(&mut b).unwrap(), 10);
        assert_eq!(&b[..10], &payload[49_990..]);
        assert_eq!(r.read(&mut b).unwrap(), 0);
        r.seek(SeekFrom::Start(5)).unwrap();
        assert_eq!(r.seek(SeekFrom::Current(10)).unwrap(), 15);
        assert_eq!(r.read(&mut b[..4]).unwrap(), 4);
        assert_eq!(&b[..4], &payload[15..19]);
        assert!(r.seek(SeekFrom::Current(-100)).is_err());
        assert_eq!(r.position(), 19);
        r.seek(SeekFrom::Start(1_000_000)).unwrap();
        assert_eq!(r.read(&mut b).unwrap(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn deep_seek_builds_stride_index() {
        let (dir, bpm) = env("deep", 32);
        let payload = pattern(3 * 1024 * 1024, 77); // ~776 pages
        let fin = make(&bpm, &payload);
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        let far = payload.len() as u64 - 5_000;
        let got = r.read_range(far, 4_000).unwrap();
        assert!(got == payload[far as usize..far as usize + 4_000]);
        assert!(r.stride.len() >= 12, "stride entries: {}", r.stride.len());
        let got = r.read_range(1_000, 4_000).unwrap();
        assert!(got == payload[1_000..5_000]);
        let mid = 1_700_000u64;
        let got = r.read_range(mid, 9_000).unwrap();
        assert!(got == payload[mid as usize..mid as usize + 9_000]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn flipped_payload_byte_fails_crc_but_other_ranges_read() {
        let (dir, bpm) = env("crc", 32);
        let payload = pattern(5 * OVERFLOW_CHUNK_CAPACITY, 4);
        let fin = make(&bpm, &payload);
        bpm.update_page_data(fin.pages[2], |p| {
            p.data[PAGE_HEADER_SIZE + BODY_HEADER_LEN + 100] ^= 0xFF;
            Ok(())
        })
        .unwrap();
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        let e = r.verify().unwrap_err();
        assert!(e.to_string().contains("CRC32C"), "{e}");
        assert_eq!(r.read_range(0, 1000).unwrap(), &payload[..1000]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn wrong_chunk_len_is_detected() {
        let (dir, bpm) = env("len", 32);
        let payload = pattern(3 * OVERFLOW_CHUNK_CAPACITY, 6);
        let fin = make(&bpm, &payload);
        bpm.update_page_data(fin.pages[1], |p| {
            p.data[PAGE_HEADER_SIZE + 2..PAGE_HEADER_SIZE + 4].copy_from_slice(&7u16.to_le_bytes());
            Ok(())
        })
        .unwrap();
        let mut r = BlobReader::open(bpm.clone(), fin.descriptor).unwrap();
        assert!(r.read_range(OVERFLOW_CHUNK_CAPACITY as u64, 10).is_err());
        let mut buf = vec![0u8; payload.len()];
        let e = r.seek(SeekFrom::Start(0)).and_then(|_| r.read_exact(&mut buf));
        assert_eq!(e.unwrap_err().kind(), io::ErrorKind::InvalidData);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn empty_blob_and_bad_descriptors() {
        let (dir, bpm) = env("empty", 8);
        let empty = OverflowDescriptorV2 {
            head_page_id: 0,
            total_len: 0,
            payload_crc32c: 0,
        };
        let mut r = BlobReader::open(bpm.clone(), empty).unwrap();
        assert!(r.is_empty());
        let mut b = [0u8; 8];
        assert_eq!(r.read(&mut b).unwrap(), 0);
        r.verify().unwrap();
        let bad = |head, total| OverflowDescriptorV2 {
            head_page_id: head,
            total_len: total,
            payload_crc32c: 0,
        };
        assert!(BlobReader::open(bpm.clone(), bad(0, 10)).is_err());
        assert!(BlobReader::open(bpm.clone(), bad(5, 0)).is_err());
        assert!(BlobReader::open(bpm.clone(), bad(u64::MAX, 10)).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
mod tracker_tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::page::{PAGE_FLAG_FREE, PAGE_TYPE_RAW};

    #[test]
    fn tracker_sees_every_page_and_free_marks_them() {
        let dir = std::env::temp_dir().join(format!("mitrdb_blob_trk_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk, 64));
        let (p0, _) = bpm.new_page(PAGE_TYPE_RAW).unwrap();
        bpm.unpin_page(p0, true).unwrap();
        assert_eq!(p0, 0);

        let tracker = Arc::new(Mutex::new(Vec::new()));
        let mut w = BlobWriter::new(bpm.clone(), None, 1, INVALID_LSN, u64::MAX)
            .with_tracker(tracker.clone());
        w.write_all(&vec![7u8; 20_000]).unwrap(); // 4 full pages flushed, rest buffered
        assert_eq!(tracker.lock().unwrap().as_slice(), w.pages());
        drop(w);
        let pages = tracker.lock().unwrap().clone();
        assert_eq!(pages.len(), 4);
        assert_eq!(free_blob_pages(&bpm, 1, &pages).unwrap(), 4);
        for pid in pages {
            let p = bpm.fetch_page(pid).unwrap();
            bpm.unpin_page(pid, false).unwrap();
            assert!(p.flags() & PAGE_FLAG_FREE != 0);
            assert_eq!(p.page_type(), PAGE_TYPE_RAW);
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
