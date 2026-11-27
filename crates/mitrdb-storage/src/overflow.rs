use mitrdb_common::{Error, INVALID_LSN, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};

use crate::buffer_pool::BufferPoolManager;
use crate::crc32c;
use crate::btree::NODE_KIND_FREE;
use crate::page::{PAGE_FLAG_FREE, PAGE_TYPE_OVERFLOW, PAGE_TYPE_RAW};
use crate::wal::{LogRecord, WalManager};

pub const NODE_KIND_OVERFLOW: u8 = 4;
pub const INLINE_BLOB_MAX_BYTES: usize = 2048;
pub const MAX_OVERFLOW_PAYLOAD_BYTES: usize = 16 * 1024;
pub const OVERFLOW_DESCRIPTOR_LEN: usize = 16;
pub const MAX_INLINE_TUPLE_BYTES: usize = 3800;

pub const TUPLE_TAG_NULL: u8 = 0;
pub const TUPLE_TAG_INT64: u8 = 1;
pub const TUPLE_TAG_TEXT: u8 = 2;
pub const TUPLE_TAG_BOOL: u8 = 3;
pub const TUPLE_TAG_BYTES: u8 = 4;
pub const TUPLE_TAG_FLOAT: u8 = 5;
pub const TUPLE_TAG_OVERFLOW_TEXT: u8 = 6;
pub const TUPLE_TAG_OVERFLOW_BYTES: u8 = 7;
pub const TUPLE_TAG_OVERFLOW_TUPLE: u8 = 8;

const OVERFLOW_PAGE_HEADER_LEN: usize = 8;
pub const OVERFLOW_CHUNK_CAPACITY: usize = PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_PAGE_HEADER_LEN;
const MAX_OVERFLOW_CHAIN_PAGES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowDescriptor {
    pub head_page_id: u64,
    pub total_len: u32,
    pub payload_crc32c: u32,
}

impl OverflowDescriptor {
    pub fn to_bytes(&self) -> [u8; OVERFLOW_DESCRIPTOR_LEN] {
        let mut out = [0u8; OVERFLOW_DESCRIPTOR_LEN];
        out[0..8].copy_from_slice(&self.head_page_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.total_len.to_le_bytes());
        out[12..16].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        out
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < OVERFLOW_DESCRIPTOR_LEN {
            return Err(Error::PageCorrupted(
                "Truncated overflow descriptor".to_string(),
            ));
        }
        let head_page_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let total_len = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let payload_crc32c = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        Ok(Self {
            head_page_id,
            total_len,
            payload_crc32c,
        })
    }
}

pub fn write_overflow_chain(
    bpm: &BufferPoolManager,
    wal: Option<&WalManager>,
    txn_id: TxnId,
    last_lsn: &mut Lsn,
    payload: &[u8],
) -> Result<OverflowDescriptor> {
    if payload.is_empty() {
        return Err(Error::PageCorrupted(
            "Empty payload cannot use overflow chain".to_string(),
        ));
    }
    if payload.len() > MAX_OVERFLOW_PAYLOAD_BYTES {
        return Err(Error::OutOfSpace);
    }

    let chunks: Vec<&[u8]> = payload.chunks(OVERFLOW_CHUNK_CAPACITY).collect();
    let mut pids = Vec::with_capacity(chunks.len());
    for _ in 0..chunks.len() {
        match bpm.new_page(PAGE_TYPE_OVERFLOW) {
            Ok((pid, _)) => pids.push(pid),
            Err(e) => {
                for &alloc_pid in pids.iter().rev() {
                    let _ = bpm.unpin_page(alloc_pid, false);
                    let _ = bpm.free_page(alloc_pid);
                }
                return Err(e);
            }
        }
    }

    let body_len = PAGE_SIZE - PAGE_HEADER_SIZE;
    let zero_undo = vec![0u8; body_len];

    for idx in (0..chunks.len()).rev() {
        let pid = pids[idx];
        let next_pid: PageId = if idx + 1 < pids.len() {
            pids[idx + 1]
        } else {
            0
        };
        let chunk = chunks[idx];

        let mut redo_body = vec![0u8; body_len];
        redo_body[0] = NODE_KIND_OVERFLOW;
        redo_body[1] = 0;
        redo_body[2..4].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
        redo_body[4..8].copy_from_slice(&next_pid.to_le_bytes());
        redo_body[OVERFLOW_PAGE_HEADER_LEN..OVERFLOW_PAGE_HEADER_LEN + chunk.len()]
            .copy_from_slice(chunk);

        let lsn = if let Some(wal) = wal {
            let rec = LogRecord::new_update(
                0,
                txn_id,
                *last_lsn,
                pid,
                PAGE_HEADER_SIZE as u16,
                redo_body.clone(),
                zero_undo.clone(),
            );
            let l = wal.append_record(rec)?;
            *last_lsn = l;
            l
        } else {
            INVALID_LSN
        };

        let update_res = bpm.update_page_data(pid, |p| {
            p.set_page_type(PAGE_TYPE_OVERFLOW);
            p.set_flags(0);
            p.data[PAGE_HEADER_SIZE..PAGE_SIZE].copy_from_slice(&redo_body);
            if lsn != INVALID_LSN {
                p.set_page_lsn(lsn);
            } else {
                p.update_checksum();
            }
            Ok(())
        });
        let _ = bpm.unpin_page(pid, update_res.is_ok());
        update_res?;
    }

    Ok(OverflowDescriptor {
        head_page_id: pids[0] as u64,
        total_len: payload.len() as u32,
        payload_crc32c: crc32c::checksum(payload),
    })
}

pub fn read_overflow_chain(
    bpm: &BufferPoolManager,
    desc: &OverflowDescriptor,
) -> Result<Vec<u8>> {
    let total_len = desc.total_len as usize;
    if total_len == 0 || total_len > MAX_OVERFLOW_PAYLOAD_BYTES {
        return Err(Error::PageCorrupted(format!(
            "Invalid overflow payload length: {}",
            total_len
        )));
    }

    let mut curr_pid = u32::try_from(desc.head_page_id)
        .map_err(|_| Error::PageCorrupted("Invalid overflow head_page_id".to_string()))?;
    if curr_pid == 0 {
        return Err(Error::PageCorrupted(
            "Overflow head_page_id cannot be 0".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(total_len);
    let mut visited = 0usize;

    while curr_pid != 0 {
        visited += 1;
        if visited > MAX_OVERFLOW_CHAIN_PAGES {
            return Err(Error::PageCorrupted(
                "Overflow chain exceeded max page hop limit".to_string(),
            ));
        }

        let page = bpm.fetch_page(curr_pid)?;
        let res = (|| -> Result<PageId> {
            if page.page_type() != PAGE_TYPE_OVERFLOW || (page.flags() & PAGE_FLAG_FREE) != 0 {
                return Err(Error::PageCorrupted(format!(
                    "Page {} is not an active overflow page",
                    curr_pid
                )));
            }
            let body = &page.data[PAGE_HEADER_SIZE..PAGE_SIZE];
            if body[0] != NODE_KIND_OVERFLOW {
                return Err(Error::PageCorrupted(format!(
                    "Invalid overflow node kind {} on page {}",
                    body[0], curr_pid
                )));
            }
            let chunk_len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;
            let next_pid = u32::from_le_bytes(body[4..8].try_into().unwrap());
            if chunk_len == 0
                || chunk_len > OVERFLOW_CHUNK_CAPACITY
                || out.len() + chunk_len > total_len
            {
                return Err(Error::PageCorrupted(format!(
                    "Corrupt overflow chunk length {} on page {}",
                    chunk_len, curr_pid
                )));
            }
            out.extend_from_slice(
                &body[OVERFLOW_PAGE_HEADER_LEN..OVERFLOW_PAGE_HEADER_LEN + chunk_len],
            );
            Ok(next_pid)
        })();
        bpm.unpin_page(curr_pid, false)?;
        curr_pid = res?;
    }

    if out.len() != total_len {
        return Err(Error::PageCorrupted(format!(
            "Overflow length mismatch: expected {}, got {}",
            total_len,
            out.len()
        )));
    }
    let actual_crc = crc32c::checksum(&out);
    if actual_crc != desc.payload_crc32c {
        return Err(Error::PageCorrupted(format!(
            "Overflow payload CRC mismatch: expected {:#x}, got {:#x}",
            desc.payload_crc32c, actual_crc
        )));
    }
    Ok(out)
}

pub fn collect_overflow_chain_pages(
    bpm: &BufferPoolManager,
    head_page_id: PageId,
) -> Result<Vec<PageId>> {
    let mut curr_pid = head_page_id;
    let mut pages = Vec::new();
    while curr_pid != 0 {
        if pages.len() >= MAX_OVERFLOW_CHAIN_PAGES {
            return Err(Error::PageCorrupted(
                "Overflow chain exceeded max page hop limit".to_string(),
            ));
        }
        let page = bpm.fetch_page(curr_pid)?;
        let next_res = (|| -> Result<PageId> {
            if page.page_type() != PAGE_TYPE_OVERFLOW || (page.flags() & PAGE_FLAG_FREE) != 0 {
                return Err(Error::PageCorrupted(format!(
                    "Page {} is not an active overflow page",
                    curr_pid
                )));
            }
            let body = &page.data[PAGE_HEADER_SIZE..PAGE_SIZE];
            if body[0] != NODE_KIND_OVERFLOW {
                return Err(Error::PageCorrupted(format!(
                    "Invalid overflow node kind {} on page {}",
                    body[0], curr_pid
                )));
            }
            Ok(u32::from_le_bytes(body[4..8].try_into().unwrap()))
        })();
        bpm.unpin_page(curr_pid, false)?;
        let next_pid = next_res?;
        pages.push(curr_pid);
        curr_pid = next_pid;
    }
    Ok(pages)
}

pub fn stage_free_overflow_chain(
    bpm: &BufferPoolManager,
    txn_id: TxnId,
    head_page_id: PageId,
) -> Result<usize> {
    let pages = collect_overflow_chain_pages(bpm, head_page_id)?;
    let count = pages.len();
    for pid in pages {
        if bpm.fetch_page(pid).is_ok() {
            let _ = bpm.update_page_data(pid, |page| {
                page.set_page_type(PAGE_TYPE_RAW);
                page.set_flags(PAGE_FLAG_FREE);
                page.data[PAGE_HEADER_SIZE] = NODE_KIND_FREE;
                page.update_checksum();
                Ok(())
            });
            let _ = bpm.unpin_page(pid, true);
        }
        bpm.stage_free_page(txn_id, pid)?;
    }
    Ok(count)
}

/// Scans an encoded tuple buffer and extracts all overflow chain `head_page_id`s
/// referenced by `TUPLE_TAG_OVERFLOW_TEXT`, `TUPLE_TAG_OVERFLOW_BYTES`, or
/// `TUPLE_TAG_OVERFLOW_TUPLE`.
pub fn collect_tuple_overflow_heads(buf: &[u8]) -> Vec<PageId> {
    let mut heads = Vec::new();
    if buf.len() < 2 {
        return heads;
    }
    let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let mut pos = 2usize;
    for _ in 0..count {
        if pos >= buf.len() {
            break;
        }
        let tag = buf[pos];
        pos += 1;
        match tag {
            TUPLE_TAG_NULL => {}
            TUPLE_TAG_BOOL => {
                if pos + 1 > buf.len() {
                    break;
                }
                pos += 1;
            }
            TUPLE_TAG_INT64 | TUPLE_TAG_FLOAT => {
                if pos + 8 > buf.len() {
                    break;
                }
                pos += 8;
            }
            TUPLE_TAG_TEXT | TUPLE_TAG_BYTES => {
                if pos + 2 > buf.len() {
                    break;
                }
                let len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                pos += 2;
                if pos + len > buf.len() {
                    break;
                }
                pos += len;
            }
            TUPLE_TAG_OVERFLOW_TEXT | TUPLE_TAG_OVERFLOW_BYTES | TUPLE_TAG_OVERFLOW_TUPLE => {
                if pos + OVERFLOW_DESCRIPTOR_LEN > buf.len() {
                    break;
                }
                if let Ok(desc) = OverflowDescriptor::from_bytes(&buf[pos..pos + OVERFLOW_DESCRIPTOR_LEN])
                    && let Ok(pid) = u32::try_from(desc.head_page_id)
                    && pid != 0
                {
                    heads.push(pid);
                }
                pos += OVERFLOW_DESCRIPTOR_LEN;
                if tag == TUPLE_TAG_OVERFLOW_TUPLE {
                    break;
                }
            }
            _ => break,
        }
    }
    heads
}
