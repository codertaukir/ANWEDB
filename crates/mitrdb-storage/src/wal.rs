use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, MutexGuard};

use std::os::unix::io::AsRawFd;

use mitrdb_common::{Error, INVALID_LSN, Lsn, MAGIC_BYTES, PageId, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;

use crate::crc32c::{self, Crc32cHasher};
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024; // 64 MB (High-throughput production standard)
pub const MIN_WAL_SEGMENT_BYTES: u64 = 64;
const CHECKPOINT_META_VERSION: u32 = 1;
const CHECKPOINT_META_SIZE: usize = 60;
const SYS_TXN_ID_MIN: TxnId = 1_000_000_000;
/// Fixed (non-payload) part of a serialized log record, excluding the trailing CRC.
const LOG_RECORD_FIXED_HEADER: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2 + 8 + 2 + 2;
/// Initial capacity of the pending-bytes arena.
const PENDING_ARENA_INITIAL_CAPACITY: usize = 64 * 1024;
/// Above this capacity an emptied arena is shrunk back (a bulk statement can grow it to tens of MB).
const ARENA_SHRINK_MIN_CAPACITY: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointMetadata {
    pub checkpoint_lsn: Lsn,
    pub min_recovery_lsn: Lsn,
    pub min_retained_segment_id: u64,
    pub checkpoint_segment_id: u64,
    pub max_user_txn_id: TxnId,
    pub max_sys_txn_id: TxnId,
    pub segments_reclaimed: usize,
}

impl CheckpointMetadata {
    pub fn serialize(&self) -> [u8; CHECKPOINT_META_SIZE] {
        let mut buf = [0u8; CHECKPOINT_META_SIZE];
        buf[0..4].copy_from_slice(&MAGIC_BYTES.to_le_bytes());
        buf[4..8].copy_from_slice(&CHECKPOINT_META_VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[16..24].copy_from_slice(&self.min_recovery_lsn.to_le_bytes());
        buf[24..32].copy_from_slice(&self.min_retained_segment_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.checkpoint_segment_id.to_le_bytes());
        buf[40..48].copy_from_slice(&self.max_user_txn_id.to_le_bytes());
        buf[48..56].copy_from_slice(&self.max_sys_txn_id.to_le_bytes());
        let crc = crc32c::checksum(&buf[0..56]);
        buf[56..60].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() != CHECKPOINT_META_SIZE {
            return Err(Error::WalCorrupted(format!(
                "Checkpoint metadata size mismatch: expected {}, got {}",
                CHECKPOINT_META_SIZE,
                buf.len()
            )));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC_BYTES {
            return Err(Error::WalCorrupted(
                "Invalid checkpoint metadata magic".to_string(),
            ));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != CHECKPOINT_META_VERSION {
            return Err(Error::WalCorrupted(format!(
                "Unsupported checkpoint metadata version {}",
                version
            )));
        }
        let stored_crc = u32::from_le_bytes(buf[56..60].try_into().unwrap());
        let computed_crc = crc32c::checksum(&buf[0..56]);
        if stored_crc != computed_crc {
            return Err(Error::WalCorrupted(format!(
                "Checkpoint metadata CRC32C mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }
        let checkpoint_lsn = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let min_recovery_lsn = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let min_retained_segment_id = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let checkpoint_segment_id = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let max_user_txn_id = u64::from_le_bytes(buf[40..48].try_into().unwrap());
        let max_sys_txn_id = u64::from_le_bytes(buf[48..56].try_into().unwrap());

        if checkpoint_lsn == INVALID_LSN
            || min_recovery_lsn == INVALID_LSN
            || min_recovery_lsn > checkpoint_lsn
        {
            return Err(Error::WalCorrupted(
                "Invalid LSN bounds in checkpoint metadata".to_string(),
            ));
        }
        if min_retained_segment_id > checkpoint_segment_id {
            return Err(Error::WalCorrupted(
                "Invalid segment bounds in checkpoint metadata".to_string(),
            ));
        }

        Ok(CheckpointMetadata {
            checkpoint_lsn,
            min_recovery_lsn,
            min_retained_segment_id,
            checkpoint_segment_id,
            max_user_txn_id,
            max_sys_txn_id,
            segments_reclaimed: 0,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    Begin = 1,
    Update = 2,
    Commit = 3,
    Abort = 4,
    Clr = 5,
    Checkpoint = 6,
}

impl LogRecordType {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            1 => Ok(LogRecordType::Begin),
            2 => Ok(LogRecordType::Update),
            3 => Ok(LogRecordType::Commit),
            4 => Ok(LogRecordType::Abort),
            5 => Ok(LogRecordType::Clr),
            6 => Ok(LogRecordType::Checkpoint),
            _ => Err(Error::WalCorrupted(format!(
                "Unknown log record type: {}",
                val
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: LogRecordType,
    pub page_id: PageId,
    pub offset: u16,
    pub undo_next_lsn: Lsn,
    pub redo_data: Vec<u8>,
    pub undo_data: Vec<u8>,
}

impl LogRecord {
    pub fn new_begin(lsn: Lsn, txn_id: TxnId) -> Self {
        LogRecord {
            lsn,
            prev_lsn: INVALID_LSN,
            txn_id,
            record_type: LogRecordType::Begin,
            page_id: 0,
            offset: 0,
            undo_next_lsn: INVALID_LSN,
            redo_data: Vec::new(),
            undo_data: Vec::new(),
        }
    }

    pub fn new_update(
        lsn: Lsn,
        txn_id: TxnId,
        prev_lsn: Lsn,
        page_id: PageId,
        offset: u16,
        redo: Vec<u8>,
        undo: Vec<u8>,
    ) -> Self {
        LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type: LogRecordType::Update,
            page_id,
            offset,
            undo_next_lsn: INVALID_LSN,
            redo_data: redo,
            undo_data: undo,
        }
    }

    pub fn new_commit(lsn: Lsn, txn_id: TxnId, prev_lsn: Lsn) -> Self {
        LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type: LogRecordType::Commit,
            page_id: 0,
            offset: 0,
            undo_next_lsn: INVALID_LSN,
            redo_data: Vec::new(),
            undo_data: Vec::new(),
        }
    }

    pub fn new_abort(lsn: Lsn, txn_id: TxnId, prev_lsn: Lsn) -> Self {
        LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type: LogRecordType::Abort,
            page_id: 0,
            offset: 0,
            undo_next_lsn: INVALID_LSN,
            redo_data: Vec::new(),
            undo_data: Vec::new(),
        }
    }

    pub fn new_clr(
        lsn: Lsn,
        txn_id: TxnId,
        prev_lsn: Lsn,
        page_id: PageId,
        offset: u16,
        undo_next_lsn: Lsn,
        redo: Vec<u8>,
    ) -> Self {
        LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type: LogRecordType::Clr,
            page_id,
            offset,
            undo_next_lsn,
            redo_data: redo,
            undo_data: Vec::new(),
        }
    }

    pub fn new_checkpoint(lsn: Lsn, dirty_pages: &[(PageId, Lsn)]) -> Self {
        let mut redo = Vec::with_capacity(dirty_pages.len() * 12);
        for &(pid, rec_lsn) in dirty_pages {
            redo.extend_from_slice(&pid.to_le_bytes());
            redo.extend_from_slice(&rec_lsn.to_le_bytes());
        }
        LogRecord {
            lsn,
            prev_lsn: INVALID_LSN,
            txn_id: 0,
            record_type: LogRecordType::Checkpoint,
            page_id: 0,
            offset: 0,
            undo_next_lsn: INVALID_LSN,
            redo_data: redo,
            undo_data: Vec::new(),
        }
    }

    pub fn checkpoint_dirty_pages(&self) -> Result<Vec<(PageId, Lsn)>> {
        if self.record_type != LogRecordType::Checkpoint {
            return Err(Error::WalCorrupted(
                "Not a checkpoint log record".to_string(),
            ));
        }
        if !self.redo_data.len().is_multiple_of(12) {
            return Err(Error::WalCorrupted(
                "Corrupted checkpoint dirty page table payload".to_string(),
            ));
        }
        let mut out = Vec::with_capacity(self.redo_data.len() / 12);
        for chunk in self.redo_data.as_chunks::<12>().0 {
            let pid = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let rec_lsn = u64::from_le_bytes(chunk[4..12].try_into().unwrap());
            out.push((pid, rec_lsn));
        }
        Ok(out)
    }

    /// Exact number of bytes `serialize_append` / `serialize_into` will produce.
    pub fn serialized_size(&self) -> usize {
        LOG_RECORD_FIXED_HEADER + self.redo_data.len() + self.undo_data.len() + 4
    }

    /// Appends this record's serialized bytes to the END of `buf` (existing
    /// contents are preserved). The CRC covers only the bytes appended here.
    pub fn serialize_append(&self, buf: &mut Vec<u8>) {
        let start = buf.len();
        let total_size = self.serialized_size();
        buf.reserve(total_size);

        buf.extend_from_slice(&(total_size as u32).to_le_bytes());
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.to_le_bytes());
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&self.page_id.to_le_bytes());
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.undo_next_lsn.to_le_bytes());
        buf.extend_from_slice(&(self.redo_data.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(self.undo_data.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.redo_data);
        buf.extend_from_slice(&self.undo_data);

        let crc = crc32c::checksum(&buf[start..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    pub fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.clear();
        self.serialize_append(buf);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.serialized_size());
        self.serialize_append(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 51 {
            return Err(Error::WalCorrupted(
                "Log record buffer too short".to_string(),
            ));
        }
        let total_size = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        if buf.len() != total_size {
            return Err(Error::WalCorrupted(format!(
                "Log record size mismatch: expected {}, got {}",
                total_size,
                buf.len()
            )));
        }
        let stored_crc = u32::from_le_bytes(buf[total_size - 4..total_size].try_into().unwrap());
        let computed_crc = crc32c::checksum(&buf[0..total_size - 4]);
        if stored_crc != computed_crc {
            return Err(Error::WalCorrupted(format!(
                "WAL CRC32C mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        let lsn = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let prev_lsn = u64::from_le_bytes(buf[12..20].try_into().unwrap());
        let txn_id = u64::from_le_bytes(buf[20..28].try_into().unwrap());
        let record_type = LogRecordType::from_u8(buf[28])?;
        let page_id = u32::from_le_bytes(buf[29..33].try_into().unwrap());
        let offset = u16::from_le_bytes(buf[33..35].try_into().unwrap());
        let undo_next_lsn = u64::from_le_bytes(buf[35..43].try_into().unwrap());
        let redo_len = u16::from_le_bytes(buf[43..45].try_into().unwrap()) as usize;
        let undo_len = u16::from_le_bytes(buf[45..47].try_into().unwrap()) as usize;

        let payload_start = 47;
        let redo_end = payload_start + redo_len;
        let undo_end = redo_end + undo_len;

        if undo_end + 4 != total_size {
            return Err(Error::WalCorrupted("Invalid payload bounds".to_string()));
        }

        let redo_data = buf[payload_start..redo_end].to_vec();
        let undo_data = buf[redo_end..undo_end].to_vec();

        Ok(LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            page_id,
            offset,
            undo_next_lsn,
            redo_data,
            undo_data,
        })
    }
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    {
        if let Some(parent) = path.parent() {
            let dir_path = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let dir = File::open(dir_path)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn write_and_sync_uring(
    ring: &mut io_uring::IoUring,
    file: &std::fs::File,
    offset: u64,
    buf: &[u8],
) -> std::io::Result<()> {
    use io_uring::{opcode, types};

    let fd = types::Fd(file.as_raw_fd());

    let write_op = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
        .offset(offset)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK);

    let fsync_op = opcode::Fsync::new(fd)
        .flags(types::FsyncFlags::DATASYNC)
        .build();

    unsafe {
        let mut sq = ring.submission();
        sq.push(&write_op)
            .map_err(|_| std::io::Error::other("SQ full on write"))?;
        sq.push(&fsync_op)
            .map_err(|_| std::io::Error::other("SQ full on fsync"))?;
    }

    ring.submit_and_wait(2)?;

    for cqe in ring.completion() {
        if cqe.result() < 0 {
            return Err(std::io::Error::from_raw_os_error(-cqe.result()));
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct WalSegmentMeta {
    segment_id: u64,
    path: PathBuf,
    first_lsn: Option<Lsn>,
    last_lsn: Option<Lsn>,
    flushed_bytes: u64,
    planned_bytes: u64,
}

/// A record that has been appended but not yet flushed. Its serialized bytes live in
/// `WalInner::pending_bytes` at absolute offset `start` (see `WalInner::pending_base`),
/// `len` bytes long. Storing offsets instead of an owned `Vec<u8>` removes one heap
/// allocation per appended record.
#[derive(Debug, Clone)]
struct PendingRecord {
    lsn: Lsn,
    segment_id: u64,
    start: u64,
    len: u32,
}

/// Work item describing one WAL segment's pending byte range in the flushed arena.
struct FlushSegmentWork {
    segment_id: u64,
    path: PathBuf,
    prev_flushed_bytes: u64,
    existed_before: bool,
    slice_start: usize,
    slice_end: usize,
}

/// Standalone batch handed off to the group-commit flush leader.
struct FlushBatch {
    segments: Vec<FlushSegmentWork>,
    arena: Vec<u8>,
    #[allow(dead_code)]
    target_lsn: Lsn,
    drained_records: Vec<PendingRecord>,
    bytes_by_seg: HashMap<u64, u64>,
}

#[derive(Debug, Clone, Copy)]
struct WalTxnTracker {
    first_lsn: Lsn,
    last_lsn: Lsn,
    terminated: bool,
}

struct WalInner {
    segments: Vec<WalSegmentMeta>,
    max_segment_bytes: u64,
    next_lsn: Lsn,
    flushed_lsn: Lsn,
    pending: Vec<PendingRecord>,
    /// Contiguous serialized bytes of all `pending` records, in LSN order.
    /// Invariant: `pending[0].start == pending_base` and the arena holds exactly
    /// the bytes of `pending` back to back.
    pending_bytes: Vec<u8>,
    /// Standby arena for zero-copy ping-pong swap during group commits.
    standby_arena: Vec<u8>,
    /// Absolute offset (in "all bytes ever appended" space) of `pending_bytes[0]`.
    pending_base: u64,
    checkpoint_meta: Option<CheckpointMetadata>,
    txn_trackers: HashMap<TxnId, WalTxnTracker>,
    max_user_txn_id: TxnId,
    max_sys_txn_id: TxnId,
    backup_pins: HashMap<u64, Lsn>,
    next_pin_id: u64,
    flush_in_progress: bool,
}

impl WalInner {
    /// Removes the first `count` pending records and their arena bytes.
    fn drain_pending(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let total: u64 = self.pending[..count].iter().map(|p| p.len as u64).sum();
        self.pending.drain(0..count);
        self.pending_bytes.drain(0..total as usize);
        self.pending_base += total;
        // Vec::drain never returns capacity; give it back once the arena is mostly empty.
        if self.pending_bytes.capacity() > ARENA_SHRINK_MIN_CAPACITY
            && self.pending_bytes.len() < self.pending_bytes.capacity() / 4
        {
            let keep = self.pending_bytes.len().max(PENDING_ARENA_INITIAL_CAPACITY);
            self.pending_bytes.shrink_to(keep);
        }
    }

    fn track_record(&mut self, record: &LogRecord) {
        if record.txn_id != 0 {
            if record.txn_id < SYS_TXN_ID_MIN {
                if record.txn_id > self.max_user_txn_id {
                    self.max_user_txn_id = record.txn_id;
                }
            } else if record.txn_id > self.max_sys_txn_id {
                self.max_sys_txn_id = record.txn_id;
            }

            match record.record_type {
                LogRecordType::Begin | LogRecordType::Update | LogRecordType::Clr => {
                    let entry = self
                        .txn_trackers
                        .entry(record.txn_id)
                        .or_insert(WalTxnTracker {
                            first_lsn: record.lsn,
                            last_lsn: record.lsn,
                            terminated: false,
                        });
                    entry.last_lsn = record.lsn;
                    entry.terminated = false;
                }
                LogRecordType::Commit | LogRecordType::Abort => {
                    let entry = self
                        .txn_trackers
                        .entry(record.txn_id)
                        .or_insert(WalTxnTracker {
                            first_lsn: record.lsn,
                            last_lsn: record.lsn,
                            terminated: true,
                        });
                    entry.last_lsn = record.lsn;
                    entry.terminated = true;
                }
                LogRecordType::Checkpoint => {}
            }
        }
    }
}

pub struct WalManager {
    inner: Mutex<WalInner>,
    /// Signaled when a group-commit flush completes (success or failure).
    flush_cv: Condvar,
    path: PathBuf,
    bpm_listeners: Mutex<Vec<std::sync::Weak<crate::buffer_pool::BufferPoolManager>>>,
    /// (segment_id, open_file_handle) cached for fast sequential append & flush.
    active_segment_file: Mutex<Option<(u64, std::fs::File)>>,
    #[allow(dead_code)]
    ring: Mutex<Option<io_uring::IoUring>>,
    /// Pages whose page_lsn is <= this value get a full-page image on their next logged write.
    ckpt_full_image_threshold: std::sync::atomic::AtomicU64,
    /// Test-only: allow n appends, then fail one (-1 = off).
    #[cfg(feature = "fault-injection")]
    fail_append_after: std::sync::atomic::AtomicI64,
}

impl WalManager {
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_segment_size(path, DEFAULT_WAL_SEGMENT_BYTES)
    }

    pub fn open_with_segment_size<P: AsRef<Path>>(path: P, max_segment_bytes: u64) -> Result<Self> {
        if max_segment_bytes < MIN_WAL_SEGMENT_BYTES {
            return Err(Error::WalCorrupted(format!(
                "WAL max_segment_bytes {} is smaller than minimum {}",
                max_segment_bytes, MIN_WAL_SEGMENT_BYTES
            )));
        }
        let path_buf = path.as_ref().to_path_buf();
        if let Some(parent) = path_buf.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mgr = WalManager {
            inner: Mutex::new(WalInner {
                segments: Vec::new(),
                max_segment_bytes,
                next_lsn: 1,
                flushed_lsn: 0,
                pending: Vec::new(),
                pending_bytes: Vec::with_capacity(PENDING_ARENA_INITIAL_CAPACITY),
                standby_arena: Vec::with_capacity(PENDING_ARENA_INITIAL_CAPACITY),
                pending_base: 0,
                checkpoint_meta: None,
                txn_trackers: HashMap::new(),
                max_user_txn_id: 0,
                max_sys_txn_id: 0,
                backup_pins: HashMap::new(),
                next_pin_id: 1,
                flush_in_progress: false,
            }),
            flush_cv: Condvar::new(),
            path: path_buf,
            bpm_listeners: Mutex::new(Vec::new()),
            active_segment_file: Mutex::new(None),
            ring: Mutex::new(io_uring::IoUring::new(16).ok()),
            ckpt_full_image_threshold: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "fault-injection")]
            fail_append_after: std::sync::atomic::AtomicI64::new(-1),
        };
        let _ = mgr.truncate_torn_tail()?;
        let startup_ckpt = mgr
            .inner
            .lock()
            .unwrap()
            .checkpoint_meta
            .map(|m| m.checkpoint_lsn)
            .unwrap_or(0);
        mgr.ckpt_full_image_threshold
            .store(startup_ckpt, std::sync::atomic::Ordering::Relaxed);
        Ok(mgr)
    }

    /// Locks `inner` and waits until no group-commit leader is mid-I/O.
    ///
    /// A leader can only be elected while holding `inner`, and it sets
    /// `flush_in_progress` before releasing the lock. So once this returns, no
    /// flush I/O is running, and none can start until the caller drops the guard.
    /// Paths that perform WAL file I/O while holding the lock (`flush_up_to_locked`
    /// callers) must use this instead of `self.inner.lock()`, otherwise they can
    /// write/truncate the same segment concurrently with the leader.
    fn lock_quiesced(&self) -> MutexGuard<'_, WalInner> {
        let mut inner = self.inner.lock().unwrap();
        while inner.flush_in_progress {
            inner = self.flush_cv.wait(inner).unwrap();
        }
        inner
    }

    /// Test hook: allow `n` more appends, then fail one (-1 = off).
    #[cfg(feature = "fault-injection")]
    pub fn set_fail_append_after(&self, n: i64) {
        self.fail_append_after
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "fault-injection")]
    fn inject_append_fail(&self) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let inj = self.fail_append_after.load(Relaxed);
        if inj >= 0 {
            if inj == 0 {
                self.fail_append_after.store(-1, Relaxed);
                return Err(Error::Io("injected wal append failure".to_string()));
            }
            self.fail_append_after.store(inj - 1, Relaxed);
        }
        Ok(())
    }

    /// True if the next logged write to a page with this LSN must carry a full-page image
    /// (first write since the last checkpoint). Hot path: one relaxed atomic load.
    pub fn needs_full_page_image(&self, page_lsn: Lsn) -> bool {
        page_lsn
            <= self
                .ckpt_full_image_threshold
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn register_bpm(&self, bpm: &std::sync::Arc<crate::buffer_pool::BufferPoolManager>) {
        let mut listeners = self.bpm_listeners.lock().unwrap();
        listeners.retain(|w| w.strong_count() > 0);
        if !listeners
            .iter()
            .any(|w| w.as_ptr() == std::sync::Arc::as_ptr(bpm))
        {
            listeners.push(std::sync::Arc::downgrade(bpm));
        }
    }

    /// Notify registered buffer pools after a transaction reaches a final state.
    /// All listeners are attempted; the first reclamation error is returned.
    pub fn notify_txn_finalized(&self, txn_id: TxnId) -> Result<()> {
        let upgraded: Vec<std::sync::Arc<crate::buffer_pool::BufferPoolManager>> = {
            let mut listeners = self.bpm_listeners.lock().unwrap();
            listeners.retain(|w| w.strong_count() > 0);
            listeners.iter().filter_map(|w| w.upgrade()).collect()
        };
        let mut first_error = None;
        for bpm in upgraded {
            if let Err(err) = bpm.finalize_free_pages(txn_id) {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn base_file_name(base_path: &Path) -> Result<&str> {
        base_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::WalCorrupted("Invalid WAL file path".to_string()))
    }

    fn segment_path(base_path: &Path, segment_id: u64) -> PathBuf {
        if segment_id == 0 {
            base_path.to_path_buf()
        } else {
            let name = base_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("wal");
            base_path.with_file_name(format!("{}.{:08}", name, segment_id))
        }
    }

    fn ckpt_path(base_path: &Path) -> PathBuf {
        let name = base_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("wal");
        base_path.with_file_name(format!("{}.ckpt", name))
    }

    fn ckpt_tmp_path(base_path: &Path) -> PathBuf {
        let name = base_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("wal");
        base_path.with_file_name(format!("{}.ckpt.tmp", name))
    }

    fn discover_segments_on_disk(&self) -> Result<Vec<(u64, PathBuf)>> {
        let base_name = Self::base_file_name(&self.path)?;
        let parent_dir = match self.path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        if !parent_dir.exists() {
            return Ok(Vec::new());
        }

        let prefix = format!("{}.", base_name);
        let mut found = Vec::new();
        for entry_res in std::fs::read_dir(parent_dir)? {
            let entry = entry_res?;
            let fname_os = entry.file_name();
            let Some(fname) = fname_os.to_str() else {
                continue;
            };
            if fname == base_name {
                found.push((0u64, entry.path()));
            } else if let Some(suffix) = fname.strip_prefix(&prefix) {
                if suffix == "ckpt" || suffix == "ckpt.tmp" {
                    continue;
                }
                if suffix.len() >= 8 && suffix.bytes().all(|b| b.is_ascii_digit()) {
                    let seg_id = suffix.parse::<u64>().map_err(|_| {
                        Error::WalCorrupted(format!("Invalid WAL segment suffix: {}", suffix))
                    })?;
                    if seg_id == 0 {
                        return Err(Error::WalCorrupted(
                            "Duplicate segment 0 suffix file detected".to_string(),
                        ));
                    }
                    found.push((seg_id, entry.path()));
                }
            }
        }
        found.sort_by_key(|(id, _)| *id);
        Ok(found)
    }

    fn load_checkpoint_metadata_from_disk(&self) -> Result<Option<CheckpointMetadata>> {
        let tmp_path = Self::ckpt_tmp_path(&self.path);
        if tmp_path.exists() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        let ckpt_path = Self::ckpt_path(&self.path);
        if !ckpt_path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&ckpt_path)?;
        let meta = CheckpointMetadata::deserialize(&bytes)?;
        Ok(Some(meta))
    }

    fn write_checkpoint_metadata_to_disk(&self, meta: &CheckpointMetadata) -> Result<()> {
        let tmp_path = Self::ckpt_tmp_path(&self.path);
        let ckpt_path = Self::ckpt_path(&self.path);
        let bytes = meta.serialize();
        {
            let mut tmp_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp_file.write_all(&bytes)?;
            tmp_file.flush()?;
            tmp_file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &ckpt_path)?;
        sync_parent_dir(&ckpt_path)?;
        Ok(())
    }

    pub fn truncate_torn_tail(&self) -> Result<Vec<LogRecord>> {
        let mut inner = self.inner.lock().unwrap();
        let ckpt_meta = self.load_checkpoint_metadata_from_disk()?;
        let mut discovered = self.discover_segments_on_disk()?;

        if let Some(ref meta) = ckpt_meta {
            let mut removed_obsolete = false;
            let mut retained = Vec::with_capacity(discovered.len());
            for (seg_id, seg_path) in discovered {
                if seg_id < meta.min_retained_segment_id {
                    let _ = std::fs::remove_file(&seg_path);
                    removed_obsolete = true;
                } else {
                    retained.push((seg_id, seg_path));
                }
            }
            if removed_obsolete {
                sync_parent_dir(&self.path)?;
            }
            discovered = retained;
        }

        if discovered.is_empty() {
            if let Some(ref meta) = ckpt_meta {
                return Err(Error::WalCorrupted(format!(
                    "Missing required WAL segment {} referenced by checkpoint",
                    meta.min_retained_segment_id
                )));
            }
            let seg0_path = Self::segment_path(&self.path, 0);
            let _file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&seg0_path)?;
            sync_parent_dir(&seg0_path)?;
            if inner.pending.is_empty() {
                inner.segments = vec![WalSegmentMeta {
                    segment_id: 0,
                    path: seg0_path,
                    first_lsn: None,
                    last_lsn: None,
                    flushed_bytes: 0,
                    planned_bytes: 0,
                }];
                inner.next_lsn = 1;
                inner.flushed_lsn = 0;
                inner.checkpoint_meta = None;
                self.ckpt_full_image_threshold
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                inner.txn_trackers.clear();
                inner.max_user_txn_id = 0;
                inner.max_sys_txn_id = 0;
            }
            return Ok(Vec::new());
        }

        let expected_start_seg = ckpt_meta
            .as_ref()
            .map(|m| m.min_retained_segment_id)
            .unwrap_or(0);
        if discovered[0].0 != expected_start_seg {
            return Err(Error::WalCorrupted(format!(
                "Missing required WAL segment {} (first found is {})",
                expected_start_seg, discovered[0].0
            )));
        }
        for pair in discovered.windows(2) {
            let expected_next = pair[0]
                .0
                .checked_add(1)
                .ok_or_else(|| Error::WalCorrupted("Segment ID overflow".to_string()))?;
            if pair[1].0 != expected_next {
                return Err(Error::WalCorrupted(format!(
                    "Missing required WAL segment {} between {} and {}",
                    expected_next, pair[0].0, pair[1].0
                )));
            }
        }
        if let Some(ref meta) = ckpt_meta {
            let max_seg_id = discovered.last().unwrap().0;
            if max_seg_id < meta.checkpoint_segment_id {
                return Err(Error::WalCorrupted(format!(
                    "Missing checkpoint WAL segment {} (max found is {})",
                    meta.checkpoint_segment_id, max_seg_id
                )));
            }
        }

        let mut records = Vec::new();
        let mut scanned_segments = Vec::with_capacity(discovered.len());
        let mut expected_next_lsn: Option<Lsn> = None;
        let num_discovered = discovered.len();

        for (idx, (seg_id, seg_path)) in discovered.into_iter().enumerate() {
            let is_last_segment = idx + 1 == num_discovered;
            let mut file = OpenOptions::new().read(true).write(true).open(&seg_path)?;
            let file_len = file.metadata()?.len();
            if !is_last_segment && file_len == 0 {
                return Err(Error::WalCorrupted(format!(
                    "Empty non-final WAL segment {}",
                    seg_id
                )));
            }
            file.seek(SeekFrom::Start(0))?;
            let mut offset = 0u64;
            let mut seg_first_lsn = None;
            let mut seg_last_lsn = None;

            while offset < file_len {
                let remaining = file_len - offset;
                if remaining < 4 {
                    if is_last_segment {
                        file.set_len(offset)?;
                        file.sync_all()?;
                        break;
                    }
                    return Err(Error::WalCorrupted(format!(
                        "Truncated record length in non-final WAL segment {}",
                        seg_id
                    )));
                }

                let mut size_buf = [0u8; 4];
                file.read_exact(&mut size_buf)?;
                let total_size = u32::from_le_bytes(size_buf) as usize;
                if !(51..=131_121).contains(&total_size) {
                    if is_last_segment {
                        file.set_len(offset)?;
                        file.sync_all()?;
                        break;
                    }
                    return Err(Error::WalCorrupted(format!(
                        "Record length less than minimum header or exceeds bounds in non-final WAL segment {}",
                        seg_id
                    )));
                }

                if (total_size as u64) > remaining {
                    if !is_last_segment {
                        return Err(Error::WalCorrupted(format!(
                            "Truncated record body in non-final WAL segment {}",
                            seg_id
                        )));
                    }
                    if remaining >= 47 {
                        let mut hdr = [0u8; 43];
                        file.read_exact(&mut hdr)?;
                        let _ = LogRecordType::from_u8(hdr[24])?;
                        let redo_len = u16::from_le_bytes(hdr[39..41].try_into().unwrap()) as usize;
                        let undo_len = u16::from_le_bytes(hdr[41..43].try_into().unwrap()) as usize;
                        if 47 + redo_len + undo_len + 4 != total_size {
                            return Err(Error::WalCorrupted(
                                "Invalid payload bounds in WAL header".to_string(),
                            ));
                        }
                    }
                    file.set_len(offset)?;
                    file.sync_all()?;
                    break;
                }

                let mut record_buf = vec![0u8; total_size];
                record_buf[0..4].copy_from_slice(&size_buf);
                file.read_exact(&mut record_buf[4..])?;

                let record = LogRecord::deserialize(&record_buf)?;
                if let Some(exp_lsn) = expected_next_lsn {
                    if record.lsn != exp_lsn {
                        return Err(Error::WalCorrupted(format!(
                            "WAL LSN discontinuity: expected {}, got {}",
                            exp_lsn, record.lsn
                        )));
                    }
                } else if let Some(ref meta) = ckpt_meta {
                    if record.lsn == INVALID_LSN || record.lsn > meta.min_recovery_lsn {
                        return Err(Error::WalCorrupted(format!(
                            "WAL starts at LSN {} which exceeds checkpoint min_recovery_lsn {}",
                            record.lsn, meta.min_recovery_lsn
                        )));
                    }
                } else if record.lsn != 1 {
                    return Err(Error::WalCorrupted(format!(
                        "Initial WAL segment 0 must start at LSN 1, found {}",
                        record.lsn
                    )));
                }

                expected_next_lsn = Some(
                    record
                        .lsn
                        .checked_add(1)
                        .ok_or_else(|| Error::WalCorrupted("LSN overflow".to_string()))?,
                );
                if seg_first_lsn.is_none() {
                    seg_first_lsn = Some(record.lsn);
                }
                seg_last_lsn = Some(record.lsn);
                records.push(record);
                offset = offset
                    .checked_add(total_size as u64)
                    .ok_or_else(|| Error::WalCorrupted("Segment offset overflow".to_string()))?;
            }

            if !is_last_segment && seg_first_lsn.is_none() {
                return Err(Error::WalCorrupted(format!(
                    "Non-final WAL segment {} has no records",
                    seg_id
                )));
            }

            scanned_segments.push(WalSegmentMeta {
                segment_id: seg_id,
                path: seg_path,
                first_lsn: seg_first_lsn,
                last_lsn: seg_last_lsn,
                flushed_bytes: offset,
                planned_bytes: offset,
            });
        }

        if let Some(ref meta) = ckpt_meta {
            let has_ckpt = records.iter().any(|r| {
                r.lsn == meta.checkpoint_lsn && r.record_type == LogRecordType::Checkpoint
            });
            if !has_ckpt {
                return Err(Error::WalCorrupted(format!(
                    "Checkpoint record at LSN {} missing from WAL segments",
                    meta.checkpoint_lsn
                )));
            }
        }

        if inner.pending.is_empty() {
            let max_lsn = records
                .last()
                .map(|r| r.lsn)
                .or_else(|| ckpt_meta.as_ref().map(|m| m.checkpoint_lsn))
                .unwrap_or(0);
            inner.next_lsn = max_lsn
                .checked_add(1)
                .ok_or_else(|| Error::WalCorrupted("LSN overflow".to_string()))?;
            inner.flushed_lsn = max_lsn;
            inner.segments = scanned_segments;
            inner.checkpoint_meta = ckpt_meta;
            self.ckpt_full_image_threshold.store(
                ckpt_meta.as_ref().map(|m| m.checkpoint_lsn).unwrap_or(0),
                std::sync::atomic::Ordering::Relaxed,
            );
            inner.txn_trackers.clear();
            inner.max_user_txn_id = ckpt_meta.as_ref().map(|m| m.max_user_txn_id).unwrap_or(0);
            inner.max_sys_txn_id = ckpt_meta.as_ref().map(|m| m.max_sys_txn_id).unwrap_or(0);
            for r in &records {
                inner.track_record(r);
            }
        }

        Ok(records)
    }

    pub fn next_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().next_lsn
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().flushed_lsn
    }

    pub fn max_segment_bytes(&self) -> u64 {
        self.inner.lock().unwrap().max_segment_bytes
    }

    pub fn set_max_segment_bytes(&self, max_segment_bytes: u64) -> Result<()> {
        if max_segment_bytes < MIN_WAL_SEGMENT_BYTES {
            return Err(Error::WalCorrupted(format!(
                "WAL max_segment_bytes {} is smaller than minimum {}",
                max_segment_bytes, MIN_WAL_SEGMENT_BYTES
            )));
        }
        self.inner.lock().unwrap().max_segment_bytes = max_segment_bytes;
        Ok(())
    }

    pub fn active_segment_id(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .segments
            .last()
            .map(|s| s.segment_id)
            .unwrap_or(0)
    }

    pub fn segment_ids(&self) -> Vec<u64> {
        self.inner
            .lock()
            .unwrap()
            .segments
            .iter()
            .map(|s| s.segment_id)
            .collect()
    }

    /// Flush ka intezaar karte (buffered) WAL bytes. Bulk statements isse memory pressure dekhte hain.
    pub fn pending_bytes_len(&self) -> usize {
        self.inner.lock().unwrap().pending_bytes.len()
    }

    pub fn segment_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }

    pub fn segment_path_for_id(&self, segment_id: u64) -> PathBuf {
        Self::segment_path(&self.path, segment_id)
    }

    pub fn checkpoint_meta_path(&self) -> PathBuf {
        Self::ckpt_path(&self.path)
    }

    pub fn checkpoint_metadata(&self) -> Option<CheckpointMetadata> {
        self.inner.lock().unwrap().checkpoint_meta
    }

    pub fn max_user_txn_id(&self) -> TxnId {
        self.inner.lock().unwrap().max_user_txn_id
    }

    pub fn max_sys_txn_id(&self) -> TxnId {
        self.inner.lock().unwrap().max_sys_txn_id
    }

    pub fn rotate_segment(&self) -> Result<u64> {
        self.flush_all()?;
        let mut inner = self.inner.lock().unwrap();
        let Some(last_seg) = inner.segments.last() else {
            return Err(Error::WalCorrupted("WAL has no active segment".to_string()));
        };
        if last_seg.planned_bytes == 0 {
            return Ok(last_seg.segment_id);
        }
        let next_seg_id = last_seg
            .segment_id
            .checked_add(1)
            .ok_or_else(|| Error::WalCorrupted("Segment ID overflow".to_string()))?;
        let next_path = Self::segment_path(&self.path, next_seg_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&next_path)?;
        file.sync_all()?;
        sync_parent_dir(&next_path)?;
        inner.segments.push(WalSegmentMeta {
            segment_id: next_seg_id,
            path: next_path,
            first_lsn: None,
            last_lsn: None,
            flushed_bytes: 0,
            planned_bytes: 0,
        });
        GLOBAL_METRICS.record_wal_rotation();
        Ok(next_seg_id)
    }

    pub fn append_records_batch(&self, records: &mut [LogRecord]) -> Result<Lsn> {
        if records.is_empty() {
            return Ok(INVALID_LSN);
        }
        if records.len() == 1 {
            let lsn = self.append_record(records[0].clone())?;
            records[0].lsn = lsn;
            return Ok(lsn);
        }
        let profiling = mitrdb_observe::profiling_enabled();
        let append_started = profiling.then(std::time::Instant::now);
        #[cfg(feature = "fault-injection")]
        self.inject_append_fail()?;
        let mut inner = self.inner.lock().unwrap();
        let mut last_lsn = INVALID_LSN;

        for record in records.iter_mut() {
            let assigned_lsn = inner.next_lsn;
            let next_lsn = assigned_lsn
                .checked_add(1)
                .ok_or_else(|| Error::WalCorrupted("LSN overflow".to_string()))?;
            if record.prev_lsn == INVALID_LSN && last_lsn != INVALID_LSN { record.prev_lsn = last_lsn; }
            record.lsn = assigned_lsn;
            let rec_len = record.serialized_size() as u64;

            if inner.segments.is_empty() {
                let seg0_path = Self::segment_path(&self.path, 0);
                inner.segments.push(WalSegmentMeta {
                    segment_id: 0,
                    path: seg0_path,
                    first_lsn: None,
                    last_lsn: None,
                    flushed_bytes: 0,
                    planned_bytes: 0,
                });
            }

            let max_seg_bytes = inner.max_segment_bytes;
            let should_rotate = {
                let active = inner.segments.last().unwrap();
                let projected = active
                    .planned_bytes
                    .checked_add(rec_len)
                    .ok_or_else(|| Error::WalCorrupted("Segment byte count overflow".to_string()))?;
                active.planned_bytes > 0 && projected > max_seg_bytes
            };

            if should_rotate {
                let prev_id = inner.segments.last().unwrap().segment_id;
                let next_seg_id = prev_id
                    .checked_add(1)
                    .ok_or_else(|| Error::WalCorrupted("Segment ID overflow".to_string()))?;
                let next_path = Self::segment_path(&self.path, next_seg_id);
                inner.segments.push(WalSegmentMeta {
                    segment_id: next_seg_id,
                    path: next_path,
                    first_lsn: None,
                    last_lsn: None,
                    flushed_bytes: 0,
                    planned_bytes: 0,
                });
                GLOBAL_METRICS.record_wal_rotation();
            }

            let seg_id = {
                let active = inner.segments.last_mut().unwrap();
                active.planned_bytes = active
                    .planned_bytes
                    .checked_add(rec_len)
                    .ok_or_else(|| Error::WalCorrupted("Segment byte count overflow".to_string()))?;
                if active.first_lsn.is_none() {
                    active.first_lsn = Some(assigned_lsn);
                }
                active.last_lsn = Some(assigned_lsn);
                active.segment_id
            };

            let start = inner.pending_base + inner.pending_bytes.len() as u64;
            record.serialize_append(&mut inner.pending_bytes);
            inner.track_record(record);
            inner.next_lsn = next_lsn;
            inner.pending.push(PendingRecord {
                lsn: assigned_lsn,
                segment_id: seg_id,
                start,
                len: rec_len as u32,
            });
            GLOBAL_METRICS.record_wal_append();
            last_lsn = assigned_lsn;
        }

        if let Some(started) = append_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::WalAppend,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        Ok(last_lsn)
    }

    pub fn append_record(&self, mut record: LogRecord) -> Result<Lsn> {
        let profiling = mitrdb_observe::profiling_enabled();
        let append_started = profiling.then(std::time::Instant::now);
        #[cfg(feature = "fault-injection")]
        self.inject_append_fail()?;
        let mut inner = self.inner.lock().unwrap();
        let assigned_lsn = inner.next_lsn;
        let next_lsn = assigned_lsn
            .checked_add(1)
            .ok_or_else(|| Error::WalCorrupted("LSN overflow".to_string()))?;
        record.lsn = assigned_lsn;
        let rec_len = record.serialized_size() as u64;

        if inner.segments.is_empty() {
            let seg0_path = Self::segment_path(&self.path, 0);
            inner.segments.push(WalSegmentMeta {
                segment_id: 0,
                path: seg0_path,
                first_lsn: None,
                last_lsn: None,
                flushed_bytes: 0,
                planned_bytes: 0,
            });
        }

        let max_seg_bytes = inner.max_segment_bytes;
        let should_rotate = {
            let active = inner.segments.last().unwrap();
            let projected = active
                .planned_bytes
                .checked_add(rec_len)
                .ok_or_else(|| Error::WalCorrupted("Segment byte count overflow".to_string()))?;
            active.planned_bytes > 0 && projected > max_seg_bytes
        };

        if should_rotate {
            let prev_id = inner.segments.last().unwrap().segment_id;
            let next_seg_id = prev_id
                .checked_add(1)
                .ok_or_else(|| Error::WalCorrupted("Segment ID overflow".to_string()))?;
            let next_path = Self::segment_path(&self.path, next_seg_id);
            inner.segments.push(WalSegmentMeta {
                segment_id: next_seg_id,
                path: next_path,
                first_lsn: None,
                last_lsn: None,
                flushed_bytes: 0,
                planned_bytes: 0,
            });
            GLOBAL_METRICS.record_wal_rotation();
        }

        let seg_id = {
            let active = inner.segments.last_mut().unwrap();
            active.planned_bytes = active
                .planned_bytes
                .checked_add(rec_len)
                .ok_or_else(|| Error::WalCorrupted("Segment byte count overflow".to_string()))?;
            if active.first_lsn.is_none() {
                active.first_lsn = Some(assigned_lsn);
            }
            active.last_lsn = Some(assigned_lsn);
            active.segment_id
        };

        // All fallible checks are done. Serialize straight into the shared arena:
        // no per-record heap allocation, no intermediate copy.
        let start = inner.pending_base + inner.pending_bytes.len() as u64;
        record.serialize_append(&mut inner.pending_bytes);

        inner.track_record(&record);
        inner.next_lsn = next_lsn;
        inner.pending.push(PendingRecord {
            lsn: assigned_lsn,
            segment_id: seg_id,
            start,
            len: rec_len as u32,
        });
        GLOBAL_METRICS.record_wal_append();
        if let Some(started) = append_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::WalAppend,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        Ok(assigned_lsn)
    }

    pub fn commit(&self, txn_id: TxnId, prev_lsn: Lsn) -> Result<Lsn> {
        let lsn = self.append_record(LogRecord::new_commit(0, txn_id, prev_lsn))?;
        self.flush_up_to(lsn)?;
        self.notify_txn_finalized(txn_id)?;
        Ok(lsn)
    }

    pub fn abort(&self, txn_id: TxnId, prev_lsn: Lsn) -> Result<Lsn> {
        let lsn = self.append_record(LogRecord::new_abort(0, txn_id, prev_lsn))?;
        self.flush_up_to(lsn)?;
        self.notify_txn_finalized(txn_id)?;
        Ok(lsn)
    }

    pub fn flush_up_to(&self, target_lsn: Lsn) -> Result<()> {
        let profiling = mitrdb_observe::profiling_enabled();
        let started = profiling.then(std::time::Instant::now);
        let result = self.flush_up_to_inner(target_lsn);
        if let Some(started) = started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::WalFlush,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    fn flush_up_to_inner(&self, target_lsn: Lsn) -> Result<()> {
        let flush_started = std::time::Instant::now();
        let lock_wait_started = std::time::Instant::now();
        let mut inner = self.inner.lock().unwrap();
        GLOBAL_METRICS.record_wal_flush_lock_wait_ns(
            lock_wait_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );

        // Group commit protocol:
        // 1. Wait until our target LSN is durable, or become the flush leader.
        // 2. The leader batches every pending record appended so far (not only
        //    its own target), performs write+fsync *outside* the mutex, then
        //    publishes the new flushed_lsn and wakes waiters.
        // 3. Concurrent appends continue under the mutex while the leader syncs.
        // A commit still returns only after its own target LSN is durable.
        //
        // Any other path that does WAL file I/O under the mutex (flush_all,
        // checkpoint, backup) must call `lock_quiesced()` so it never overlaps
        // with a leader's outside-the-lock I/O.
        //
        // On leader I/O failure the leader returns the error; waiters re-evaluate
        // (their LSN may still be pending) and a new leader will retry. The error
        // is not sticky across independent flush attempts.
        loop {
            if target_lsn == 0 || target_lsn <= inner.flushed_lsn {
                GLOBAL_METRICS.record_wal_flush_total_ns(
                    flush_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
                return Ok(());
            }
            if target_lsn >= inner.next_lsn {
                GLOBAL_METRICS.record_wal_flush_total_ns(
                    flush_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
                return Err(Error::WalCorrupted(format!(
                    "Cannot flush WAL up to unwritten LSN {} (next_lsn is {})",
                    target_lsn, inner.next_lsn
                )));
            }
            if inner.flush_in_progress {
                inner = self.flush_cv.wait(inner).unwrap();
                continue;
            }

            // Become the group-commit leader.
            inner.flush_in_progress = true;
            let latest_appended_lsn = inner.next_lsn.saturating_sub(1);
            let flush_target = target_lsn.max(latest_appended_lsn);

            let mut batch = Self::collect_flush_batch(&mut inner, flush_target);
            drop(inner);

            let io_result = self.perform_flush_io(&batch);

            inner = self.inner.lock().unwrap();
            match io_result {
                Ok(()) => {
                    for (seg_id, nbytes) in batch.bytes_by_seg {
                        if let Some(seg) =
                            inner.segments.iter_mut().find(|s| s.segment_id == seg_id)
                        {
                            seg.flushed_bytes = seg.flushed_bytes.saturating_add(nbytes);
                        }
                    }
                    if let Some(last) = batch.drained_records.last() {
                        inner.flushed_lsn = inner.flushed_lsn.max(last.lsn);
                    }
                    GLOBAL_METRICS.record_wal_flush();

                    batch.arena.clear();
                    if batch.arena.capacity() > ARENA_SHRINK_MIN_CAPACITY {
                        batch.arena.shrink_to(PENDING_ARENA_INITIAL_CAPACITY);
                    }
                    inner.standby_arena = batch.arena;

                    inner.flush_in_progress = false;
                    self.flush_cv.notify_all();
                }
                Err(e) => {
                    let drained_bytes: u64 =
                        batch.drained_records.iter().map(|p| p.len as u64).sum();
                    inner.pending_base = inner.pending_base.saturating_sub(drained_bytes);

                    let current_records = std::mem::take(&mut inner.pending);
                    inner.pending = batch.drained_records;
                    inner.pending.extend(current_records);

                    let current_bytes = std::mem::take(&mut inner.pending_bytes);
                    batch.arena.extend_from_slice(&current_bytes);
                    inner.pending_bytes = batch.arena;

                    inner.flush_in_progress = false;
                    self.flush_cv.notify_all();
                    GLOBAL_METRICS.record_wal_flush_total_ns(
                        flush_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    );
                    return Err(e);
                }
            }
        }
    }

    fn collect_flush_batch(inner: &mut WalInner, target_lsn: Lsn) -> FlushBatch {
        let mut runs: Vec<(u64, u64, u64)> = Vec::new();
        let mut drained_count = 0usize;
        let mut bytes_by_seg: HashMap<u64, u64> = HashMap::new();

        for item in &inner.pending {
            if item.lsn > target_lsn {
                break;
            }
            let end = item.start + item.len as u64;
            match runs.last_mut() {
                Some(last) if last.0 == item.segment_id && last.2 == item.start => {
                    last.2 = end;
                }
                _ => runs.push((item.segment_id, item.start, end)),
            }
            *bytes_by_seg.entry(item.segment_id).or_insert(0) += item.len as u64;
            drained_count += 1;
        }

        let mut by_seg: Vec<FlushSegmentWork> = Vec::with_capacity(runs.len());
        for (segment_id, start, end) in runs {
            let seg = inner.segments.iter().find(|s| s.segment_id == segment_id);
            let (path, flushed_bytes) = match seg {
                Some(s) => (s.path.clone(), s.flushed_bytes),
                None => continue,
            };
            let existed = if flushed_bytes > 0 {
                true
            } else {
                path.exists()
            };
            let slice_start = (start - inner.pending_base) as usize;
            let slice_end = (end - inner.pending_base) as usize;

            by_seg.push(FlushSegmentWork {
                segment_id,
                path,
                prev_flushed_bytes: flushed_bytes,
                existed_before: existed,
                slice_start,
                slice_end,
            });
        }

        let mut batch_arena = std::mem::take(&mut inner.standby_arena);
        std::mem::swap(&mut inner.pending_bytes, &mut batch_arena);

        let drained_records: Vec<PendingRecord> = inner.pending.drain(0..drained_count).collect();
        let total_bytes: u64 = drained_records.iter().map(|p| p.len as u64).sum();
        inner.pending_base += total_bytes;

        FlushBatch {
            segments: by_seg,
            arena: batch_arena,
            target_lsn,
            drained_records,
            bytes_by_seg,
        }
    }

    fn perform_flush_io(&self, batch: &FlushBatch) -> Result<()> {
        let mut active_slot = self.active_segment_file.lock().unwrap();

        for seg in &batch.segments {
            if seg.slice_start == seg.slice_end {
                continue;
            }

            let needs_reopen = match active_slot.as_ref() {
                Some((id, _)) => *id != seg.segment_id,
                None => true,
            };

            if needs_reopen {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&seg.path)?;
                if !seg.existed_before {
                    sync_parent_dir(&seg.path)?;
                }
                *active_slot = Some((seg.segment_id, file));
            }
            let file = &mut active_slot.as_mut().unwrap().1;
            let payload: &[u8] = &batch.arena[seg.slice_start..seg.slice_end];
            let sync_started = std::time::Instant::now();

            file.seek(SeekFrom::Start(seg.prev_flushed_bytes))?;
            if let Err(e) = file.write_all(payload) {
                let _ = file.set_len(seg.prev_flushed_bytes);
                *active_slot = None;
                return Err(e.into());
            }
            if let Err(e) = file.sync_data() {
                let _ = file.set_len(seg.prev_flushed_bytes);
                *active_slot = None;
                return Err(e.into());
            }

            GLOBAL_METRICS
                .record_wal_sync_ns(sync_started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn apply_flush_success(inner: &mut WalInner, target_lsn: Lsn) {
        let mut drained = 0usize;
        let mut max_lsn = inner.flushed_lsn;
        let mut bytes_by_seg: HashMap<u64, u64> = HashMap::new();
        for item in &inner.pending {
            if item.lsn > target_lsn {
                break;
            }
            *bytes_by_seg.entry(item.segment_id).or_insert(0) += item.len as u64;
            max_lsn = item.lsn;
            drained += 1;
        }
        if drained == 0 {
            return;
        }
        for (seg_id, nbytes) in bytes_by_seg {
            if let Some(seg) = inner.segments.iter_mut().find(|s| s.segment_id == seg_id) {
                seg.flushed_bytes = seg.flushed_bytes.saturating_add(nbytes);
            }
        }
        inner.drain_pending(drained);
        inner.flushed_lsn = max_lsn;
        GLOBAL_METRICS.record_wal_flush();
    }

    /// Synchronous flush performed entirely under the WAL mutex.
    ///
    /// Callers MUST obtain `inner` via `lock_quiesced()`: this function writes to
    /// segment files and drains `pending`, which would race with a group-commit
    /// leader doing the same outside the lock.
    fn flush_up_to_locked(&self, inner: &mut WalInner, target_lsn: Lsn) -> Result<()> {
        debug_assert!(
            !inner.flush_in_progress,
            "flush_up_to_locked called while a group-commit leader is active"
        );
        if target_lsn == 0 || target_lsn <= inner.flushed_lsn {
            return Ok(());
        }
        if target_lsn >= inner.next_lsn {
            return Err(Error::WalCorrupted(format!(
                "Cannot flush WAL up to unwritten LSN {} (next_lsn is {})",
                target_lsn, inner.next_lsn
            )));
        }

        let mut target_segments: Vec<u64> = Vec::new();
        for item in &inner.pending {
            if item.lsn <= target_lsn {
                if target_segments.last().copied() != Some(item.segment_id) {
                    target_segments.push(item.segment_id);
                }
            } else {
                break;
            }
        }

        for seg_id in target_segments {
            let seg_idx = inner
                .segments
                .iter()
                .position(|s| s.segment_id == seg_id)
                .ok_or_else(|| {
                    Error::WalCorrupted(format!("Missing metadata for WAL segment {}", seg_id))
                })?;
            let seg_path = inner.segments[seg_idx].path.clone();
            let prev_flushed_bytes = inner.segments[seg_idx].flushed_bytes;
            let existed_before = seg_path.exists();

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&seg_path)?;
            if !existed_before {
                sync_parent_dir(&seg_path)?;
            }

            file.seek(SeekFrom::Start(prev_flushed_bytes))?;

            // Previous segments of this flush were already drained, so this segment's
            // records are at the front of `pending` and their bytes are contiguous at
            // the front of the arena: write them with a single write_all.
            let mut count_in_seg = 0usize;
            let mut bytes_in_seg = 0u64;
            let mut max_lsn_in_seg = inner.flushed_lsn;
            for item in &inner.pending {
                if item.lsn <= target_lsn && item.segment_id == seg_id {
                    bytes_in_seg = bytes_in_seg.checked_add(item.len as u64).ok_or_else(|| {
                        Error::WalCorrupted("Segment byte count overflow".to_string())
                    })?;
                    max_lsn_in_seg = item.lsn;
                    count_in_seg += 1;
                } else {
                    break;
                }
            }

            if let Err(e) = file.write_all(&inner.pending_bytes[..bytes_in_seg as usize]) {
                let _ = file.set_len(prev_flushed_bytes);
                return Err(e.into());
            }

            let sync_started = std::time::Instant::now();
            let sync_result = file.sync_data();
            GLOBAL_METRICS
                .record_wal_sync_ns(sync_started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
            if let Err(e) = sync_result {
                let _ = file.set_len(prev_flushed_bytes);
                return Err(e.into());
            }

            inner.segments[seg_idx].flushed_bytes = prev_flushed_bytes
                .checked_add(bytes_in_seg)
                .ok_or_else(|| Error::WalCorrupted("Segment byte count overflow".to_string()))?;
            inner.drain_pending(count_in_seg);
            inner.flushed_lsn = max_lsn_in_seg;
            GLOBAL_METRICS.record_wal_flush();
        }

        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let mut inner = self.lock_quiesced();
        let last_lsn = inner.next_lsn.saturating_sub(1);
        if last_lsn > 0 {
            self.flush_up_to_locked(&mut inner, last_lsn)?;
        }
        Ok(())
    }

    pub fn record_checkpoint_and_reclaim(
        &self,
        checkpoint_lsn: Lsn,
        dirty_pages: &[(PageId, Lsn)],
    ) -> Result<CheckpointMetadata> {
        let (meta, _) =
            self.record_checkpoint_and_reclaim_with_pin(checkpoint_lsn, dirty_pages, false)?;
        Ok(meta)
    }

    pub fn record_checkpoint_and_reclaim_with_pin(
        &self,
        checkpoint_lsn: Lsn,
        dirty_pages: &[(PageId, Lsn)],
        pin_backup: bool,
    ) -> Result<(CheckpointMetadata, Option<u64>)> {
        let mut inner = self.lock_quiesced();
        self.flush_up_to_locked(&mut inner, checkpoint_lsn)?;

        let min_active_txn_lsn = inner
            .txn_trackers
            .values()
            .filter(|t| !t.terminated && t.first_lsn <= checkpoint_lsn)
            .map(|t| t.first_lsn)
            .min();
        let min_dirty_page_lsn = dirty_pages
            .iter()
            .map(|&(_, rec_lsn)| rec_lsn)
            .filter(|&lsn| lsn > 0)
            .min();

        let min_recovery_lsn = [min_active_txn_lsn, min_dirty_page_lsn, Some(checkpoint_lsn)]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(checkpoint_lsn);

        let allocated_pin_id = if pin_backup {
            let pin_id = inner.next_pin_id;
            inner.next_pin_id += 1;
            inner.backup_pins.insert(pin_id, min_recovery_lsn);
            Some(pin_id)
        } else {
            None
        };

        let ckpt_seg_id = inner
            .segments
            .iter()
            .find(|s| {
                if let (Some(first), Some(last)) = (s.first_lsn, s.last_lsn) {
                    first <= checkpoint_lsn && checkpoint_lsn <= last
                } else {
                    false
                }
            })
            .map(|s| s.segment_id)
            .ok_or_else(|| {
                if let Some(pid) = allocated_pin_id {
                    inner.backup_pins.remove(&pid);
                }
                Error::WalCorrupted(format!(
                    "Checkpoint LSN {} not found in any tracked WAL segment",
                    checkpoint_lsn
                ))
            })?;

        let min_pinned_lsn = inner.backup_pins.values().copied().min();
        let effective_min_rec_lsn = match min_pinned_lsn {
            Some(pin_lsn) => min_recovery_lsn.min(pin_lsn),
            None => min_recovery_lsn,
        };

        let min_rec_seg_id = inner
            .segments
            .iter()
            .find(|s| {
                if let (Some(first), Some(last)) = (s.first_lsn, s.last_lsn) {
                    first <= effective_min_rec_lsn && effective_min_rec_lsn <= last
                } else {
                    false
                }
            })
            .map(|s| s.segment_id)
            .ok_or_else(|| {
                if let Some(pid) = allocated_pin_id {
                    inner.backup_pins.remove(&pid);
                }
                Error::WalCorrupted(format!(
                    "Minimum recovery LSN {} not found in any tracked WAL segment",
                    effective_min_rec_lsn
                ))
            })?;

        let active_seg_id = inner
            .segments
            .last()
            .map(|s| s.segment_id)
            .unwrap_or(ckpt_seg_id);
        let min_retained_segment_id = min_rec_seg_id.min(ckpt_seg_id).min(active_seg_id);

        let mut meta = CheckpointMetadata {
            checkpoint_lsn,
            min_recovery_lsn,
            min_retained_segment_id,
            checkpoint_segment_id: ckpt_seg_id,
            max_user_txn_id: inner.max_user_txn_id,
            max_sys_txn_id: inner.max_sys_txn_id,
            segments_reclaimed: 0,
        };

        if let Err(e) = self.write_checkpoint_metadata_to_disk(&meta) {
            if let Some(pid) = allocated_pin_id {
                inner.backup_pins.remove(&pid);
            }
            return Err(e);
        }

        let mut reclaimed_count = 0usize;
        let old_segments = std::mem::take(&mut inner.segments);
        let mut retained_segments = Vec::with_capacity(old_segments.len());
        let mut seg_iter = old_segments.into_iter();
        while let Some(seg) = seg_iter.next() {
            if seg.segment_id < min_retained_segment_id {
                if seg.path.exists()
                    && let Err(e) = std::fs::remove_file(&seg.path)
                {
                    retained_segments.push(seg);
                    retained_segments.extend(seg_iter);
                    inner.segments = retained_segments;
                    if let Some(pid) = allocated_pin_id {
                        inner.backup_pins.remove(&pid);
                    }
                    return Err(e.into());
                }
                reclaimed_count += 1;
            } else {
                retained_segments.push(seg);
            }
        }
        inner.segments = retained_segments;
        if reclaimed_count > 0 {
            if let Err(e) = sync_parent_dir(&self.path) {
                if let Some(pid) = allocated_pin_id {
                    inner.backup_pins.remove(&pid);
                }
                return Err(e);
            }
            GLOBAL_METRICS.record_wal_segments_reclaimed(reclaimed_count as u64);
        }

        inner
            .txn_trackers
            .retain(|_, t| !t.terminated || t.last_lsn >= min_recovery_lsn);
        meta.segments_reclaimed = reclaimed_count;
        inner.checkpoint_meta = Some(meta);
        self.ckpt_full_image_threshold
            .store(meta.checkpoint_lsn, std::sync::atomic::Ordering::Relaxed);
        Ok((meta, allocated_pin_id))
    }

    pub fn read_all_records(&self) -> Result<Vec<LogRecord>> {
        let inner = self.inner.lock().unwrap();
        let ckpt_meta = self.load_checkpoint_metadata_from_disk()?;
        let discovered = self.discover_segments_on_disk()?;
        if discovered.is_empty() {
            if let Some(ref meta) = ckpt_meta {
                return Err(Error::WalCorrupted(format!(
                    "Missing required WAL segment {} referenced by checkpoint",
                    meta.min_retained_segment_id
                )));
            }
            return Ok(Vec::new());
        }

        let expected_start_seg = ckpt_meta
            .as_ref()
            .map(|m| m.min_retained_segment_id)
            .or_else(|| inner.segments.first().map(|s| s.segment_id))
            .unwrap_or(0);

        let filtered: Vec<(u64, PathBuf)> = discovered
            .into_iter()
            .filter(|(seg_id, _)| *seg_id >= expected_start_seg)
            .collect();

        if filtered.is_empty() || filtered[0].0 != expected_start_seg {
            return Err(Error::WalCorrupted(format!(
                "Missing required WAL segment {}",
                expected_start_seg
            )));
        }
        for pair in filtered.windows(2) {
            let expected_next = pair[0]
                .0
                .checked_add(1)
                .ok_or_else(|| Error::WalCorrupted("Segment ID overflow".to_string()))?;
            if pair[1].0 != expected_next {
                return Err(Error::WalCorrupted(format!(
                    "Missing required WAL segment {} between {} and {}",
                    expected_next, pair[0].0, pair[1].0
                )));
            }
        }

        let mut records = Vec::new();
        let mut expected_next_lsn: Option<Lsn> = None;
        let num_filtered = filtered.len();

        for (idx, (seg_id, seg_path)) in filtered.into_iter().enumerate() {
            let is_last_segment = idx + 1 == num_filtered;
            let mut file = File::open(&seg_path)?;
            let file_len = file.metadata()?.len();
            if !is_last_segment && file_len == 0 {
                return Err(Error::WalCorrupted(format!(
                    "Empty non-final WAL segment {}",
                    seg_id
                )));
            }
            file.seek(SeekFrom::Start(0))?;

            loop {
                let mut size_buf = [0u8; 4];
                let n = file.read(&mut size_buf)?;
                if n == 0 {
                    break;
                }
                if n < 4 {
                    return Err(Error::WalCorrupted(
                        "Incomplete record length field".to_string(),
                    ));
                }
                let total_size = u32::from_le_bytes(size_buf) as usize;
                if !(51..=131_121).contains(&total_size) {
                    return Err(Error::WalCorrupted(
                        "Record length less than minimum header".to_string(),
                    ));
                }

                let mut record_buf = vec![0u8; total_size];
                record_buf[0..4].copy_from_slice(&size_buf);
                if let Err(e) = file.read_exact(&mut record_buf[4..]) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Err(Error::WalCorrupted("Truncated WAL record body".to_string()));
                    }
                    return Err(e.into());
                }

                let record = LogRecord::deserialize(&record_buf)?;
                if let Some(exp_lsn) = expected_next_lsn {
                    if record.lsn != exp_lsn {
                        return Err(Error::WalCorrupted(format!(
                            "WAL LSN discontinuity: expected {}, got {}",
                            exp_lsn, record.lsn
                        )));
                    }
                } else if let Some(ref meta) = ckpt_meta {
                    if record.lsn == INVALID_LSN || record.lsn > meta.min_recovery_lsn {
                        return Err(Error::WalCorrupted(format!(
                            "WAL starts at LSN {} which exceeds checkpoint min_recovery_lsn {}",
                            record.lsn, meta.min_recovery_lsn
                        )));
                    }
                } else if record.lsn != 1 {
                    return Err(Error::WalCorrupted(format!(
                        "Initial WAL segment 0 must start at LSN 1, found {}",
                        record.lsn
                    )));
                }
                expected_next_lsn = Some(
                    record
                        .lsn
                        .checked_add(1)
                        .ok_or_else(|| Error::WalCorrupted("LSN overflow".to_string()))?,
                );
                records.push(record);
            }
        }

        Ok(records)
    }

    pub fn pin_backup_horizon(&self, min_lsn: Lsn) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let pin_id = inner.next_pin_id;
        inner.next_pin_id += 1;
        inner.backup_pins.insert(pin_id, min_lsn);
        pin_id
    }

    pub fn unpin_backup_horizon(&self, pin_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.backup_pins.remove(&pin_id);
    }

    pub fn list_retained_segments(&self) -> Result<Vec<(u64, PathBuf)>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .segments
            .iter()
            .map(|s| (s.segment_id, s.path.clone()))
            .collect())
    }

    pub fn copy_checkpoint_meta_to<P: AsRef<Path>>(
        &self,
        dest_path: P,
    ) -> Result<Option<(u64, u32)>> {
        let src_ckpt = Self::ckpt_path(&self.path);
        if !src_ckpt.exists() {
            return Ok(None);
        }
        let data = std::fs::read(&src_ckpt)?;
        let crc = crc32c::checksum(&data);
        let path_ref = dest_path.as_ref();
        if let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut dest_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path_ref)?;
        dest_file.write_all(&data)?;
        dest_file.sync_all()?;
        sync_parent_dir(path_ref)?;
        Ok(Some((data.len() as u64, crc)))
    }

    pub fn copy_segment_to<P: AsRef<Path>>(
        &self,
        segment_id: u64,
        dest_path: P,
    ) -> Result<(u64, u32)> {
        let path_ref = dest_path.as_ref();
        if let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let (seg_path, target_len) = {
            let inner = self.inner.lock().unwrap();
            match inner.segments.iter().find(|s| s.segment_id == segment_id) {
                Some(s) => (s.path.clone(), Some(s.flushed_bytes)),
                None => (Self::segment_path(&self.path, segment_id), None),
            }
        };

        Self::copy_segment_file_prefix(&seg_path, segment_id, target_len, path_ref)
    }

    fn copy_segment_file_prefix(
        seg_path: &Path,
        segment_id: u64,
        exact_bytes: Option<u64>,
        dest_path: &Path,
    ) -> Result<(u64, u32)> {
        if !seg_path.exists() {
            return Err(Error::WalCorrupted(format!(
                "WAL segment {} not found on disk at {}",
                segment_id,
                seg_path.display()
            )));
        }

        let mut src_file = OpenOptions::new().read(true).open(seg_path)?;
        let file_len = src_file.metadata()?.len();
        let len = match exact_bytes {
            Some(target) => {
                if file_len < target {
                    return Err(Error::WalCorrupted(format!(
                        "WAL segment {} on disk ({} bytes) is shorter than flushed_bytes ({})",
                        segment_id, file_len, target
                    )));
                }
                target
            }
            None => file_len,
        };
        let mut dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)?;

        let mut hasher = Crc32cHasher::new();
        let mut buf = [0u8; 64 * 1024];
        let mut remaining = len;
        while remaining > 0 {
            let to_read = (buf.len() as u64).min(remaining) as usize;
            src_file.read_exact(&mut buf[..to_read])?;
            dest_file.write_all(&buf[..to_read])?;
            hasher.update(&buf[..to_read]);
            remaining -= to_read as u64;
        }
        dest_file.sync_all()?;
        sync_parent_dir(dest_path)?;
        Ok((len, hasher.finalize()))
    }

    #[allow(clippy::type_complexity)]
    pub fn flush_all_and_capture_cutoff<F, R>(
        &self,
        staging_dir: &Path,
        sync_db_fn: F,
    ) -> Result<(Lsn, TxnId, TxnId, u64, Vec<(String, u64, u32)>, R)>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        let mut inner = self.lock_quiesced();
        let last_lsn = inner.next_lsn.saturating_sub(1);
        if last_lsn > 0 {
            self.flush_up_to_locked(&mut inner, last_lsn)?;
        }
        let backup_end_lsn = inner.flushed_lsn;
        let max_user_txn_id = inner.max_user_txn_id;
        let max_sys_txn_id = inner.max_sys_txn_id;
        let max_segment_bytes = inner.max_segment_bytes;

        let db_res = sync_db_fn(backup_end_lsn)?;

        let mut copied_segments = Vec::with_capacity(inner.segments.len());
        for seg in &inner.segments {
            let seg_fname = seg
                .path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| Error::BackupError("Invalid WAL segment file name".to_string()))?
                .to_string();
            let dest_seg_path = staging_dir.join(&seg_fname);
            let (seg_size, seg_crc) = Self::copy_segment_file_prefix(
                &seg.path,
                seg.segment_id,
                Some(seg.flushed_bytes),
                &dest_seg_path,
            )?;
            copied_segments.push((seg_fname, seg_size, seg_crc));
        }

        Ok((
            backup_end_lsn,
            max_user_txn_id,
            max_sys_txn_id,
            max_segment_bytes,
            copied_segments,
            db_res,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_serialization_and_crc() {
        let rec = LogRecord::new_update(
            10,
            1,
            5,
            42,
            128,
            b"new value".to_vec(),
            b"old value".to_vec(),
        );
        let bytes = rec.serialize();
        let decoded = LogRecord::deserialize(&bytes).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn test_wal_arena_capacity_released_after_bulk_flush() {
        let dir = std::env::temp_dir().join(format!("mitrdb_wal_arena_cap_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal = WalManager::open_or_create(dir.join("t.wal")).unwrap();
        let target = 18 * 1024 * 1024;
        while wal.pending_bytes_len() < target {
            wal.append_record(LogRecord::new_update(
                0,
                1,
                0,
                1,
                0,
                vec![7u8; 1000],
                Vec::new(),
            ))
            .unwrap();
        }
        let cap_before = wal.inner.lock().unwrap().pending_bytes.capacity();
        wal.flush_all().unwrap();
        let (len_after, cap_after) = {
            let g = wal.inner.lock().unwrap();
            (g.pending_bytes.len(), g.pending_bytes.capacity())
        };
        println!(
            "ARENA before_flush cap={}MB after_flush len={}B cap={}MB",
            cap_before / (1024 * 1024),
            len_after,
            cap_after / (1024 * 1024)
        );
        assert_eq!(len_after, 0);
        assert!(
            cap_after < 4 * 1024 * 1024,
            "arena capacity stayed at {} bytes after flush",
            cap_after
        );
        drop(wal);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_record_corruption_detection() {
        let rec = LogRecord::new_begin(1, 100);
        let mut bytes = rec.serialize();
        bytes[10] ^= 0x55;
        let err = LogRecord::deserialize(&bytes).unwrap_err();
        match err {
            Error::WalCorrupted(msg) => assert!(msg.contains("CRC32C")),
            _ => panic!("Expected WalCorrupted error"),
        }
    }

    #[test]
    fn test_wal_serialize_append_matches_serialize_and_keeps_prefix() {
        let rec = LogRecord::new_update(
            7,
            3,
            2,
            9,
            16,
            b"redo-bytes".to_vec(),
            b"undo-bytes".to_vec(),
        );
        let standalone = rec.serialize();
        assert_eq!(standalone.len(), rec.serialized_size());

        // Appending after existing bytes must not disturb them, and the CRC must
        // cover only the appended record (so it decodes exactly like a standalone one).
        let mut arena = b"PREFIX-BYTES".to_vec();
        let prefix_len = arena.len();
        rec.serialize_append(&mut arena);
        assert_eq!(&arena[..prefix_len], b"PREFIX-BYTES");
        assert_eq!(&arena[prefix_len..], standalone.as_slice());
        assert_eq!(LogRecord::deserialize(&arena[prefix_len..]).unwrap(), rec);
    }

    #[test]
    fn test_wal_append_flush_and_read_all() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_wal_test1_{}", std::process::id()));
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let lsn1 = wal.append_record(LogRecord::new_begin(0, 101)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                101,
                lsn1,
                0,
                32,
                b"redo".to_vec(),
                b"undo".to_vec(),
            ))
            .unwrap();
        let lsn3 = wal
            .append_record(LogRecord::new_commit(0, 101, lsn2))
            .unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        assert_eq!(wal.flushed_lsn(), 0);
        wal.flush_up_to(lsn3).unwrap();
        assert_eq!(wal.flushed_lsn(), 3);

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].lsn, 1);
        assert_eq!(records[1].lsn, 2);
        assert_eq!(records[2].lsn, 3);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_wal_incomplete_tail_rejection() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_wal_test2_{}", std::process::id()));
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        wal.append_record(LogRecord::new_begin(0, 200)).unwrap();
        wal.flush_all().unwrap();

        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0x10, 0x00, 0x00]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let read_res = wal.read_all_records();
        assert!(read_res.is_err());
        drop(wal);

        let reopened = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let recovered_records = reopened.read_all_records().unwrap();
        assert_eq!(recovered_records.len(), 1);
        assert_eq!(recovered_records[0].txn_id, 200);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_wal_arena_interleaved_append_flush_and_rotation() {
        // Small segments force rotation; interleaving appends with both flush paths
        // (leader path via flush_up_to, locked path via flush_all) exercises the
        // arena offsets, drains and base tracking.
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_wal_arena_{}", std::process::id()));
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let wal = WalManager::open_with_segment_size(wal_path.as_path(), 512).unwrap();
        let mut total = 0u64;
        let mut prev = INVALID_LSN;

        let append_batch = |wal: &WalManager, n: u64, prev: &mut Lsn| -> Lsn {
            let mut last = 0;
            for i in 0..n {
                let payload = vec![(i % 251) as u8; 20 + (i % 7) as usize];
                last = wal
                    .append_record(LogRecord::new_update(
                        0,
                        42,
                        *prev,
                        (i % 13) as u32,
                        8,
                        payload.clone(),
                        payload,
                    ))
                    .unwrap();
                *prev = last;
            }
            last
        };

        let l1 = append_batch(&wal, 40, &mut prev);
        total += 40;
        wal.flush_up_to(l1 / 2).unwrap(); // leader batches everything pending
        assert!(wal.flushed_lsn() >= l1 / 2);

        let l2 = append_batch(&wal, 30, &mut prev);
        total += 30;
        wal.flush_all().unwrap(); // locked path
        assert_eq!(wal.flushed_lsn(), l2);

        let l3 = append_batch(&wal, 25, &mut prev);
        total += 25;
        wal.flush_up_to(l3).unwrap();
        assert_eq!(wal.flushed_lsn(), l3);

        assert!(wal.segment_count() > 1, "expected WAL rotation");

        // read_all_records enforces CRC and LSN continuity across segments.
        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len() as u64, total);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.lsn, i as u64 + 1);
            // payload must survive the arena round-trip intact (redo == undo, uniform fill)
            assert_eq!(r.redo_data, r.undo_data);
            assert!(r.redo_data.iter().all(|b| *b == r.redo_data[0]));
        }
        drop(wal);

        let reopened = WalManager::open_with_segment_size(wal_path.as_path(), 512).unwrap();
        assert_eq!(reopened.read_all_records().unwrap().len() as u64, total);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_flush_all_races_with_group_commit_leader() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = std::env::temp_dir().join(format!("mitrdb_wal_race_{}", std::process::id()));
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let mut handles = Vec::new();

        for t in 0..4u64 {
            let w = Arc::clone(&wal);
            handles.push(thread::spawn(move || {
                for i in 0..500u64 {
                    let txn = t * 10_000 + i + 1;
                    let b = w.append_record(LogRecord::new_begin(0, txn)).unwrap();
                    w.commit(txn, b).unwrap();
                }
            }));
        }
        let w = Arc::clone(&wal);
        handles.push(thread::spawn(move || {
            for _ in 0..300 {
                w.flush_all().unwrap();
            }
        }));
        for h in handles {
            h.join().unwrap();
        }
        wal.flush_all().unwrap();
        drop(wal);

        // read_all_records already enforces CRC + LSN continuity.
        let reopened = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let records = reopened.read_all_records().unwrap();
        assert_eq!(records.len(), 4 * 500 * 2);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
