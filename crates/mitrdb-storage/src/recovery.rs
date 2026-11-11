use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mitrdb_common::{Error, INVALID_LSN, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;

use crate::btree::{NODE_KIND_FREE, NODE_KIND_INTERNAL, NODE_KIND_LEAF};
use crate::buffer_pool::BufferPoolManager;
use crate::page::{PAGE_FLAG_FREE, PAGE_TYPE_BTREE_INTERNAL, PAGE_TYPE_BTREE_LEAF, PAGE_TYPE_RAW};
use crate::wal::{CheckpointMetadata, LogRecord, LogRecordType, WalManager};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SkippedUndo {
    pub txn_id: TxnId,
    pub page_id: PageId,
    pub offset: usize,
    pub redo_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    pub active_txns: Vec<TxnId>,
    pub committed_txns: Vec<TxnId>,
    pub aborted_txns: Vec<TxnId>,
    pub redone_records: usize,
    pub undone_records: usize,
    pub skipped_undos: Vec<SkippedUndo>,
}

#[derive(Debug, Clone, Copy)]
struct ActiveTxnEntry {
    last_lsn: Lsn,
    undo_next_lsn: Lsn,
}

pub struct RecoveryManager {
    wal_manager: Arc<WalManager>,
    bpm: Arc<BufferPoolManager>,
}

impl RecoveryManager {
    pub fn new(wal_manager: Arc<WalManager>, bpm: Arc<BufferPoolManager>) -> Self {
        let wal_for_hook = wal_manager.clone();
        bpm.set_wal_check_hook(move |page_lsn| wal_for_hook.flush_up_to(page_lsn));
        wal_manager.register_bpm(&bpm);
        RecoveryManager { wal_manager, bpm }
    }

    pub fn create_checkpoint(&self) -> Result<Lsn> {
        Ok(self.create_checkpoint_with_meta()?.checkpoint_lsn)
    }

    pub fn create_checkpoint_with_meta(&self) -> Result<CheckpointMetadata> {
        let _op_guard = self.bpm.acquire_op_exclusive();
        self.wal_manager.flush_all()?;
        self.bpm.flush_all()?;
        let dirty_pages = self.bpm.dirty_page_lsns();
        let ckpt_lsn = self
            .wal_manager
            .append_record(LogRecord::new_checkpoint(0, &dirty_pages))?;
        self.wal_manager.flush_up_to(ckpt_lsn)?;
        let meta = self
            .wal_manager
            .record_checkpoint_and_reclaim(ckpt_lsn, &dirty_pages)?;
        GLOBAL_METRICS.record_checkpoint();
        Ok(meta)
    }

    pub fn create_checkpoint_with_backup_pin(&self) -> Result<(CheckpointMetadata, u64)> {
        let _op_guard = self.bpm.acquire_op_exclusive();
        self.wal_manager.flush_all()?;
        self.bpm.flush_all()?;
        let dirty_pages = self.bpm.dirty_page_lsns();
        let ckpt_lsn = self
            .wal_manager
            .append_record(LogRecord::new_checkpoint(0, &dirty_pages))?;
        self.wal_manager.flush_up_to(ckpt_lsn)?;
        let (meta, pin_opt) = self.wal_manager.record_checkpoint_and_reclaim_with_pin(
            ckpt_lsn,
            &dirty_pages,
            true,
        )?;
        let pin_id = pin_opt.ok_or_else(|| {
            Error::BackupError("Failed to register backup horizon pin".to_string())
        })?;
        GLOBAL_METRICS.record_checkpoint();
        Ok((meta, pin_id))
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let records = self.wal_manager.truncate_torn_tail()?;
        let mut active_txns: HashMap<TxnId, ActiveTxnEntry> = HashMap::new();
        let mut committed_txns: HashSet<TxnId> = HashSet::new();
        let mut aborted_txns: HashSet<TxnId> = HashSet::new();
        let mut dirty_pages: HashMap<PageId, Lsn> = HashMap::new();
        let mut record_by_lsn: HashMap<Lsn, LogRecord> = HashMap::new();

        for r in &records {
            record_by_lsn.insert(r.lsn, r.clone());
            match r.record_type {
                LogRecordType::Begin => {
                    active_txns.insert(
                        r.txn_id,
                        ActiveTxnEntry {
                            last_lsn: r.lsn,
                            undo_next_lsn: r.lsn,
                        },
                    );
                }
                LogRecordType::Update => {
                    active_txns.insert(
                        r.txn_id,
                        ActiveTxnEntry {
                            last_lsn: r.lsn,
                            undo_next_lsn: r.lsn,
                        },
                    );
                    dirty_pages.entry(r.page_id).or_insert(r.lsn);
                }
                LogRecordType::Clr => {
                    active_txns.insert(
                        r.txn_id,
                        ActiveTxnEntry {
                            last_lsn: r.lsn,
                            undo_next_lsn: r.undo_next_lsn,
                        },
                    );
                    dirty_pages.entry(r.page_id).or_insert(r.lsn);
                }
                LogRecordType::Commit => {
                    active_txns.remove(&r.txn_id);
                    committed_txns.insert(r.txn_id);
                }
                LogRecordType::Abort => {
                    active_txns.remove(&r.txn_id);
                    aborted_txns.insert(r.txn_id);
                }
                LogRecordType::Checkpoint => {
                    let ckpt_dpt = r.checkpoint_dirty_pages()?;
                    for (pid, rec_lsn) in ckpt_dpt {
                        dirty_pages
                            .entry(pid)
                            .and_modify(|existing| {
                                if rec_lsn < *existing {
                                    *existing = rec_lsn;
                                }
                            })
                            .or_insert(rec_lsn);
                    }
                }
            }
        }

        // First full-page-image record per page. Used to rebuild torn pages from a blank page.
        let mut first_full_lsn: HashMap<PageId, Lsn> = HashMap::new();
        for r in &records {
            if (r.record_type == LogRecordType::Update || r.record_type == LogRecordType::Clr)
                && r.offset as usize == PAGE_HEADER_SIZE
                && r.redo_data.len() == PAGE_SIZE - PAGE_HEADER_SIZE
                && matches!(
                    r.redo_data.first().copied(),
                    Some(NODE_KIND_LEAF | NODE_KIND_INTERNAL | NODE_KIND_FREE)
                )
            {
                first_full_lsn.entry(r.page_id).or_insert(r.lsn);
            }
        }

        let mut redone_count = 0;
        if let Some(&min_rec_lsn) = dirty_pages.values().min() {
            for r in &records {
                if r.lsn >= min_rec_lsn
                    && (r.record_type == LogRecordType::Update
                        || r.record_type == LogRecordType::Clr)
                    && let Some(&rec_lsn) = dirty_pages.get(&r.page_id)
                    && r.lsn >= rec_lsn
                {
                    self.bpm.unmark_page_free(r.page_id);
                    let page = match self.bpm.fetch_page(r.page_id) {
                        Ok(p) => p,
                        Err(Error::PageCorrupted(msg)) => match first_full_lsn.get(&r.page_id) {
                            Some(&full_lsn) if r.lsn < full_lsn => continue,
                            Some(&full_lsn) if r.lsn == full_lsn => {
                                self.bpm.install_blank_page(r.page_id)?;
                                self.bpm.fetch_page(r.page_id)?
                            }
                            Some(_) => {
                                return Err(Error::CrashRecovery(format!(
                                    "Torn page {} met record LSN {} after its full image was skipped",
                                    r.page_id, r.lsn
                                )));
                            }
                            None => return Err(Error::PageCorrupted(msg)),
                        },
                        Err(e) => return Err(e),
                    };
                    let page_lsn = page.page_lsn();
                    self.bpm.unpin_page(r.page_id, false)?;

                    if page_lsn < r.lsn {
                        let offset = r.offset as usize;
                        let len = r.redo_data.len();
                        if offset < PAGE_HEADER_SIZE || offset + len > PAGE_SIZE {
                            return Err(Error::CrashRecovery(format!(
                                "Redo payload out of page bounds at LSN {}",
                                r.lsn
                            )));
                        }
                        self.bpm.update_page_data(r.page_id, |p| {
                            p.data[offset..offset + len].copy_from_slice(&r.redo_data);
                            if offset == PAGE_HEADER_SIZE && len == PAGE_SIZE - PAGE_HEADER_SIZE {
                                match r.redo_data[0] {
                                    NODE_KIND_LEAF => {
                                        p.set_page_type(PAGE_TYPE_BTREE_LEAF);
                                        p.set_flags(0);
                                    }
                                    NODE_KIND_INTERNAL => {
                                        p.set_page_type(PAGE_TYPE_BTREE_INTERNAL);
                                        p.set_flags(0);
                                    }
                                    NODE_KIND_FREE => {
                                        p.set_page_type(PAGE_TYPE_RAW);
                                        p.set_flags(PAGE_FLAG_FREE);
                                    }
                                    _ => {}
                                }
                            }
                            p.set_page_lsn(r.lsn);
                            Ok(())
                        })?;
                        GLOBAL_METRICS.record_recovery_redo();
                        redone_count += 1;
                    }
                }
            }
        }

        let mut undone_count = 0;
        let mut generated_clrs = std::collections::HashSet::new();
        let mut skipped_undos = Vec::new();
        while !active_txns.is_empty() {
            let max_txn = *active_txns
                .iter()
                .max_by_key(|(_, entry)| entry.undo_next_lsn)
                .map(|(txn_id, _)| txn_id)
                .unwrap();

            let entry = active_txns[&max_txn];
            let cur_lsn = entry.undo_next_lsn;
            if cur_lsn == INVALID_LSN {
                let abort_lsn = self.wal_manager.append_record(LogRecord::new_abort(
                    0,
                    max_txn,
                    entry.last_lsn,
                ))?;
                self.wal_manager.flush_up_to(abort_lsn)?;
                active_txns.remove(&max_txn);
                aborted_txns.insert(max_txn);
                continue;
            }

            let rec = match record_by_lsn.get(&cur_lsn) {
                Some(r) => r.clone(),
                None => {
                    return Err(Error::CrashRecovery(format!(
                        "Referenced LSN {} not found in WAL",
                        cur_lsn
                    )));
                }
            };

            match rec.record_type {
                LogRecordType::Update => {
                    let offset = rec.offset as usize;
                    let len = rec.undo_data.len();
                    if offset < PAGE_HEADER_SIZE || offset + len > PAGE_SIZE {
                        return Err(Error::CrashRecovery(format!(
                            "Undo payload out of page bounds at LSN {}",
                            cur_lsn
                        )));
                    }

                    let mut undo_payload = rec.undo_data.clone();
                    let is_full_btree_payload = offset == PAGE_HEADER_SIZE
                        && len == PAGE_SIZE - PAGE_HEADER_SIZE
                        && matches!(
                            rec.redo_data.first().copied(),
                            Some(NODE_KIND_LEAF | NODE_KIND_INTERNAL | NODE_KIND_FREE)
                        );
                    if is_full_btree_payload
                        && rec.page_id > 0
                        && undo_payload[0] == 0
                        && undo_payload.iter().all(|&b| b == 0)
                    {
                        undo_payload[0] = NODE_KIND_FREE;
                    }

                    let (clr_off, clr_payload) = clr_image_payload(
                        &self.bpm,
                        &self.wal_manager,
                        rec.page_id,
                        rec.offset,
                        &undo_payload,
                    )?;
                    let clr = LogRecord::new_clr(
                        0,
                        max_txn,
                        entry.last_lsn,
                        rec.page_id,
                        clr_off,
                        rec.prev_lsn,
                        clr_payload,
                    );
                    let clr_lsn = self.wal_manager.append_record(clr)?;
                    self.wal_manager.flush_up_to(clr_lsn)?;
                    generated_clrs.insert(clr_lsn);

                    self.bpm.unmark_page_free(rec.page_id);
                    let curr_page = self.bpm.fetch_page(rec.page_id)?;
                    let curr_page_lsn = curr_page.page_lsn();
                    self.bpm.unpin_page(rec.page_id, false)?;
                    eprintln!("UNDO_PAGE_CHECK: page_id={} cur_lsn={} page_lsn={}", rec.page_id, cur_lsn, curr_page_lsn);

                    let is_stale = curr_page_lsn > cur_lsn && !generated_clrs.contains(&curr_page_lsn);
                    if !is_stale {
                        self.bpm.update_page_data(rec.page_id, |p| {
                            p.data[offset..offset + len].copy_from_slice(&undo_payload);
                            if is_full_btree_payload {
                                match undo_payload[0] {
                                    NODE_KIND_LEAF => {
                                        p.set_page_type(PAGE_TYPE_BTREE_LEAF);
                                        p.set_flags(0);
                                    }
                                    NODE_KIND_INTERNAL => {
                                        p.set_page_type(PAGE_TYPE_BTREE_INTERNAL);
                                        p.set_flags(0);
                                    }
                                    NODE_KIND_FREE => {
                                        p.set_page_type(PAGE_TYPE_RAW);
                                        p.set_flags(PAGE_FLAG_FREE);
                                    }
                                    0 if rec.page_id == 0 => {
                                        p.set_page_type(PAGE_TYPE_RAW);
                                        p.set_flags(0);
                                    }
                                    _ => {}
                                }
                            }
                            p.set_page_lsn(clr_lsn);
                            Ok(())
                        })?;
                    } else {
                        eprintln!("SKIPPING stale undo slice: page_id={} cur_lsn={} page_lsn={}", rec.page_id, cur_lsn, curr_page_lsn);
                        skipped_undos.push(SkippedUndo {
                            txn_id: max_txn,
                            page_id: rec.page_id,
                            offset,
                            redo_data: rec.redo_data.clone(),
                        });
                    }

                    eprintln!("RECOVERY_UNDO: txn={} lsn={} page_id={} off={} len={} is_full={}", max_txn, cur_lsn, rec.page_id, offset, len, is_full_btree_payload);
                    GLOBAL_METRICS.record_recovery_undo();
                    undone_count += 1;

                    if rec.prev_lsn == INVALID_LSN {
                        let abort_lsn = self
                            .wal_manager
                            .append_record(LogRecord::new_abort(0, max_txn, clr_lsn))?;
                        self.wal_manager.flush_up_to(abort_lsn)?;
                        active_txns.remove(&max_txn);
                        aborted_txns.insert(max_txn);
                    } else {
                        active_txns.insert(
                            max_txn,
                            ActiveTxnEntry {
                                last_lsn: clr_lsn,
                                undo_next_lsn: rec.prev_lsn,
                            },
                        );
                    }
                }
                LogRecordType::Clr => {
                    if rec.undo_next_lsn != INVALID_LSN {
                        active_txns.insert(
                            max_txn,
                            ActiveTxnEntry {
                                last_lsn: entry.last_lsn,
                                undo_next_lsn: rec.undo_next_lsn,
                            },
                        );
                    } else {
                        let abort_lsn = self.wal_manager.append_record(LogRecord::new_abort(
                            0,
                            max_txn,
                            entry.last_lsn,
                        ))?;
                        self.wal_manager.flush_up_to(abort_lsn)?;
                        active_txns.remove(&max_txn);
                        aborted_txns.insert(max_txn);
                    }
                }
                LogRecordType::Begin => {
                    let abort_lsn = self.wal_manager.append_record(LogRecord::new_abort(
                        0,
                        max_txn,
                        entry.last_lsn,
                    ))?;
                    self.wal_manager.flush_up_to(abort_lsn)?;
                    active_txns.remove(&max_txn);
                    aborted_txns.insert(max_txn);
                }
                _ => {
                    active_txns.remove(&max_txn);
                }
            }
        }

        self.wal_manager.flush_all()?;
        self.bpm.flush_all()?;
        self.bpm.rebuild_freelist_from_disk()?;

        let mut sorted_active: Vec<TxnId> = active_txns.into_keys().collect();
        sorted_active.sort();
        let mut sorted_committed: Vec<TxnId> = committed_txns.into_iter().collect();
        sorted_committed.sort();
        let mut sorted_aborted: Vec<TxnId> = aborted_txns.into_iter().collect();
        sorted_aborted.sort();

        Ok(RecoveryReport {
            active_txns: sorted_active,
            committed_txns: sorted_committed,
            aborted_txns: sorted_aborted,
            redone_records: redone_count,
            undone_records: undone_count,
            skipped_undos,
        })
    }

    pub fn rollback_txn(&self, txn_id: TxnId) -> Result<usize> {
        if txn_id == 0 {
            return Ok(0);
        }
        self.wal_manager.flush_all()?;
        let records = self.wal_manager.read_all_records()?;
        let mut record_by_lsn: HashMap<Lsn, LogRecord> = HashMap::new();
        let mut entry_opt: Option<ActiveTxnEntry> = None;

        for r in &records {
            record_by_lsn.insert(r.lsn, r.clone());
            if r.txn_id != txn_id {
                continue;
            }
            match r.record_type {
                LogRecordType::Begin | LogRecordType::Update => {
                    entry_opt = Some(ActiveTxnEntry {
                        last_lsn: r.lsn,
                        undo_next_lsn: r.lsn,
                    });
                }
                LogRecordType::Clr => {
                    entry_opt = Some(ActiveTxnEntry {
                        last_lsn: r.lsn,
                        undo_next_lsn: r.undo_next_lsn,
                    });
                }
                LogRecordType::Commit | LogRecordType::Abort => {
                    entry_opt = None;
                }
                LogRecordType::Checkpoint => {}
            }
        }

        let Some(mut entry) = entry_opt else {
            return Ok(0);
        };

        let mut undone_count = 0usize;
        let mut generated_clrs = std::collections::HashSet::new();
        loop {
            let cur_lsn = entry.undo_next_lsn;
            if cur_lsn == INVALID_LSN {
                self.wal_manager.abort(txn_id, entry.last_lsn)?;
                break;
            }

            let rec = match record_by_lsn.get(&cur_lsn) {
                Some(r) => r.clone(),
                None => {
                    return Err(Error::CrashRecovery(format!(
                        "Referenced LSN {} not found in WAL",
                        cur_lsn
                    )));
                }
            };

            match rec.record_type {
                LogRecordType::Update => {
                    let offset = rec.offset as usize;
                    let len = rec.undo_data.len();
                    if offset < PAGE_HEADER_SIZE || offset + len > PAGE_SIZE {
                        return Err(Error::CrashRecovery(format!(
                            "Undo payload out of page bounds at LSN {}",
                            cur_lsn
                        )));
                    }

                    let mut undo_payload = rec.undo_data.clone();
                    let is_full_btree_payload = offset == PAGE_HEADER_SIZE
                        && len == PAGE_SIZE - PAGE_HEADER_SIZE
                        && matches!(
                            rec.redo_data.first().copied(),
                            Some(NODE_KIND_LEAF | NODE_KIND_INTERNAL | NODE_KIND_FREE)
                        );
                    if is_full_btree_payload
                        && rec.page_id > 0
                        && undo_payload[0] == 0
                        && undo_payload.iter().all(|&b| b == 0)
                    {
                        undo_payload[0] = NODE_KIND_FREE;
                    }

                    let _op_guard = self.bpm.acquire_op_shared();
                    let (clr_off, clr_payload) = clr_image_payload(
                        &self.bpm,
                        &self.wal_manager,
                        rec.page_id,
                        rec.offset,
                        &undo_payload,
                    )?;
                    let clr = LogRecord::new_clr(
                        0,
                        txn_id,
                        entry.last_lsn,
                        rec.page_id,
                        clr_off,
                        rec.prev_lsn,
                        clr_payload,
                    );
                    let clr_lsn = self.wal_manager.append_record(clr)?;
                    self.wal_manager.flush_up_to(clr_lsn)?;
                    generated_clrs.insert(clr_lsn);

                    self.bpm.unmark_page_free(rec.page_id);
                    self.bpm.update_page_data(rec.page_id, |p| {
                        p.data[offset..offset + len].copy_from_slice(&undo_payload);
                        if is_full_btree_payload {
                            match undo_payload[0] {
                                NODE_KIND_LEAF => {
                                    p.set_page_type(PAGE_TYPE_BTREE_LEAF);
                                    p.set_flags(0);
                                }
                                NODE_KIND_INTERNAL => {
                                    p.set_page_type(PAGE_TYPE_BTREE_INTERNAL);
                                    p.set_flags(0);
                                }
                                NODE_KIND_FREE => {
                                    p.set_page_type(PAGE_TYPE_RAW);
                                    p.set_flags(PAGE_FLAG_FREE);
                                }
                                0 if rec.page_id == 0 => {
                                    p.set_page_type(PAGE_TYPE_RAW);
                                    p.set_flags(0);
                                }
                                _ => {}
                            }
                        }
                        p.set_page_lsn(clr_lsn);
                        Ok(())
                    })?;
                    if is_full_btree_payload && undo_payload[0] == NODE_KIND_FREE {
                        self.bpm.stage_free_page(txn_id, rec.page_id)?;
                    }
                    drop(_op_guard);

                    GLOBAL_METRICS.record_recovery_undo();
                    undone_count += 1;

                    if rec.prev_lsn == INVALID_LSN {
                        self.wal_manager.abort(txn_id, clr_lsn)?;
                        break;
                    } else {
                        entry = ActiveTxnEntry {
                            last_lsn: clr_lsn,
                            undo_next_lsn: rec.prev_lsn,
                        };
                    }
                }
                LogRecordType::Clr if rec.undo_next_lsn != INVALID_LSN => {
                    entry = ActiveTxnEntry {
                        last_lsn: entry.last_lsn,
                        undo_next_lsn: rec.undo_next_lsn,
                    };
                }
                _ => {
                    self.wal_manager.abort(txn_id, entry.last_lsn)?;
                    break;
                }
            }
        }

        Ok(undone_count)
    }
}

/// CLR payload: if the page needs a full-page image after the last checkpoint, log the
/// whole post-undo body (same rule as btree write_node) so a torn page can be rebuilt.
fn clr_image_payload(
    bpm: &crate::buffer_pool::BufferPoolManager,
    wal: &crate::wal::WalManager,
    page_id: PageId,
    offset: u16,
    undo_payload: &[u8],
) -> Result<(u16, Vec<u8>)> {
    let off = offset as usize;
    if off == PAGE_HEADER_SIZE && undo_payload.len() == PAGE_SIZE - PAGE_HEADER_SIZE {
        return Ok((offset, undo_payload.to_vec()));
    }
    if off < PAGE_HEADER_SIZE || off + undo_payload.len() > PAGE_SIZE {
        return Ok((offset, undo_payload.to_vec()));
    }
    let page = bpm.fetch_page(page_id)?;
    let page_lsn = page.page_lsn();
    let ptype = page.page_type();
    let mut body = page.data[PAGE_HEADER_SIZE..PAGE_SIZE].to_vec();
    bpm.unpin_page(page_id, false)?;
    let is_btree = ptype == PAGE_TYPE_BTREE_LEAF || ptype == PAGE_TYPE_BTREE_INTERNAL;
    if !is_btree || !wal.needs_full_page_image(page_lsn) {
        return Ok((offset, undo_payload.to_vec()));
    }
    let start = off - PAGE_HEADER_SIZE;
    body[start..start + undo_payload.len()].copy_from_slice(undo_payload);
    if !matches!(body.first().copied(), Some(NODE_KIND_LEAF | NODE_KIND_INTERNAL)) {
        return Ok((offset, undo_payload.to_vec()));
    }
    Ok((PAGE_HEADER_SIZE as u16, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::page::PAGE_TYPE_RAW;

    #[test]
    fn test_recovery_committed_transaction_redo() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_rec_test1_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
            let _bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

            let lsn1 = wal.append_record(LogRecord::new_begin(0, 10)).unwrap();
            let lsn2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    10,
                    lsn1,
                    pid,
                    64,
                    b"committed payload".to_vec(),
                    b"initial zero bytes".to_vec(),
                ))
                .unwrap();
            let lsn3 = wal
                .append_record(LogRecord::new_commit(0, 10, lsn2))
                .unwrap();
            wal.flush_up_to(lsn3).unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));

            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover().unwrap();

            assert_eq!(report.committed_txns, vec![10]);
            assert_eq!(report.aborted_txns, Vec::<TxnId>::new());
            assert_eq!(report.redone_records, 1);
            assert_eq!(report.undone_records, 0);

            let page = bpm.fetch_page(0).unwrap();
            assert_eq!(&page.data[64..64 + 17], b"committed payload");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_recovery_torn_page_rebuilt_from_full_image() {
        use std::io::{Seek, SeekFrom, Write};
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_rec_torn_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let body_len = PAGE_SIZE - PAGE_HEADER_SIZE;
        let pid;

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let (p, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
            pid = p;
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let mut redo = vec![0u8; body_len];
            redo[0] = crate::btree::NODE_KIND_LEAF;
            redo[100..105].copy_from_slice(b"hello");
            let lsn1 = wal.append_record(LogRecord::new_begin(0, 10)).unwrap();
            let lsn2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    10,
                    lsn1,
                    pid,
                    PAGE_HEADER_SIZE as u16,
                    redo,
                    vec![0u8; body_len],
                ))
                .unwrap();
            let lsn3 = wal
                .append_record(LogRecord::new_commit(0, 10, lsn2))
                .unwrap();
            wal.flush_up_to(lsn3).unwrap();
        }

        // Simulate a torn write: only the first 2 KB of the page hit the disk.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&db_path)
                .unwrap();
            f.seek(SeekFrom::Start(pid as u64 * PAGE_SIZE as u64)).unwrap();
            f.write_all(&[0xABu8; 2048]).unwrap();
            f.sync_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover().unwrap();
            assert_eq!(report.redone_records, 1);
            let page = bpm.fetch_page(pid).unwrap();
            assert_eq!(
                &page.data[PAGE_HEADER_SIZE + 100..PAGE_HEADER_SIZE + 105],
                b"hello"
            );
            assert_eq!(page.page_type(), crate::page::PAGE_TYPE_BTREE_LEAF);
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_recovery_uncommitted_transaction_undo() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_rec_test2_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

            let lsn1 = wal.append_record(LogRecord::new_begin(0, 20)).unwrap();
            let lsn2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    20,
                    lsn1,
                    pid,
                    64,
                    b"loser change".to_vec(),
                    b"clean origin".to_vec(),
                ))
                .unwrap();
            wal.flush_up_to(lsn2).unwrap();

            bpm.update_page_data(pid, |p| {
                p.data[64..64 + 12].copy_from_slice(b"loser change");
                p.set_page_lsn(lsn2);
                Ok(())
            })
            .unwrap();
            bpm.flush_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));

            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover().unwrap();

            assert_eq!(report.aborted_txns, vec![20]);
            assert_eq!(report.undone_records, 1);

            let page = bpm.fetch_page(0).unwrap();
            assert_eq!(&page.data[64..64 + 12], b"clean origin");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_recovery_idempotence_on_restart() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_rec_test3_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

            let lsn1 = wal.append_record(LogRecord::new_begin(0, 30)).unwrap();
            let lsn2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    30,
                    lsn1,
                    pid,
                    64,
                    b"should be undone".to_vec(),
                    b"safe initial data".to_vec(),
                ))
                .unwrap();
            wal.flush_up_to(lsn2).unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover().unwrap();
            assert_eq!(report.aborted_txns, vec![30]);
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report2 = recovery.recover().unwrap();
            assert_eq!(report2.undone_records, 0);

            let page = bpm.fetch_page(0).unwrap();
            assert_eq!(&page.data[64..64 + 17], b"safe initial data");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
