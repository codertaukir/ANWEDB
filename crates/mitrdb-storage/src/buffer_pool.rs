use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use mitrdb_common::{Error, Lsn, PageId, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;

use crate::disk::DiskManager;
use crate::page::{PAGE_FLAG_FREE, Page};

const PAGE_TABLE_SHARDS: usize = 64;

#[inline]
fn shard_of(page_id: PageId) -> usize {
    (page_id as usize) & (PAGE_TABLE_SHARDS - 1)
}

pub struct Frame {
    pub page: Page,
    pub page_id: Option<PageId>,
    pub pin_count: AtomicU32,
    pub is_dirty: AtomicBool,
    pub ref_bit: AtomicBool,
}

pub type WalCheckHook = Box<dyn Fn(Lsn) -> Result<()> + Send + Sync>;

pub struct BufferPoolManager {
    disk_manager: Arc<DiskManager>,
    deferred_free_txns: Mutex<Vec<TxnId>>,
    frames: Box<[RwLock<Frame>]>,
    free_frames: Mutex<Vec<usize>>,
    page_tables: Box<[RwLock<HashMap<PageId, usize>>; PAGE_TABLE_SHARDS]>,
    clock_hand: Mutex<usize>,
    pool_size: usize,
    wal_check_fn: Mutex<Option<WalCheckHook>>,
    /// Test-only: allow n page ops (fetch/mutate/update), then fail one (-1 = off).
    #[cfg(feature = "fault-injection")]
    fail_page_op_after: std::sync::atomic::AtomicI64,
    #[cfg(feature = "fault-injection")]
    last_injection_trace: Mutex<Option<String>>,
    #[cfg(feature = "fault-injection")]
    fail_miss_after: std::sync::atomic::AtomicI64,
}

impl BufferPoolManager {
    pub fn new(disk_manager: Arc<DiskManager>, pool_size: usize) -> Self {
        assert!(pool_size > 0);
        let mut frames = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            frames.push(RwLock::new(Frame {
                page: Page::new_empty(0, 0),
                page_id: None,
                pin_count: AtomicU32::new(0),
                is_dirty: AtomicBool::new(false),
                ref_bit: AtomicBool::new(false),
            }));
        }
        BufferPoolManager {
            disk_manager,
            deferred_free_txns: Mutex::new(Vec::new()),
            frames: frames.into_boxed_slice(),
            free_frames: Mutex::new((0..pool_size).rev().collect()),
            page_tables: Box::new(std::array::from_fn(|_| RwLock::new(HashMap::new()))),
            clock_hand: Mutex::new(0),
            pool_size,
            wal_check_fn: Mutex::new(None),
            #[cfg(feature = "fault-injection")]
            fail_page_op_after: std::sync::atomic::AtomicI64::new(-1),
            #[cfg(feature = "fault-injection")]
            last_injection_trace: Mutex::new(None),
            #[cfg(feature = "fault-injection")]
            fail_miss_after: std::sync::atomic::AtomicI64::new(-1),
        }
    }

    fn lock_all_shards_write(&self) -> Vec<RwLockWriteGuard<'_, HashMap<PageId, usize>>> {
        let mut guards = Vec::with_capacity(PAGE_TABLE_SHARDS);
        for s in self.page_tables.iter() {
            guards.push(s.write().unwrap());
        }
        guards
    }

    fn lock_all_shards_read(&self) -> Vec<RwLockReadGuard<'_, HashMap<PageId, usize>>> {
        let mut guards = Vec::with_capacity(PAGE_TABLE_SHARDS);
        for s in self.page_tables.iter() {
            guards.push(s.read().unwrap());
        }
        guards
    }

    pub fn acquire_op_shared(&self) -> RwLockReadGuard<'_, ()> {
        self.disk_manager.acquire_op_shared()
    }

    pub fn acquire_op_exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.disk_manager.acquire_op_exclusive()
    }

    pub fn set_wal_check_hook<F>(&self, hook: F)
    where
        F: Fn(Lsn) -> Result<()> + Send + Sync + 'static,
    {
        let mut guard = self.wal_check_fn.lock().unwrap();
        *guard = Some(Box::new(hook));
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_fail_page_op_after(&self, n: i64) {
        self.fail_page_op_after
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "fault-injection")]
    pub fn take_injection_trace(&self) -> Option<String> {
        self.last_injection_trace.lock().unwrap().take()
    }

    #[cfg(feature = "fault-injection")]
    fn inject_page_op_fail(&self) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let inj = self.fail_page_op_after.load(Relaxed);
        if inj >= 0 {
            if inj == 0 {
                self.fail_page_op_after.store(-1, Relaxed);
                *self.last_injection_trace.lock().unwrap() =
                    Some(std::backtrace::Backtrace::force_capture().to_string());
                return Err(Error::Io("injected page op failure".to_string()));
            }
            self.fail_page_op_after.store(inj - 1, Relaxed);
        }
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_fail_miss_after(&self, n: i64) {
        self.fail_miss_after
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "fault-injection")]
    fn inject_page_op_fail_miss(&self) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let inj = self.fail_miss_after.load(Relaxed);
        if inj >= 0 {
            if inj == 0 {
                self.fail_miss_after.store(-1, Relaxed);
                *self.last_injection_trace.lock().unwrap() =
                    Some(std::backtrace::Backtrace::force_capture().to_string());
                return Err(Error::Io("injected page read failure (miss)".to_string()));
            }
            self.fail_miss_after.store(inj - 1, Relaxed);
        }
        Ok(())
    }

    /// Lock-free / shared-read fast path on cache hits; acquires all shards only on miss/eviction.
    fn pin_or_load_frame(&self, page_id: PageId) -> Result<usize> {
        let s_idx = shard_of(page_id);
        {
            let shard = self.page_tables[s_idx].read().unwrap();
            if let Some(&frame_idx) = shard.get(&page_id) {
                let frame = self.frames[frame_idx].read().unwrap();
                frame.pin_count.fetch_add(1, Ordering::AcqRel);
                frame.ref_bit.store(true, Ordering::Release);
                GLOBAL_METRICS.record_hit();
                return Ok(frame_idx);
            }
            if self.disk_manager.is_page_free(page_id) {
                return Err(Error::InvalidPageId(page_id));
            }
        }

        // Cache miss: acquire shards in canonical order 0..64 to guarantee deadlock freedom during eviction
        let mut shards = self.lock_all_shards_write();
        if self.disk_manager.is_page_free(page_id) {
            return Err(Error::InvalidPageId(page_id));
        }
        if let Some(&frame_idx) = shards[s_idx].get(&page_id) {
            let frame = self.frames[frame_idx].read().unwrap();
            frame.pin_count.fetch_add(1, Ordering::AcqRel);
            frame.ref_bit.store(true, Ordering::Release);
            GLOBAL_METRICS.record_hit();
            return Ok(frame_idx);
        }

        GLOBAL_METRICS.record_miss();
        #[cfg(feature = "fault-injection")]
        self.inject_page_op_fail_miss()?;
        let page_on_disk = self.disk_manager.read_page(page_id)?;
        let victim_idx = match self.free_frames.lock().unwrap().pop() {
            Some(idx) => idx,
            None => self.find_victim_frame_sharded(&mut shards)?,
        };

        let mut frame = self.frames[victim_idx].write().unwrap();
        frame.page = page_on_disk;
        frame.page_id = Some(page_id);
        frame.pin_count.store(1, Ordering::Release);
        frame.is_dirty.store(false, Ordering::Release);
        frame.ref_bit.store(true, Ordering::Release);

        shards[s_idx].insert(page_id, victim_idx);
        Ok(victim_idx)
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<Page> {
        #[cfg(feature = "fault-injection")]
        self.inject_page_op_fail()?;
        let frame_idx = self.pin_or_load_frame(page_id)?;
        let frame = self.frames[frame_idx].read().unwrap();
        Ok(frame.page.clone())
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let result = {
            let shard = self.page_tables[shard_of(page_id)].read().unwrap();
            if let Some(&frame_idx) = shard.get(&page_id) {
                let frame = self.frames[frame_idx].read().unwrap();
                let prev = frame.pin_count.load(Ordering::Acquire);
                if prev == 0 {
                    Err(Error::Io(format!(
                        "Attempted to unpin page {} with pin count 0",
                        page_id
                    )))
                } else {
                    frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                    if is_dirty {
                        frame.is_dirty.store(true, Ordering::Release);
                    }
                    Ok(())
                }
            } else {
                Err(Error::InvalidPageId(page_id))
            }
        };
        if let Some(started) = profile_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::BufferPoolUnpin,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    pub fn with_page<F, R>(&self, page_id: PageId, f: F) -> Result<R>
    where
        F: FnOnce(&Page) -> Result<R>,
    {
        #[cfg(feature = "fault-injection")]
        self.inject_page_op_fail()?;
        let frame_idx = self.pin_or_load_frame(page_id)?;
        let frame = self.frames[frame_idx].read().unwrap();
        let res = f(&frame.page);
        frame.pin_count.fetch_sub(1, Ordering::AcqRel);
        res
    }

    pub fn update_page_data<F, R>(&self, page_id: PageId, f: F) -> Result<R>
    where
        F: FnOnce(&mut Page) -> Result<R>,
    {
        #[cfg(feature = "fault-injection")]
        self.inject_page_op_fail()?;
        let frame_idx = self.pin_or_load_frame(page_id)?;

        let mut working_page = {
            let frame = self.frames[frame_idx].read().unwrap();
            frame.page.clone()
        };
        let result = f(&mut working_page);
        let mut frame = self.frames[frame_idx].write().unwrap();
        match result {
            Ok(val) => {
                if working_page.page_id() != page_id {
                    frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                    return Err(Error::PageCorrupted(
                        "Page ID modified during update_page_data".to_string(),
                    ));
                }
                if let Err(e) = working_page.validate_structure() {
                    frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                    return Err(e);
                }
                frame.page = working_page;
                frame.is_dirty.store(true, Ordering::Release);
                frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                Ok(val)
            }
            Err(e) => {
                frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }

    pub fn mutate_page<F, R>(&self, page_id: PageId, f: F) -> Result<R>
    where
        F: FnOnce(&mut Page) -> Result<R>,
    {
        #[cfg(feature = "fault-injection")]
        self.inject_page_op_fail()?;
        let frame_idx = self.pin_or_load_frame(page_id)?;
        let mut frame = self.frames[frame_idx].write().unwrap();
        let result = f(&mut frame.page);
        frame.pin_count.fetch_sub(1, Ordering::AcqRel);
        match result {
            Ok(val) => {
                if frame.page.page_id() != page_id {
                    return Err(Error::PageCorrupted(
                        "Page ID modified during mutate_page".to_string(),
                    ));
                }
                frame.page.validate_structure()?;
                frame.is_dirty.store(true, Ordering::Release);
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub fn new_page(&self, page_type: u16) -> Result<(PageId, Page)> {
        let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let result = (|| {
            if self.disk_manager.free_page_count() == 0 {
                let has_deferred = !self.deferred_free_txns.lock().unwrap().is_empty();
                if !has_deferred
                    && let Some(victim_idx) = self.free_frames.lock().unwrap().pop()
                {
                    let (new_page_id, new_page) = self.disk_manager.allocate_page(page_type)?;
                    let mut target_shard = self.page_tables[shard_of(new_page_id)].write().unwrap();
                    if !target_shard.contains_key(&new_page_id) {
                        let mut frame = self.frames[victim_idx].write().unwrap();
                        frame.page = new_page.clone();
                        frame.page_id = Some(new_page_id);
                        frame.pin_count.store(1, Ordering::Release);
                        frame.is_dirty.store(false, Ordering::Release);
                        frame.ref_bit.store(true, Ordering::Release);
                        target_shard.insert(new_page_id, victim_idx);
                        return Ok((new_page_id, new_page));
                    }
                    drop(target_shard);
                    let _ = self.disk_manager.free_page(new_page_id);
                    self.free_frames.lock().unwrap().push(victim_idx);
                }
            }
            let mut deferred_txns = self.deferred_free_txns.lock().unwrap();
            let mut shards = self.lock_all_shards_write();

            self.process_deferred_free_txns_locked(
                &mut deferred_txns,
                &mut shards,
                None,
            )?;

            let mut resident_pids: Vec<(PageId, usize)> = Vec::new();
            for s in shards.iter() {
                for (&pid, &idx) in s.iter() {
                    resident_pids.push((pid, idx));
                }
            }
            for (pid, idx) in resident_pids {
                if !self.disk_manager.is_page_free(pid) {
                    continue;
                }
                let mut frame = self.frames[idx].write().unwrap();
                if frame.pin_count.load(Ordering::Acquire) > 0 {
                    self.disk_manager.unmark_page_free(pid);
                    if (frame.page.flags() & PAGE_FLAG_FREE) != 0 {
                        self.disk_manager.stage_free_page(0, pid);
                        if !deferred_txns.contains(&0) {
                            deferred_txns.push(0);
                        }
                    }
                } else if frame.is_dirty.load(Ordering::Acquire) && (frame.page.flags() & PAGE_FLAG_FREE) == 0 {
                    self.disk_manager.unmark_page_free(pid);
                } else {
                    frame.page_id = None;
                    frame.pin_count.store(0, Ordering::Release);
                    frame.is_dirty.store(false, Ordering::Release);
                    frame.ref_bit.store(false, Ordering::Release);
                    shards[shard_of(pid)].remove(&pid);
                }
            }

            let victim_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
            let victim_idx = match self.free_frames.lock().unwrap().pop() {
                Some(idx) => idx,
                None => self.find_victim_frame_sharded(&mut shards)?,
            };
            if let Some(started) = victim_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::BufferPoolVictimSelect,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            let allocate_started =
                mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
            let mut excluded_candidates: [PageId; 8] = [0; 8];
            let mut excluded_count = 0usize;

            let (new_page_id, new_page) = loop {
                let (cand_pid, cand_page) =
                    self.disk_manager
                        .allocate_page_with_filter(page_type, |cand_pid| {
                            !shards[shard_of(cand_pid)].contains_key(&cand_pid)
                                && !excluded_candidates[..excluded_count].contains(&cand_pid)
                        })?;

                let cs = shard_of(cand_pid);
                if let Some(&existing_idx) = shards[cs].get(&cand_pid) {
                    let mut existing = self.frames[existing_idx].write().unwrap();
                    if existing.pin_count.load(Ordering::Acquire) > 0
                        || (existing.is_dirty.load(Ordering::Acquire)
                            && (existing.page.flags() & PAGE_FLAG_FREE) == 0)
                    {
                        drop(existing);
                        let _ = self.disk_manager.free_page(cand_pid);
                        if excluded_count < excluded_candidates.len() {
                            excluded_candidates[excluded_count] = cand_pid;
                            excluded_count += 1;
                            continue;
                        } else {
                            return Err(Error::Io(format!(
                                "Allocated page {} conflicts with pinned or dirty active frame after retry exhaust",
                                cand_pid
                            )));
                        }
                    }
                    existing.page_id = None;
                    existing.pin_count.store(0, Ordering::Release);
                    existing.is_dirty.store(false, Ordering::Release);
                    existing.ref_bit.store(false, Ordering::Release);
                    shards[cs].remove(&cand_pid);
                }

                break (cand_pid, cand_page);
            };

            if let Some(started) = allocate_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::BufferPoolDiskAllocate,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            let mut frame = self.frames[victim_idx].write().unwrap();
            frame.page = new_page.clone();
            frame.page_id = Some(new_page_id);
            frame.pin_count.store(1, Ordering::Release);
            frame.is_dirty.store(false, Ordering::Release);
            frame.ref_bit.store(true, Ordering::Release);

            shards[shard_of(new_page_id)].insert(new_page_id, victim_idx);
            Ok((new_page_id, new_page))
        })();
        if let Some(started) = profile_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::BufferPoolNewPage,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    pub fn free_page(&self, page_id: PageId) -> Result<()> {
        let _op_guard = self.acquire_op_shared();
        self.free_page_in_op(page_id)
    }

    pub(crate) fn free_page_in_op(&self, page_id: PageId) -> Result<()> {
        let mut shard = self.page_tables[shard_of(page_id)].write().unwrap();
        if let Some(&frame_idx) = shard.get(&page_id) {
            let mut frame = self.frames[frame_idx].write().unwrap();
            if frame.pin_count.load(Ordering::Acquire) > 0 {
                return Err(Error::Io(format!(
                    "Attempted to free pinned page {}",
                    page_id
                )));
            }
            self.disk_manager.free_page(page_id)?;
            frame.is_dirty.store(false, Ordering::Release);
            frame.page_id = None;
            frame.pin_count.store(0, Ordering::Release);
            frame.ref_bit.store(false, Ordering::Release);
            shard.remove(&page_id);
            self.free_frames.lock().unwrap().push(frame_idx);
            return Ok(());
        }
        self.disk_manager.free_page(page_id)
    }

    pub fn stage_free_page(&self, txn_id: TxnId, page_id: PageId) -> Result<()> {
        if page_id == 0 {
            return Err(Error::TransactionError(
                "Cannot free reserved page 0".to_string(),
            ));
        }
        self.disk_manager.stage_free_page(txn_id, page_id);
        Ok(())
    }

    pub fn finalize_free_pages(&self, txn_id: TxnId) -> Result<()> {
        let _op_guard = self.acquire_op_shared();
        let mut deferred_txns = self.deferred_free_txns.lock().unwrap();
        let mut shards = self.lock_all_shards_write();
        self.process_deferred_free_txns_locked(
            &mut deferred_txns,
            &mut shards,
            Some(txn_id),
        )
    }

    fn process_deferred_free_txns_locked(
        &self,
        deferred_txns: &mut Vec<TxnId>,
        shards: &mut [RwLockWriteGuard<'_, HashMap<PageId, usize>>],
        extra_txn_id: Option<TxnId>,
    ) -> Result<()> {
        let mut txns_to_process: Vec<TxnId> = std::mem::take(deferred_txns);
        if let Some(txn_id) = extra_txn_id
            && !txns_to_process.contains(&txn_id)
        {
            txns_to_process.push(txn_id);
        }

        let mut first_err: Option<Error> = None;

        for tid in txns_to_process {
            let staged = self.disk_manager.take_pending_free_pages(tid);
            let mut keep_tid_deferred = false;

            let mut to_free: Vec<(PageId, Option<usize>)> = Vec::new();
            for pid in staged {
                if pid == 0 {
                    continue;
                }
                if let Some(&frame_idx) = shards[shard_of(pid)].get(&pid) {
                    let frame = self.frames[frame_idx].read().unwrap();
                    if frame.pin_count.load(Ordering::Acquire) > 0 {
                        self.disk_manager.stage_free_page(tid, pid);
                        keep_tid_deferred = true;
                    } else if (frame.page.flags() & PAGE_FLAG_FREE) != 0 {
                        to_free.push((pid, Some(frame_idx)));
                    }
                } else if self.disk_manager.is_page_free(pid) {
                    continue;
                } else {
                    match self.disk_manager.read_page(pid) {
                        Ok(p) => {
                            if (p.flags() & PAGE_FLAG_FREE) != 0 {
                                to_free.push((pid, None));
                            }
                        }
                        Err(e) => {
                            self.disk_manager.stage_free_page(tid, pid);
                            keep_tid_deferred = true;
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                }
            }

            if !to_free.is_empty() {
                let pids: Vec<PageId> = to_free.iter().map(|(p, _)| *p).collect();
                match self.disk_manager.free_pages_batch(&pids) {
                    Ok(()) => {
                        for (pid, fidx) in &to_free {
                            if let Some(frame_idx) = *fidx {
                                let mut frame = self.frames[frame_idx].write().unwrap();
                                frame.is_dirty.store(false, Ordering::Release);
                                frame.page_id = None;
                                frame.pin_count.store(0, Ordering::Release);
                                frame.ref_bit.store(false, Ordering::Release);
                                shards[shard_of(*pid)].remove(pid);
                            }
                        }
                    }
                    Err(e) => {
                        for pid in &pids {
                            self.disk_manager.stage_free_page(tid, *pid);
                        }
                        keep_tid_deferred = true;
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }

            if keep_tid_deferred && !deferred_txns.contains(&tid) {
                deferred_txns.push(tid);
            }
        }

        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    pub fn install_blank_page(&self, page_id: PageId) -> Result<()> {
        let mut shards = self.lock_all_shards_write();
        let s = shard_of(page_id);
        if shards[s].contains_key(&page_id) {
            return Ok(());
        }
        let victim_idx = self.find_victim_frame_sharded(&mut shards)?;
        let mut frame = self.frames[victim_idx].write().unwrap();
        frame.page = Page::new_empty(page_id, crate::page::PAGE_TYPE_RAW);
        frame.page_id = Some(page_id);
        frame.pin_count.store(0, Ordering::Release);
        frame.is_dirty.store(true, Ordering::Release);
        frame.ref_bit.store(true, Ordering::Release);
        shards[s].insert(page_id, victim_idx);
        Ok(())
    }

    pub fn unmark_page_free(&self, page_id: PageId) {
        self.disk_manager.unmark_page_free(page_id);
    }

    pub fn rebuild_freelist_from_disk(&self) -> Result<()> {
        let _op_guard = self.acquire_op_shared();
        let mut deferred_txns = self.deferred_free_txns.lock().unwrap();
        let mut shards = self.lock_all_shards_write();

        let mut dirty_batch = Vec::new();
        for s in shards.iter() {
            for (&page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    self.enforce_wal_rule(frame.page.page_lsn())?;
                    dirty_batch.push((page_id, frame.page.clone()));
                }
            }
        }
        self.disk_manager.write_pages_batch(&dirty_batch)?;
        self.disk_manager.sync()?;
        for s in shards.iter() {
            for (&_page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    frame.is_dirty.store(false, Ordering::Release);
                }
            }
        }

        let mut pinned_pages = std::collections::HashSet::new();
        for s in shards.iter() {
            for (&pid, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.pin_count.load(Ordering::Acquire) > 0 {
                    pinned_pages.insert(pid);
                }
            }
        }

        let free_list = self
            .disk_manager
            .rebuild_freelist_from_disk_excluding(&pinned_pages)?;
        for pid in free_list {
            let s = shard_of(pid);
            if let Some(&frame_idx) = shards[s].get(&pid) {
                let mut frame = self.frames[frame_idx].write().unwrap();
                if frame.pin_count.load(Ordering::Acquire) == 0 {
                    frame.is_dirty.store(false, Ordering::Release);
                    frame.page_id = None;
                    frame.pin_count.store(0, Ordering::Release);
                    frame.ref_bit.store(false, Ordering::Release);
                    shards[s].remove(&pid);
                }
            }
        }

        *deferred_txns = self.disk_manager.pending_free_txn_ids();
        Ok(())
    }

    pub fn free_page_count(&self) -> usize {
        self.disk_manager.free_page_count()
    }

    pub fn is_page_free(&self, page_id: PageId) -> bool {
        self.disk_manager.is_page_free(page_id)
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let shard = self.page_tables[shard_of(page_id)].read().unwrap();

        if let Some(&frame_idx) = shard.get(&page_id) {
            let frame = self.frames[frame_idx].write().unwrap();
            if frame.is_dirty.load(Ordering::Acquire) {
                self.enforce_wal_rule(frame.page.page_lsn())?;
                self.disk_manager.write_page(page_id, &frame.page)?;
                self.disk_manager.sync()?;
                frame.is_dirty.store(false, Ordering::Release);
            }
        }
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let shards = self.lock_all_shards_read();

        let mut dirty_batch = Vec::new();
        for s in shards.iter() {
            for (&page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    self.enforce_wal_rule(frame.page.page_lsn())?;
                    dirty_batch.push((page_id, frame.page.clone()));
                }
            }
        }
        self.disk_manager.write_pages_batch(&dirty_batch)?;
        self.disk_manager.sync()?;
        for s in shards.iter() {
            for (&_page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    frame.is_dirty.store(false, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    pub fn dirty_page_lsns(&self) -> Vec<(PageId, Lsn)> {
        let shards = self.lock_all_shards_read();
        let mut out = Vec::new();
        for s in shards.iter() {
            for (&page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) && frame.page.page_lsn() > 0 {
                    out.push((page_id, frame.page.page_lsn()));
                }
            }
        }
        out
    }

    pub fn flush_all_and_run_cutoff<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> Result<R>,
    {
        let shards = self.lock_all_shards_read();

        let mut dirty_batch = Vec::new();
        for s in shards.iter() {
            for (&page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    self.enforce_wal_rule(frame.page.page_lsn())?;
                    dirty_batch.push((page_id, frame.page.clone()));
                }
            }
        }
        self.disk_manager.write_pages_batch(&dirty_batch)?;
        self.disk_manager.sync()?;
        for s in shards.iter() {
            for (&_page_id, &frame_idx) in s.iter() {
                let frame = self.frames[frame_idx].read().unwrap();
                if frame.is_dirty.load(Ordering::Acquire) {
                    frame.is_dirty.store(false, Ordering::Release);
                }
            }
        }

        f()
    }

    fn enforce_wal_rule(&self, page_lsn: Lsn) -> Result<()> {
        let guard = self.wal_check_fn.lock().unwrap();
        if let Some(ref hook) = *guard {
            hook(page_lsn)?;
        }
        Ok(())
    }

    fn find_victim_frame_sharded(
        &self,
        shards: &mut [RwLockWriteGuard<'_, HashMap<PageId, usize>>],
    ) -> Result<usize> {
        for (i, frame_lock) in self.frames.iter().enumerate() {
            let frame = frame_lock.read().unwrap();
            if frame.page_id.is_none() {
                return Ok(i);
            }
        }

        let mut clock_hand = self.clock_hand.lock().unwrap();
        let start_hand = *clock_hand;
        let total_frames = self.pool_size;

        for _ in 0..(2 * total_frames) {
            let idx = *clock_hand;
            *clock_hand = (*clock_hand + 1) % total_frames;

            let mut frame = self.frames[idx].write().unwrap();
            if frame.pin_count.load(Ordering::Acquire) > 0 {
                continue;
            }

            if frame.ref_bit.load(Ordering::Acquire) {
                frame.ref_bit.store(false, Ordering::Release);
            } else {
                if let Some(old_page_id) = frame.page_id {
                    if frame.is_dirty.load(Ordering::Acquire) {
                        self.enforce_wal_rule(frame.page.page_lsn())?;
                        self.disk_manager.write_page(old_page_id, &frame.page)?;
                        frame.is_dirty.store(false, Ordering::Release);
                    }
                    shards[shard_of(old_page_id)].remove(&old_page_id);
                    frame.page_id = None;
                }
                GLOBAL_METRICS.record_eviction();
                return Ok(idx);
            }
        }

        *clock_hand = start_hand;
        Err(Error::BufferPoolExhausted)
    }

    pub fn inspect_frames(&self) -> Vec<(usize, Option<PageId>, u32, bool, bool, Lsn)> {
        self.frames
            .iter()
            .enumerate()
            .map(|(i, f_lock)| {
                let f = f_lock.read().unwrap();
                (
                    i,
                    f.page_id,
                    f.pin_count.load(Ordering::Acquire),
                    f.is_dirty.load(Ordering::Acquire),
                    f.ref_bit.load(Ordering::Acquire),
                    f.page.page_lsn(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_TYPE_SLOTTED;

    #[test]
    fn test_buffer_pool_fetch_and_unpin() {
        let temp_dir = std::env::temp_dir().join(format!("mitrdb_bp_test1_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, mut p) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        p.insert_record(b"bp test record").unwrap();
        disk.write_page(pid, &p).unwrap();

        let bpm = BufferPoolManager::new(disk.clone(), 3);
        let fetched_p = bpm.fetch_page(0).unwrap();
        assert_eq!(fetched_p.get_record(0).unwrap(), b"bp test record");
        assert!(bpm.unpin_page(0, false).is_ok());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_buffer_pool_eviction_clock() {
        let temp_dir = std::env::temp_dir().join(format!("mitrdb_bp_test2_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        for i in 0..5 {
            let (pid, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
            assert_eq!(pid, i);
        }

        let bpm = BufferPoolManager::new(disk.clone(), 2);
        bpm.fetch_page(0).unwrap();
        bpm.unpin_page(0, false).unwrap();

        bpm.fetch_page(1).unwrap();
        bpm.unpin_page(1, false).unwrap();

        bpm.fetch_page(2).unwrap();
        bpm.unpin_page(2, false).unwrap();

        let p2 = bpm.fetch_page(2).unwrap();
        assert_eq!(p2.page_id(), 2);
        bpm.unpin_page(2, false).unwrap();

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_buffer_pool_exhaustion_when_all_pinned() {
        let temp_dir = std::env::temp_dir().join(format!("mitrdb_bp_test3_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();

        let bpm = BufferPoolManager::new(disk.clone(), 2);
        let _p0 = bpm.fetch_page(0).unwrap();
        let _p1 = bpm.fetch_page(1).unwrap();

        let err = bpm.fetch_page(2).unwrap_err();
        assert_eq!(err, Error::BufferPoolExhausted);

        bpm.unpin_page(0, false).unwrap();
        let p2 = bpm.fetch_page(2);
        assert!(p2.is_ok());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_buffer_pool_dirty_page_flushed_on_eviction() {
        let temp_dir = std::env::temp_dir().join(format!("mitrdb_bp_test4_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();

        let bpm = BufferPoolManager::new(disk.clone(), 2);
        bpm.update_page_data(0, |p| {
            p.insert_record(b"dirty data that must survive eviction")?;
            Ok(())
        })
        .unwrap();

        bpm.fetch_page(1).unwrap();
        bpm.unpin_page(1, false).unwrap();

        bpm.fetch_page(2).unwrap();
        bpm.unpin_page(2, false).unwrap();

        let disk2 = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let p0_on_disk = disk2.read_page(0).unwrap();
        assert_eq!(
            p0_on_disk.get_record(0).unwrap(),
            b"dirty data that must survive eviction"
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_finalize_free_pages_retains_pinned_and_retries_after_unpin() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_bp_test_pin_free_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p0, 0);
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p1, 1);
        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();
        bpm.stage_free_page(10, p1).unwrap();

        bpm.finalize_free_pages(10).unwrap();
        assert!(!bpm.is_page_free(p1));
        assert_eq!(bpm.free_page_count(), 0);

        let (p2, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p2, 2);
        bpm.unpin_page(p2, false).unwrap();

        bpm.unpin_page(p1, false).unwrap();
        bpm.finalize_free_pages(11).unwrap();
        assert!(bpm.is_page_free(p1));
        assert_eq!(bpm.free_page_count(), 1);

        let (reused, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(reused, p1);
        bpm.unpin_page(reused, false).unwrap();

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_finalize_free_pages_abort_clears_unflagged_without_freeing() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_bp_test_abort_free_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();
        bpm.stage_free_page(20, p1).unwrap();

        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags & !PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();

        bpm.finalize_free_pages(20).unwrap();
        assert!(!bpm.is_page_free(p1));

        bpm.unpin_page(p1, false).unwrap();
        bpm.finalize_free_pages(20).unwrap();
        assert!(!bpm.is_page_free(p1));
        assert_eq!(bpm.free_page_count(), 0);
        assert!(bpm.deferred_free_txns.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_finalize_free_pages_error_path_preserves_pending_without_duplicates() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_bp_test_err_free_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();
        bpm.unpin_page(p1, false).unwrap();

        bpm.stage_free_page(30, 2).unwrap();
        bpm.stage_free_page(30, p1).unwrap();

        let err1 = bpm.finalize_free_pages(30);
        assert!(err1.is_err());
        assert!(bpm.is_page_free(p1));
        assert_eq!(bpm.free_page_count(), 1);

        let err2 = bpm.finalize_free_pages(30);
        assert!(err2.is_err());
        assert_eq!(bpm.deferred_free_txns.lock().unwrap().len(), 1);

        let (_reused_p1, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        let (allocated_p2, mut page2) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(allocated_p2, 2);
        let flags2 = page2.flags();
        page2.set_flags(flags2 | PAGE_FLAG_FREE);
        disk.write_page(allocated_p2, &page2).unwrap();

        bpm.finalize_free_pages(31).unwrap();
        assert!(bpm.is_page_free(2));
        assert!(bpm.deferred_free_txns.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_new_page_protects_pinned_and_dirty_active_resident_frames_when_in_freelist() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_bp_test_new_page_pin_safety_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p0, 0);
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p1, 1);
        bpm.update_page_data(p1, |page| {
            page.insert_record(b"pinned active payload")?;
            Ok(())
        })
        .unwrap();

        let (p2, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p2, 2);
        bpm.unpin_page(p2, false).unwrap();

        disk.free_page(p1).unwrap();
        disk.free_page(p2).unwrap();
        bpm.unmark_page_free(p2);
        bpm.update_page_data(p2, |page| {
            page.insert_record(b"dirty active payload")?;
            Ok(())
        })
        .unwrap();
        disk.free_page(p2).unwrap();

        let (p3, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p3, 3);
        assert!(!bpm.is_page_free(p1));
        assert!(!bpm.is_page_free(p2));

        bpm.unpin_page(p1, false).unwrap();
        let frames_snapshot = bpm.inspect_frames();
        let p3_frame = frames_snapshot.iter().find(|f| f.1 == Some(p3)).unwrap();
        assert_eq!(p3_frame.2, 1);

        let p1_fetched = bpm.fetch_page(p1).unwrap();
        assert_eq!(p1_fetched.get_record(0).unwrap(), b"pinned active payload");
        bpm.unpin_page(p1, false).unwrap();

        let p2_fetched = bpm.fetch_page(p2).unwrap();
        assert_eq!(p2_fetched.get_record(0).unwrap(), b"dirty active payload");
        bpm.unpin_page(p2, false).unwrap();
        bpm.unpin_page(p3, false).unwrap();

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_rebuild_freelist_from_disk_flushes_dirty_pages_and_preserves_pinned_deferred() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_bp_test_rebuild_freelist_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 6);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();
        bpm.unpin_page(p1, false).unwrap();

        let (p2, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.update_page_data(p2, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();
        bpm.stage_free_page(40, p2).unwrap();

        let (p3, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        bpm.unpin_page(p3, false).unwrap();
        bpm.free_page(p3).unwrap();
        assert!(bpm.is_page_free(p3));

        bpm.unmark_page_free(p3);
        bpm.update_page_data(p3, |page| {
            page.set_page_type(PAGE_TYPE_SLOTTED);
            page.set_flags(0);
            page.insert_record(b"reactivated dirty page")?;
            Ok(())
        })
        .unwrap();

        bpm.rebuild_freelist_from_disk().unwrap();

        assert!(bpm.is_page_free(p1));
        assert!(!bpm.is_page_free(p2));
        assert!(!bpm.is_page_free(p3));
        assert_eq!(bpm.free_page_count(), 1);
        assert!(bpm.deferred_free_txns.lock().unwrap().contains(&40));

        let p3_check = bpm.fetch_page(p3).unwrap();
        assert_eq!(p3_check.get_record(0).unwrap(), b"reactivated dirty page");
        bpm.unpin_page(p3, false).unwrap();

        bpm.unpin_page(p2, false).unwrap();
        bpm.finalize_free_pages(41).unwrap();
        assert!(bpm.is_page_free(p2));
        assert_eq!(bpm.free_page_count(), 2);
        assert!(bpm.deferred_free_txns.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_new_page_propagates_deferred_free_error_and_preserves_pending_state() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_bp_test_new_page_def_err_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p0, 0);
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p1, 1);
        bpm.update_page_data(p1, |page| {
            let flags = page.flags();
            page.set_flags(flags | PAGE_FLAG_FREE);
            Ok(())
        })
        .unwrap();

        bpm.stage_free_page(60, 2).unwrap();
        bpm.stage_free_page(60, p1).unwrap();
        assert!(bpm.finalize_free_pages(60).is_err());
        assert_eq!(*bpm.deferred_free_txns.lock().unwrap(), vec![60]);
        assert_eq!(disk.num_pages(), 2);

        bpm.unpin_page(p1, false).unwrap();

        let alloc_err = bpm.new_page(PAGE_TYPE_SLOTTED);
        assert!(matches!(alloc_err, Err(Error::InvalidPageId(2))));
        assert!(bpm.is_page_free(p1));
        assert_eq!(disk.num_pages(), 2);
        assert_eq!(*bpm.deferred_free_txns.lock().unwrap(), vec![60]);
        assert_eq!(disk.take_pending_free_pages(60), vec![2]);

        bpm.deferred_free_txns.lock().unwrap().clear();
        let (reused_p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(reused_p1, p1);
        bpm.unpin_page(reused_p1, false).unwrap();

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_new_page_conflict_reclaims_allocated_page_to_freelist() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_bp_test_conflict_leak_guard_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = BufferPoolManager::new(disk.clone(), 4);

        let (p0, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p0, 0);
        bpm.unpin_page(p0, false).unwrap();

        let (p1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p1, 1);
        bpm.unpin_page(p1, false).unwrap();

        let (p2, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p2, 2);
        bpm.unpin_page(p2, false).unwrap();

        // Free p1 and p2 to disk freelist
        disk.free_page(p1).unwrap();
        disk.free_page(p2).unwrap();
        assert!(disk.is_page_free(p1));
        assert!(disk.is_page_free(p2));

        // Allocating new pages correctly reuses from freelist without leakage
        let (p_reused_1, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        let (p_reused_2, _) = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap();
        assert!(!disk.is_page_free(p_reused_1));
        assert!(!disk.is_page_free(p_reused_2));
        assert!(p_reused_1 == p1 || p_reused_1 == p2);
        assert!(p_reused_2 == p1 || p_reused_2 == p2);

        bpm.unpin_page(p_reused_1, false).unwrap();
        bpm.unpin_page(p_reused_2, false).unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
