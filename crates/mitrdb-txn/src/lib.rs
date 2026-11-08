use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Bound as RangeBound, Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use mitrdb_common::{Error, INVALID_LSN, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;
pub use mitrdb_storage::{BPlusTree, BufferPoolManager, KeyBound, KeyRange, LogRecord, WalManager};

/// Detailed per-scan profiling is opt-in because console output and metric snapshots
/// can distort benchmark results. Set MITRDB_PROFILE_SCAN=1 to enable it.
fn scan_profiling_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("MITRDB_PROFILE_SCAN").is_some_and(|value| value.to_str() == Some("1"))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VacuumReport {
    pub horizon_ts: u64,
    pub versions_pruned: usize,
    pub keys_cleared: usize,
    pub txns_reclaimed: usize,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    SnapshotIsolation,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committing,
    Committed { commit_ts: u64 },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKey {
    Table(u32),
    Page(PageId),
    Record(PageId, u16),
    Key(Vec<u8>),
    Range {
        tree_prefix: [u8; 8],
        range: KeyRange,
    },
    Gap {
        tree_prefix: [u8; 8],
        range: KeyRange,
    },
    NextKey {
        tree_prefix: [u8; 8],
        range: KeyRange,
    },
}

impl LockKey {
    pub fn scoped_tree_range(&self) -> Option<([u8; 8], KeyRange)> {
        match self {
            LockKey::Key(skey) if skey.len() >= 8 => {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&skey[..8]);
                Some((prefix, KeyRange::point(skey[8..].to_vec())))
            }
            LockKey::Range { tree_prefix, range }
            | LockKey::Gap { tree_prefix, range }
            | LockKey::NextKey { tree_prefix, range } => Some((*tree_prefix, range.clone())),
            _ => None,
        }
    }

    pub fn overlaps(&self, other: &LockKey) -> bool {
        if let (Some((p1, r1)), Some((p2, r2))) =
            (self.scoped_tree_range(), other.scoped_tree_range())
        {
            return p1 == p2 && r1.overlaps(&r2);
        }
        self == other
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone)]
struct LockEntry {
    shared_holders: HashSet<TxnId>,
    exclusive_holder: Option<TxnId>,
}

impl LockEntry {
    fn new() -> Self {
        LockEntry {
            shared_holders: HashSet::new(),
            exclusive_holder: None,
        }
    }

    fn is_free(&self) -> bool {
        self.shared_holders.is_empty() && self.exclusive_holder.is_none()
    }

    fn mode_for(&self, txn_id: TxnId) -> Option<LockMode> {
        if self.exclusive_holder == Some(txn_id) {
            Some(LockMode::Exclusive)
        } else if self.shared_holders.contains(&txn_id) {
            Some(LockMode::Shared)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LockDelta {
    pub changes: Vec<(LockKey, Option<LockMode>)>,
}

struct LockManagerState {
    locks: HashMap<LockKey, LockEntry>,
    point_by_tree: HashMap<[u8; 8], BTreeMap<Vec<u8>, LockKey>>,
    ranges_by_tree: HashMap<[u8; 8], HashSet<LockKey>>,
    by_txn: HashMap<TxnId, HashSet<LockKey>>,
    wait_for: HashMap<TxnId, HashSet<TxnId>>,
}

impl LockManagerState {
    fn new() -> Self {
        LockManagerState {
            locks: HashMap::new(),
            point_by_tree: HashMap::new(),
            ranges_by_tree: HashMap::new(),
            by_txn: HashMap::new(),
            wait_for: HashMap::new(),
        }
    }

    fn index_lock_key(&mut self, key: &LockKey) {
        match key {
            LockKey::Key(skey) if skey.len() >= 8 => {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&skey[..8]);
                let raw = skey[8..].to_vec();
                self.point_by_tree
                    .entry(prefix)
                    .or_default()
                    .entry(raw)
                    .or_insert_with(|| key.clone());
            }
            LockKey::Range { tree_prefix, .. }
            | LockKey::Gap { tree_prefix, .. }
            | LockKey::NextKey { tree_prefix, .. } => {
                self.ranges_by_tree
                    .entry(*tree_prefix)
                    .or_default()
                    .insert(key.clone());
            }
            _ => {}
        }
    }

    fn unindex_lock_key(&mut self, key: &LockKey) {
        match key {
            LockKey::Key(skey) if skey.len() >= 8 => {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&skey[..8]);
                if let Some(tree_map) = self.point_by_tree.get_mut(&prefix) {
                    tree_map.remove(&skey[8..]);
                    if tree_map.is_empty() {
                        self.point_by_tree.remove(&prefix);
                    }
                }
            }
            LockKey::Range { tree_prefix, .. }
            | LockKey::Gap { tree_prefix, .. }
            | LockKey::NextKey { tree_prefix, .. } => {
                if let Some(set) = self.ranges_by_tree.get_mut(tree_prefix) {
                    set.remove(key);
                    if set.is_empty() {
                        self.ranges_by_tree.remove(tree_prefix);
                    }
                }
            }
            _ => {}
        }
    }

    fn record_conflict(
        entry: &LockEntry,
        txn_id: TxnId,
        mode: LockMode,
        conflicting_holders: &mut BTreeSet<TxnId>,
    ) {
        if entry.is_free() {
            return;
        }
        match mode {
            LockMode::Shared => {
                if let Some(ex_holder) = entry.exclusive_holder
                    && ex_holder != txn_id
                {
                    conflicting_holders.insert(ex_holder);
                }
            }
            LockMode::Exclusive => {
                if let Some(ex_holder) = entry.exclusive_holder
                    && ex_holder != txn_id
                {
                    conflicting_holders.insert(ex_holder);
                }
                for &sh_holder in &entry.shared_holders {
                    if sh_holder != txn_id {
                        conflicting_holders.insert(sh_holder);
                    }
                }
            }
        }
    }

    fn collect_conflicts_for_key(
        &self,
        req_key: &LockKey,
        txn_id: TxnId,
        mode: LockMode,
        conflicting_holders: &mut BTreeSet<TxnId>,
    ) {
        if let Some(entry) = self.locks.get(req_key) {
            Self::record_conflict(entry, txn_id, mode, conflicting_holders);
        }

        match req_key {
            LockKey::Key(skey) if skey.len() >= 8 => {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&skey[..8]);
                let raw_key = &skey[8..];

                if let Some(range_keys) = self.ranges_by_tree.get(&prefix) {
                    for rkey in range_keys {
                        if let Some((_, range)) = rkey.scoped_tree_range()
                            && range.contains(raw_key)
                            && let Some(entry) = self.locks.get(rkey)
                        {
                            Self::record_conflict(entry, txn_id, mode, conflicting_holders);
                        }
                    }
                }
            }
            LockKey::Range { tree_prefix, range }
            | LockKey::Gap { tree_prefix, range }
            | LockKey::NextKey { tree_prefix, range } => {
                if range.is_empty() {
                    return;
                }
                if let Some(point_map) = self.point_by_tree.get(tree_prefix) {
                    let start = match &range.start {
                        KeyBound::Unbounded => RangeBound::Unbounded,
                        KeyBound::Included(k) => RangeBound::Included(k.clone()),
                        KeyBound::Excluded(k) => RangeBound::Excluded(k.clone()),
                    };
                    let end = match &range.end {
                        KeyBound::Unbounded => RangeBound::Unbounded,
                        KeyBound::Included(k) => RangeBound::Included(k.clone()),
                        KeyBound::Excluded(k) => RangeBound::Excluded(k.clone()),
                    };
                    for (_, p_lock_key) in point_map.range((start, end)) {
                        if let Some(entry) = self.locks.get(p_lock_key) {
                            Self::record_conflict(entry, txn_id, mode, conflicting_holders);
                        }
                    }
                }

                if let Some(range_keys) = self.ranges_by_tree.get(tree_prefix) {
                    for rkey in range_keys {
                        if let Some((_, existing_range)) = rkey.scoped_tree_range()
                            && range.overlaps(&existing_range)
                            && let Some(entry) = self.locks.get(rkey)
                        {
                            Self::record_conflict(entry, txn_id, mode, conflicting_holders);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    #[inline]
    fn has_range_locks_for_tree(&self, prefix: &[u8; 8]) -> bool {
        self.ranges_by_tree
            .get(prefix)
            .is_some_and(|set| !set.is_empty())
    }

    #[inline]
    fn is_point_exclusive_available(&self, key: &LockKey, txn_id: TxnId) -> bool {
        match self.locks.get(key) {
            None => true,
            Some(entry) => {
                if entry.exclusive_holder == Some(txn_id) {
                    true
                } else {
                    entry.is_free()
                }
            }
        }
    }
}

pub struct LockManager {
    state: Mutex<LockManagerState>,
    cv: Condvar,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            state: Mutex::new(LockManagerState::new()),
            cv: Condvar::new(),
        }
    }

    pub fn acquire(&self, txn_id: TxnId, key: LockKey, mode: LockMode) -> Result<()> {
        let _ = self.acquire_batch_with_rollback(txn_id, std::slice::from_ref(&key), mode)?;
        Ok(())
    }

    pub fn acquire_point_exclusive(
        &self,
        txn_id: TxnId,
        tree_prefix: [u8; 8],
        raw_key: &[u8],
        scoped_key: &LockKey,
    ) -> Result<()> {
        let profiling = mitrdb_observe::profiling_enabled();
        let mutex_started = profiling.then(std::time::Instant::now);
        let mut state = self.state.lock().unwrap();

        if let Some(started) = mutex_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::LockMutexWait,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }

        // Fast path: No active range/gap locks on this tree AND the key is not locked by another txn
        if !state.has_range_locks_for_tree(&tree_prefix)
            && state.is_point_exclusive_available(scoped_key, txn_id)
        {
            let book_started = profiling.then(std::time::Instant::now);

            if !state.locks.contains_key(scoped_key) {
                state
                    .point_by_tree
                    .entry(tree_prefix)
                    .or_default()
                    .entry(raw_key.to_vec())
                    .or_insert_with(|| scoped_key.clone());
            }

            let entry = state
                .locks
                .entry(scoped_key.clone())
                .or_insert_with(LockEntry::new);

            entry.shared_holders.remove(&txn_id);
            entry.exclusive_holder = Some(txn_id);
            state
                .by_txn
                .entry(txn_id)
                .or_default()
                .insert(scoped_key.clone());

            if let Some(started) = book_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::LockBookkeeping,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            return Ok(());
        }

        // Fallback to general batch algorithm if conflicts or range locks exist
        drop(state);
        let _ = self.acquire_batch_with_rollback(
            txn_id,
            std::slice::from_ref(scoped_key),
            LockMode::Exclusive,
        )?;
        Ok(())
    }

    pub fn acquire_with_rollback(
        &self,
        txn_id: TxnId,
        key: LockKey,
        mode: LockMode,
    ) -> Result<LockDelta> {
        self.acquire_batch_with_rollback(txn_id, std::slice::from_ref(&key), mode)
    }

    pub fn acquire_batch(&self, txn_id: TxnId, keys: &[LockKey], mode: LockMode) -> Result<()> {
        let _ = self.acquire_batch_with_rollback(txn_id, keys, mode)?;
        Ok(())
    }

    pub fn acquire_batch_with_rollback(
        &self,
        txn_id: TxnId,
        keys: &[LockKey],
        mode: LockMode,
    ) -> Result<LockDelta> {
        let effective_keys: Vec<LockKey> = keys
            .iter()
            .filter(|k| {
                !k.scoped_tree_range()
                    .is_some_and(|(_, range)| range.is_empty())
            })
            .cloned()
            .collect();
        if effective_keys.is_empty() {
            return Ok(LockDelta::default());
        }

        let profiling = mitrdb_observe::profiling_enabled();
        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(50);

        let mutex_started = profiling.then(std::time::Instant::now);
        let mut state = self.state.lock().unwrap();
        if let Some(started) = mutex_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::LockMutexWait,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }

        // Fast exit: this txn already holds every requested key in a sufficient mode.
        if !state.wait_for.contains_key(&txn_id)
            && effective_keys.iter().all(|k| {
                state.locks.get(k).is_some_and(|e| {
                    e.exclusive_holder == Some(txn_id)
                        || (matches!(mode, LockMode::Shared) && e.shared_holders.contains(&txn_id))
                })
            })
        {
            return Ok(LockDelta::default());
        }

        loop {
            let scan_started = profiling.then(std::time::Instant::now);
            let mut conflicting_holders = BTreeSet::new();
            for req_key in &effective_keys {
                state.collect_conflicts_for_key(req_key, txn_id, mode, &mut conflicting_holders);
            }
            if let Some(started) = scan_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::LockTableScan,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            if conflicting_holders.is_empty() {
                let book_started = profiling.then(std::time::Instant::now);
                let had_wait_edges = state.wait_for.remove(&txn_id).is_some();
                let mut changes = Vec::new();
                let mut recorded_keys = HashSet::new();

                for req_key in effective_keys {
                    let is_new_entry = !state.locks.contains_key(&req_key);
                    if is_new_entry {
                        state.index_lock_key(&req_key);
                    }
                    let entry = state
                        .locks
                        .entry(req_key.clone())
                        .or_insert_with(LockEntry::new);
                    let prev_mode = entry.mode_for(txn_id);

                    match mode {
                        LockMode::Shared => {
                            if entry.exclusive_holder != Some(txn_id) {
                                entry.shared_holders.insert(txn_id);
                            }
                        }
                        LockMode::Exclusive => {
                            entry.shared_holders.remove(&txn_id);
                            entry.exclusive_holder = Some(txn_id);
                        }
                    }

                    let new_mode = entry.mode_for(txn_id);
                    if prev_mode != new_mode && recorded_keys.insert(req_key.clone()) {
                        changes.push((req_key.clone(), prev_mode));
                    }
                    state.by_txn.entry(txn_id).or_default().insert(req_key);
                }

                if let Some(started) = book_started {
                    mitrdb_observe::record_profile_duration(
                        mitrdb_observe::ProfilePhase::LockBookkeeping,
                        started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    );
                }
                drop(state);
                if had_wait_edges {
                    self.cv.notify_all();
                }
                return Ok(LockDelta { changes });
            }

            let holders: Vec<TxnId> = conflicting_holders.into_iter().collect();
            let edges = state.wait_for.entry(txn_id).or_default();
            for &h in &holders {
                edges.insert(h);
            }

            if Self::detect_cycle(&state.wait_for, txn_id) {
                state.wait_for.remove(&txn_id);
                drop(state);
                self.cv.notify_all();
                return Err(Error::Deadlock);
            }

            let elapsed = start_time.elapsed();
            if elapsed >= timeout {
                state.wait_for.remove(&txn_id);
                drop(state);
                self.cv.notify_all();
                return Err(Error::LockTimeout);
            }

            let remaining = timeout - elapsed;
            let wait_started = profiling.then(std::time::Instant::now);
            let result = self.cv.wait_timeout(state, remaining).unwrap();
            if let Some(started) = wait_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::LockConditionWait,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            state = result.0;

            if result.1.timed_out() && start_time.elapsed() >= timeout {
                state.wait_for.remove(&txn_id);
                drop(state);
                self.cv.notify_all();
                return Err(Error::LockTimeout);
            }
        }
    }

    pub fn rollback_delta(&self, txn_id: TxnId, delta: LockDelta) {
        if delta.changes.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        for (key, prev_mode) in delta.changes.into_iter().rev() {
            let mut remove_entry = false;
            if let Some(entry) = state.locks.get_mut(&key) {
                entry.shared_holders.remove(&txn_id);
                if entry.exclusive_holder == Some(txn_id) {
                    entry.exclusive_holder = None;
                }
                match prev_mode {
                    Some(LockMode::Shared) => {
                        entry.shared_holders.insert(txn_id);
                    }
                    Some(LockMode::Exclusive) => {
                        entry.exclusive_holder = Some(txn_id);
                    }
                    None => {}
                }
                remove_entry = entry.is_free();
            }
            if prev_mode.is_none()
                && let Some(txn_set) = state.by_txn.get_mut(&txn_id)
            {
                txn_set.remove(&key);
                if txn_set.is_empty() {
                    state.by_txn.remove(&txn_id);
                }
            }
            if remove_entry {
                state.locks.remove(&key);
                state.unindex_lock_key(&key);
            }
        }
        drop(state);
        self.cv.notify_all();
    }

    fn detect_cycle(graph: &HashMap<TxnId, HashSet<TxnId>>, start: TxnId) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![start];
        while let Some(curr) = stack.pop() {
            if let Some(neighbors) = graph.get(&curr) {
                for &next in neighbors {
                    if next == start {
                        return true;
                    }
                    if visited.insert(next) {
                        stack.push(next);
                    }
                }
            }
        }
        false
    }

    pub fn cancel_wait(&self, txn_id: TxnId) {
        let mut state = self.state.lock().unwrap();
        state.wait_for.remove(&txn_id);
    }

    pub fn wait_for_holders(&self, waiter: TxnId) -> HashSet<TxnId> {
        let state = self.state.lock().unwrap();
        state.wait_for.get(&waiter).cloned().unwrap_or_default()
    }

    pub fn active_lock_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.locks.values().filter(|e| !e.is_free()).count()
    }

    pub fn held_locks_for_txn(&self, txn_id: TxnId) -> Vec<(LockKey, LockMode)> {
        let state = self.state.lock().unwrap();
        let mut out = Vec::new();
        if let Some(keys) = state.by_txn.get(&txn_id) {
            for k in keys {
                if let Some(entry) = state.locks.get(k) {
                    if entry.exclusive_holder == Some(txn_id) {
                        out.push((k.clone(), LockMode::Exclusive));
                    } else if entry.shared_holders.contains(&txn_id) {
                        out.push((k.clone(), LockMode::Shared));
                    }
                }
            }
        }
        out
    }

    pub fn restore_locks_for_txn(&self, txn_id: TxnId, snapshot: &[(LockKey, LockMode)]) {
        let mut state = self.state.lock().unwrap();
        if let Some(held_keys) = state.by_txn.remove(&txn_id) {
            for k in held_keys {
                let mut remove_entry = false;
                if let Some(entry) = state.locks.get_mut(&k) {
                    entry.shared_holders.remove(&txn_id);
                    if entry.exclusive_holder == Some(txn_id) {
                        entry.exclusive_holder = None;
                    }
                    remove_entry = entry.is_free();
                }
                if remove_entry {
                    state.locks.remove(&k);
                    state.unindex_lock_key(&k);
                }
            }
        }
        for (k, mode) in snapshot {
            let is_new = !state.locks.contains_key(k);
            if is_new {
                state.index_lock_key(k);
            }
            let entry = state.locks.entry(k.clone()).or_insert_with(LockEntry::new);
            match mode {
                LockMode::Shared => {
                    entry.shared_holders.insert(txn_id);
                }
                LockMode::Exclusive => {
                    entry.exclusive_holder = Some(txn_id);
                }
            }
            state.by_txn.entry(txn_id).or_default().insert(k.clone());
        }
        drop(state);
        self.cv.notify_all();
    }

    pub fn release_all(&self, txn_id: TxnId) {
        let mut state = self.state.lock().unwrap();
        if let Some(held_keys) = state.by_txn.remove(&txn_id) {
            for k in held_keys {
                let mut remove_entry = false;
                if let Some(entry) = state.locks.get_mut(&k) {
                    entry.shared_holders.remove(&txn_id);
                    if entry.exclusive_holder == Some(txn_id) {
                        entry.exclusive_holder = None;
                    }
                    remove_entry = entry.is_free();
                }
                if remove_entry {
                    state.locks.remove(&k);
                    state.unindex_lock_key(&k);
                }
            }
        }

        state.wait_for.remove(&txn_id);
        for edges in state.wait_for.values_mut() {
            edges.remove(&txn_id);
        }
        state.wait_for.retain(|_, edges| !edges.is_empty());

        drop(state);
        self.cv.notify_all();
    }
    pub fn is_key_exclusively_locked_by_other(&self, key: &LockKey, txn_id: TxnId) -> bool {
        let state = self.state.lock().unwrap();
        state
            .locks
            .get(key)
            .is_some_and(|e| e.exclusive_holder.is_some_and(|h| h != txn_id))
    }
    pub fn has_active_prefix_locks(&self, prefix: &[u8; 8]) -> bool {
        let state = self.state.lock().unwrap();
        state
            .point_by_tree
            .get(prefix)
            .is_some_and(|m| !m.is_empty())
            || state
                .ranges_by_tree
                .get(prefix)
                .is_some_and(|s| !s.is_empty())
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub xmin: TxnId,
    pub xmax: Option<TxnId>,
    pub value: Option<Vec<u8>>,
}

/// Primary MVCC point-lookup map plus a tree-scoped ordered key index.
#[derive(Default)]
pub struct MvccVersionStore {
    versions: HashMap<Vec<u8>, Vec<VersionedValue>>,
    keys_by_tree: HashMap<[u8; 8], BTreeSet<Vec<u8>>>,
}

impl MvccVersionStore {
    fn index_tree_key(&mut self, prefix: [u8; 8], raw_key: &[u8]) {
        self.keys_by_tree
            .entry(prefix)
            .or_default()
            .insert(raw_key.to_vec());
    }

    pub fn rebuild_tree_index(&mut self) {
        let mut rebuilt: HashMap<[u8; 8], BTreeSet<Vec<u8>>> = HashMap::new();
        for (scoped_key, chain) in &self.versions {
            if scoped_key.len() < 8 || chain.is_empty() {
                continue;
            }
            let mut prefix = [0u8; 8];
            prefix.copy_from_slice(&scoped_key[..8]);
            rebuilt
                .entry(prefix)
                .or_default()
                .insert(scoped_key[8..].to_vec());
        }
        self.keys_by_tree = rebuilt;
    }

    pub fn keys_in_range(&self, prefix: [u8; 8], range: &KeyRange) -> Vec<Vec<u8>> {
        let Some(keys) = self.keys_by_tree.get(&prefix) else {
            return Vec::new();
        };
        if range.is_empty() {
            return Vec::new();
        }
        let start = match &range.start {
            KeyBound::Unbounded => RangeBound::Unbounded,
            KeyBound::Included(key) => RangeBound::Included(key.clone()),
            KeyBound::Excluded(key) => RangeBound::Excluded(key.clone()),
        };
        let end = match &range.end {
            KeyBound::Unbounded => RangeBound::Unbounded,
            KeyBound::Included(key) => RangeBound::Included(key.clone()),
            KeyBound::Excluded(key) => RangeBound::Excluded(key.clone()),
        };
        keys.range((start, end)).cloned().collect()
    }

    pub fn extend_keys_in_range(
        &self,
        prefix: [u8; 8],
        range: &KeyRange,
        target: &mut BTreeSet<Vec<u8>>,
    ) -> usize {
        let Some(keys) = self.keys_by_tree.get(&prefix) else {
            return 0;
        };
        if range.is_empty() {
            return 0;
        }
        let start = match &range.start {
            KeyBound::Unbounded => RangeBound::Unbounded,
            KeyBound::Included(key) => RangeBound::Included(key.clone()),
            KeyBound::Excluded(key) => RangeBound::Excluded(key.clone()),
        };
        let end = match &range.end {
            KeyBound::Unbounded => RangeBound::Unbounded,
            KeyBound::Included(key) => RangeBound::Included(key.clone()),
            KeyBound::Excluded(key) => RangeBound::Excluded(key.clone()),
        };

        let mut inspected = 0;
        for key in keys.range((start, end)) {
            target.insert(key.clone());
            inspected += 1;
        }
        inspected
    }
}

impl Deref for MvccVersionStore {
    type Target = HashMap<Vec<u8>, Vec<VersionedValue>>;

    fn deref(&self) -> &Self::Target {
        &self.versions
    }
}

impl DerefMut for MvccVersionStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.versions
    }
}

#[derive(Clone)]
pub enum UndoAction {
    PageSlice {
        page_id: PageId,
        offset: u16,
        old_data: Vec<u8>,
        update_lsn: Lsn,
        prev_lsn: Lsn,
    },
    BTreeKey {
        tree: Arc<BPlusTree>,
        key: Vec<u8>,
        old_value: Option<Vec<u8>>,
    },
}

pub struct TransactionContext {
    pub txn_id: TxnId,
    pub isolation: IsolationLevel,
    pub state: TxnState,
    pub read_ts: u64,
    pub commit_ts: Option<u64>,
    pub last_lsn: Lsn,
    pub write_set: Vec<UndoAction>,
    pub read_keys: HashSet<Vec<u8>>,
    pub read_ranges: Vec<([u8; 8], KeyRange)>,
    in_rollback: bool,
    rollback_poisoned: bool,
    commit_poisoned: bool,
    poisoned_commit_lsn: Option<Lsn>,
    abort_lsn_logged: Option<Lsn>,
}

pub struct TxnManager {
    next_txn_id: AtomicU64,
    oracle_ts: AtomicU64,
    wal: Option<Arc<WalManager>>,
    bpm: Option<Arc<BufferPoolManager>>,
    lock_manager: Arc<LockManager>,
    txns: Mutex<HashMap<TxnId, TransactionContext>>,
    poisoned_commits: std::sync::atomic::AtomicUsize,
    mvcc_versions: Mutex<MvccVersionStore>,
    chain_epoch: AtomicU64,
}

impl TxnManager {
    pub fn new() -> Self {
        TxnManager {
            next_txn_id: AtomicU64::new(1),
            oracle_ts: AtomicU64::new(1),
            wal: None,
            bpm: None,
            lock_manager: Arc::new(LockManager::new()),
            txns: Mutex::new(HashMap::new()),
            poisoned_commits: std::sync::atomic::AtomicUsize::new(0),
            mvcc_versions: Mutex::new(MvccVersionStore::default()),
            chain_epoch: AtomicU64::new(0),
        }
    }

    pub fn with_storage(wal: Arc<WalManager>, bpm: Arc<BufferPoolManager>) -> Self {
        let initial_tid = wal
            .read_all_records()
            .ok()
            .and_then(|records| {
                records
                    .iter()
                    .map(|r| r.txn_id)
                    .filter(|&tid| tid < 1_000_000_000)
                    .max()
            })
            .unwrap_or(0)
            .max(wal.max_user_txn_id())
            + 1;
        TxnManager {
            next_txn_id: AtomicU64::new(initial_tid),
            oracle_ts: AtomicU64::new(1),
            wal: Some(wal),
            bpm: Some(bpm),
            lock_manager: Arc::new(LockManager::new()),
            txns: Mutex::new(HashMap::new()),
            poisoned_commits: std::sync::atomic::AtomicUsize::new(0),
            mvcc_versions: Mutex::new(MvccVersionStore::default()),
            chain_epoch: AtomicU64::new(0),
        }
    }

    fn tree_scope_prefix(tree: &Arc<BPlusTree>) -> [u8; 8] {
        tree.tree_id().to_le_bytes()
    }

    fn scoped_key(tree: &Arc<BPlusTree>, key: &[u8]) -> Vec<u8> {
        let prefix = Self::tree_scope_prefix(tree);
        let mut out = Vec::with_capacity(8 + key.len());
        out.extend_from_slice(&prefix);
        out.extend_from_slice(key);
        out
    }

    fn has_active_tree_references_locked(
        prefix: &[u8; 8],
        tid: u64,
        versions: &HashMap<Vec<u8>, Vec<VersionedValue>>,
        txns: &HashMap<TxnId, TransactionContext>,
    ) -> bool {
        for ctx in txns.values() {
            if ctx.state != TxnState::Active
                && ctx.state != TxnState::Committing
                && !ctx.in_rollback
            {
                continue;
            }
            if ctx
                .read_keys
                .iter()
                .any(|skey| skey.len() >= 8 && &skey[..8] == prefix)
            {
                return true;
            }
            if ctx.read_ranges.iter().any(|(p, _)| p == prefix) {
                return true;
            }
            if ctx.write_set.iter().any(|action| match action {
                UndoAction::BTreeKey { tree: t, .. } => t.tree_id() == tid,
                _ => false,
            }) {
                return true;
            }
        }
        for (skey, chain) in versions.iter() {
            if skey.len() >= 8 && &skey[..8] == prefix {
                for v in chain {
                    if txns.get(&v.xmin).is_some_and(|c| {
                        c.state == TxnState::Active
                            || c.state == TxnState::Committing
                            || c.in_rollback
                    }) {
                        return true;
                    }
                    if let Some(xmax) = v.xmax
                        && txns.get(&xmax).is_some_and(|c| {
                            c.state == TxnState::Active
                                || c.state == TxnState::Committing
                                || c.in_rollback
                        })
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn has_active_tree_references(&self, tree: &Arc<BPlusTree>) -> bool {
        let prefix = Self::tree_scope_prefix(tree);
        let tid = tree.tree_id();
        if self.lock_manager.has_active_prefix_locks(&prefix) {
            return true;
        }
        let versions = self.mvcc_versions.lock().unwrap();
        let txns = self.txns.lock().unwrap();
        if self.lock_manager.has_active_prefix_locks(&prefix) {
            return true;
        }
        Self::has_active_tree_references_locked(&prefix, tid, &versions, &txns)
    }

    pub fn purge_tree_state(&self, tree: &Arc<BPlusTree>) -> Result<()> {
        self.purge_trees_state(std::slice::from_ref(tree))
    }

    pub fn purge_trees_state(&self, trees: &[Arc<BPlusTree>]) -> Result<()> {
        if trees.is_empty() {
            return Ok(());
        }
        let scoped: Vec<([u8; 8], u64)> = trees
            .iter()
            .map(|t| (Self::tree_scope_prefix(t), t.tree_id()))
            .collect();
        for (prefix, _) in &scoped {
            if self.lock_manager.has_active_prefix_locks(prefix) {
                return Err(Error::WriteConflict(
                    "Cannot purge tree state while referenced by an active transaction".to_string(),
                ));
            }
        }
        let mut versions = self.mvcc_versions.lock().unwrap();
        let mut txns = self.txns.lock().unwrap();
        for (prefix, tid) in &scoped {
            if self.lock_manager.has_active_prefix_locks(prefix)
                || Self::has_active_tree_references_locked(prefix, *tid, &versions, &txns)
            {
                return Err(Error::WriteConflict(
                    "Cannot purge tree state while referenced by an active transaction".to_string(),
                ));
            }
        }
        versions.retain(|skey, _| {
            !(skey.len() >= 8 && scoped.iter().any(|(prefix, _)| &skey[..8] == prefix))
        });
        versions.rebuild_tree_index();
        for ctx in txns.values_mut() {
            ctx.read_keys.retain(|skey| {
                !(skey.len() >= 8 && scoped.iter().any(|(prefix, _)| &skey[..8] == prefix))
            });
            ctx.read_ranges
                .retain(|(p, _)| !scoped.iter().any(|(prefix, _)| p == prefix));
        }
        Ok(())
    }

    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    pub fn begin(&self) -> Result<TxnId> {
        self.begin_with_isolation(IsolationLevel::SnapshotIsolation)
    }

    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Result<TxnId> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let last_lsn = if let Some(ref wal) = self.wal {
            wal.append_record(LogRecord::new_begin(0, txn_id))?
        } else {
            INVALID_LSN
        };

        let mut txns = self.txns.lock().unwrap();
        let read_ts = self.oracle_ts.load(Ordering::SeqCst);
        let ctx = TransactionContext {
            txn_id,
            isolation,
            state: TxnState::Active,
            read_ts,
            commit_ts: None,
            last_lsn,
            write_set: Vec::new(),
            read_keys: HashSet::new(),
            read_ranges: Vec::new(),
            in_rollback: false,
            rollback_poisoned: false,
            commit_poisoned: false,
            poisoned_commit_lsn: None,
            abort_lsn_logged: None,
        };

        txns.insert(txn_id, ctx);
        Ok(txn_id)
    }

    pub fn isolation_level(&self, txn_id: TxnId) -> Option<IsolationLevel> {
        self.txns.lock().unwrap().get(&txn_id).map(|t| t.isolation)
    }

    pub fn txn_state(&self, txn_id: TxnId) -> Option<TxnState> {
        self.txns.lock().unwrap().get(&txn_id).map(|t| t.state)
    }

    pub fn is_in_rollback(&self, txn_id: TxnId) -> bool {
        self.txns
            .lock()
            .unwrap()
            .get(&txn_id)
            .is_some_and(|t| t.in_rollback)
    }

    pub fn is_rollback_poisoned(&self, txn_id: TxnId) -> bool {
        self.txns
            .lock()
            .unwrap()
            .get(&txn_id)
            .is_some_and(|t| t.rollback_poisoned)
    }

    pub fn is_commit_poisoned(&self, txn_id: TxnId) -> bool {
        self.txns
            .lock()
            .unwrap()
            .get(&txn_id)
            .is_some_and(|t| t.commit_poisoned)
    }

    pub fn oldest_active_snapshot_ts(&self) -> u64 {
        let txns = self.txns.lock().unwrap();
        let current_oracle_ts = self.oracle_ts.load(Ordering::SeqCst);
        txns.values()
            .filter(|ctx| {
                ctx.state == TxnState::Active
                    || ctx.state == TxnState::Committing
                    || ctx.in_rollback
            })
            .map(|ctx| ctx.read_ts)
            .min()
            .unwrap_or(current_oracle_ts)
    }

    pub fn tracked_txn_count(&self) -> usize {
        self.txns.lock().unwrap().len()
    }

    pub fn active_txn_count(&self) -> usize {
        self.txns
            .lock()
            .unwrap()
            .values()
            .filter(|ctx| ctx.state == TxnState::Active)
            .count()
    }

    pub fn mvcc_key_count(&self) -> usize {
        self.mvcc_versions.lock().unwrap().len()
    }

    pub fn mvcc_version_count(&self) -> usize {
        self.mvcc_versions
            .lock()
            .unwrap()
            .values()
            .map(Vec::len)
            .sum()
    }

    pub fn vacuum(&self) -> VacuumReport {
        let mut versions = self.mvcc_versions.lock().unwrap();
        let mut txns = self.txns.lock().unwrap();

        let current_oracle_ts = self.oracle_ts.load(Ordering::SeqCst);
        let horizon_ts = txns
            .values()
            .filter(|ctx| {
                ctx.state == TxnState::Active
                    || ctx.state == TxnState::Committing
                    || ctx.in_rollback
            })
            .map(|ctx| ctx.read_ts)
            .min()
            .unwrap_or(current_oracle_ts);

        let mut versions_pruned = 0usize;

        for chain in versions.values_mut() {
            if chain.is_empty() {
                continue;
            }

            let has_in_flight_abort = chain.iter().any(|v| {
                let xmin_rb = v.xmin != 0
                    && txns
                        .get(&v.xmin)
                        .is_some_and(|c| c.in_rollback || c.state == TxnState::Committing);
                let xmax_rb = v.xmax.is_some_and(|xmax| {
                    txns.get(&xmax)
                        .is_some_and(|c| c.in_rollback || c.state == TxnState::Committing)
                });
                xmin_rb || xmax_rb
            });
            if has_in_flight_abort {
                continue;
            }

            let before_abort_clean = chain.len();
            chain.retain(|v| {
                if v.xmin == 0 {
                    return true;
                }
                !txns
                    .get(&v.xmin)
                    .is_some_and(|c| c.state == TxnState::Aborted && !c.in_rollback)
            });
            versions_pruned += before_abort_clean - chain.len();

            for v in chain.iter_mut() {
                if let Some(xmax) = v.xmax
                    && txns
                        .get(&xmax)
                        .is_some_and(|c| c.state == TxnState::Aborted && !c.in_rollback)
                {
                    v.xmax = None;
                }
            }

            let before_prune = chain.len();
            chain.retain(|v| {
                let Some(xmax) = v.xmax else {
                    return true;
                };
                if let Some(deleter) = txns.get(&xmax)
                    && let TxnState::Committed { commit_ts } = deleter.state
                {
                    commit_ts > horizon_ts
                } else {
                    true
                }
            });
            versions_pruned += before_prune - chain.len();

            if chain.len() == 1 && chain[0].xmax.is_none() {
                let can_collapse = if chain[0].xmin == 0 {
                    chain[0].value.is_some()
                } else if let Some(creator) = txns.get(&chain[0].xmin)
                    && let TxnState::Committed { commit_ts } = creator.state
                {
                    commit_ts <= horizon_ts
                } else {
                    false
                };

                if can_collapse {
                    chain.clear();
                    versions_pruned += 1;
                }
            }
        }

        let before_keys = versions.len();
        versions.retain(|_, chain| !chain.is_empty());
        let keys_cleared = before_keys - versions.len();
        versions.rebuild_tree_index();

        if versions_pruned > 0 || keys_cleared > 0 {
            self.chain_epoch.fetch_add(1, Ordering::SeqCst);
        }

        let mut referenced_txns = HashSet::new();
        for chain in versions.values() {
            for v in chain {
                if v.xmin != 0 {
                    referenced_txns.insert(v.xmin);
                }
                if let Some(xmax) = v.xmax {
                    referenced_txns.insert(xmax);
                }
            }
        }

        for ctx in txns.values_mut() {
            if ctx.state != TxnState::Active
                && ctx.state != TxnState::Committing
                && !ctx.in_rollback
            {
                ctx.read_keys.clear();
                ctx.read_ranges.clear();
                ctx.write_set.clear();
            }
        }

        let before_txns = txns.len();
        txns.retain(|tid, ctx| {
            if ctx.state == TxnState::Active || ctx.state == TxnState::Committing || ctx.in_rollback
            {
                return true;
            }
            referenced_txns.contains(tid)
        });
        let txns_reclaimed = before_txns - txns.len();

        GLOBAL_METRICS.record_vacuum(
            versions_pruned as u64,
            keys_cleared as u64,
            txns_reclaimed as u64,
        );

        VacuumReport {
            horizon_ts,
            versions_pruned,
            keys_cleared,
            txns_reclaimed,
        }
    }

    pub fn update_page(
        &self,
        txn_id: TxnId,
        page_id: PageId,
        offset: u16,
        new_data: &[u8],
    ) -> Result<Lsn> {
        let bpm = self.bpm.as_ref().ok_or_else(|| {
            Error::TransactionError("BufferPoolManager not attached to TxnManager".to_string())
        })?;
        let off = offset as usize;
        let len = new_data.len();
        if off < PAGE_HEADER_SIZE || off + len > PAGE_SIZE {
            return Err(Error::PageCorrupted(
                "Transactional page update out of bounds".to_string(),
            ));
        }

        let _ = self.ensure_active_txn_info(txn_id)?;
        let lock_delta = self.lock_manager.acquire_with_rollback(
            txn_id,
            LockKey::Page(page_id),
            LockMode::Exclusive,
        )?;

        let res = (|| -> Result<Lsn> {
            let mut txns = self.txns.lock().unwrap();
            let ctx = txns.get_mut(&txn_id).ok_or_else(|| {
                Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
            })?;
            if ctx.state != TxnState::Active {
                return Err(Error::TransactionError(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }

            let _op_guard = bpm.acquire_op_shared();
            let current_page = bpm.fetch_page(page_id)?;
            let old_data = current_page.data[off..off + len].to_vec();
            bpm.unpin_page(page_id, false)?;

            let prev_lsn = ctx.last_lsn;
            let assigned_lsn = if let Some(ref wal) = self.wal {
                let lsn = wal.append_record(LogRecord::new_update(
                    0,
                    txn_id,
                    prev_lsn,
                    page_id,
                    offset,
                    new_data.to_vec(),
                    old_data.clone(),
                ))?;
                ctx.last_lsn = lsn;
                lsn
            } else {
                INVALID_LSN
            };

            bpm.update_page_data(page_id, |page| {
                page.data[off..off + len].copy_from_slice(new_data);
                if assigned_lsn != INVALID_LSN {
                    page.set_page_lsn(assigned_lsn);
                } else {
                    page.update_checksum();
                }
                Ok(())
            })?;

            ctx.write_set.push(UndoAction::PageSlice {
                page_id,
                offset,
                old_data,
                update_lsn: assigned_lsn,
                prev_lsn,
            });

            Ok(assigned_lsn)
        })();

        if res.is_err() {
            self.lock_manager.rollback_delta(txn_id, lock_delta);
        }
        res
    }

    fn ensure_active_txn_info(&self, txn_id: TxnId) -> Result<(IsolationLevel, u64)> {
        let txns = self.txns.lock().unwrap();
        let ctx = txns
            .get(&txn_id)
            .ok_or_else(|| Error::TransactionError(format!("Unknown transaction ID {}", txn_id)))?;
        if ctx.state != TxnState::Active {
            return Err(Error::TransactionError(format!(
                "Transaction {} is not active",
                txn_id
            )));
        }
        Ok((ctx.isolation, ctx.read_ts))
    }
    fn read_key_visible_value(
        &self,
        txn_id: TxnId,
        isolation: IsolationLevel,
        read_ts: u64,
        tree: &Arc<BPlusTree>,
        key: &[u8],
        skey: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let lock_key = LockKey::Key(skey.to_vec());

        // Agar koi writer is key par exclusive lock hold kar raha hai,
        // toh short yield karke wait karein taaki uncommitted B-Tree mutation leak na ho.
        let mut retries = 0;
        while retries < 200 {
            if !self
                .lock_manager
                .is_key_exclusively_locked_by_other(&lock_key, txn_id)
            {
                break;
            }
            std::thread::yield_now();
            retries += 1;
        }

        let versions = self.mvcc_versions.lock().unwrap();
        let txns = self.txns.lock().unwrap();

        if let Some(chain) = versions.get(skey)
            && !chain.is_empty()
        {
            for ver in chain.iter().rev() {
                if self.is_version_visible(txn_id, isolation, read_ts, ver, &txns) {
                    return Ok(ver.value.clone());
                }
            }
            return Ok(None);
        }

        drop(txns);
        // IMPORTANT: keep `versions` locked while reading the B+Tree. Writers mutate the
        // tree while holding this same mutex, so we can never observe an uncommitted
        // tree value for a key that has no MVCC chain (vacuum race fix).
        let res = tree.get(key);
        drop(versions);
        res
    }
    pub fn get(&self, txn_id: TxnId, tree: &Arc<BPlusTree>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let skey = Self::scoped_key(tree, key);
        let (isolation, read_ts) = self.ensure_active_txn_info(txn_id)?;
        let lock_delta = if isolation == IsolationLevel::Serializable {
            self.lock_manager.acquire_with_rollback(
                txn_id,
                LockKey::Key(skey.clone()),
                LockMode::Shared,
            )?
        } else {
            LockDelta::default()
        };

        let val_res = self.read_key_visible_value(txn_id, isolation, read_ts, tree, key, &skey);
        match val_res {
            Ok(val) => {
                let mut txns = self.txns.lock().unwrap();
                if let Some(ctx) = txns.get_mut(&txn_id) {
                    ctx.read_keys.insert(skey);
                }
                Ok(val)
            }
            Err(e) => {
                self.lock_manager.rollback_delta(txn_id, lock_delta);
                Err(e)
            }
        }
    }

    pub fn lock_range(&self, txn_id: TxnId, tree: &Arc<BPlusTree>, range: &KeyRange) -> Result<()> {
        let (isolation, _) = self.ensure_active_txn_info(txn_id)?;
        if isolation != IsolationLevel::Serializable || range.is_empty() {
            return Ok(());
        }
        let _ = self.range_scan(txn_id, tree, range)?;
        Ok(())
    }

    pub fn range_scan(
        &self,
        txn_id: TxnId,
        tree: &Arc<BPlusTree>,
        range: &KeyRange,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_scan_directed(txn_id, tree, range, false)
    }

    pub fn range_scan_directed(
        &self,
        txn_id: TxnId,
        tree: &Arc<BPlusTree>,
        range: &KeyRange,
        descending: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (isolation, read_ts) = self.ensure_active_txn_info(txn_id)?;
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let scan_started = std::time::Instant::now();
        let profile_scan = scan_profiling_enabled();

        let prefix = Self::tree_scope_prefix(tree);

        let direct_lock_started = std::time::Instant::now();
        let mut scan_lock_delta = if isolation == IsolationLevel::Serializable {
            self.lock_manager.acquire_with_rollback(
                txn_id,
                LockKey::Range {
                    tree_prefix: prefix,
                    range: range.clone(),
                },
                LockMode::Shared,
            )?
        } else {
            LockDelta::default()
        };
        let direct_lock_elapsed = direct_lock_started.elapsed();

        let scan_res = (|| -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let bounds_started = std::time::Instant::now();
            let chain_epoch0 = self.chain_epoch.load(Ordering::SeqCst);
            let mut candidate_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
            let mut raw_values: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            let tree_metrics_before = profile_scan.then(|| GLOBAL_METRICS.snapshot());
            let tree_range_started = std::time::Instant::now();
            let raw_entries = if descending {
                tree.range_scan_bounds_rev(range)?
            } else {
                tree.range_scan_bounds(range)?
            };
            let tree_range_elapsed = tree_range_started.elapsed();
            let (tree_page_reads, tree_buffer_hits, tree_buffer_misses, tree_buffer_evictions) =
                if let Some(before) = tree_metrics_before {
                    let after = GLOBAL_METRICS.snapshot();
                    (
                        after.page_reads.saturating_sub(before.page_reads),
                        after.buffer_hits.saturating_sub(before.buffer_hits),
                        after.buffer_misses.saturating_sub(before.buffer_misses),
                        after
                            .buffer_evictions
                            .saturating_sub(before.buffer_evictions),
                    )
                } else {
                    (0, 0, 0, 0)
                };

            let candidate_collect_started = std::time::Instant::now();
            let mut raw_entry_count = 0usize;
            for (key, value) in raw_entries {
                raw_entry_count += 1;
                candidate_keys.insert(key.clone());
                raw_values.insert(key, value);
            }
            let candidate_collect_elapsed = candidate_collect_started.elapsed();

            let mvcc_merge_started = std::time::Instant::now();
            let mvcc_index_keys_inspected = {
                let versions = self.mvcc_versions.lock().unwrap();
                let inspected = versions.extend_keys_in_range(prefix, range, &mut candidate_keys);
                GLOBAL_METRICS.record_mvcc_entries_inspected(inspected as u64);
                inspected
            };
            let mvcc_merge_elapsed = mvcc_merge_started.elapsed();
            let bounds_elapsed = bounds_started.elapsed();

            let serializable_locks_started = std::time::Instant::now();
            if isolation == IsolationLevel::Serializable {
                let mut decomposed = Vec::new();
                if candidate_keys.is_empty() {
                    decomposed.push(LockKey::Gap {
                        tree_prefix: prefix,
                        range: range.clone(),
                    });
                } else {
                    let keys_vec: Vec<&Vec<u8>> = candidate_keys.iter().collect();
                    let first_nk =
                        KeyRange::new(range.start.clone(), KeyBound::Included(keys_vec[0].clone()));
                    if !first_nk.is_empty() {
                        decomposed.push(LockKey::NextKey {
                            tree_prefix: prefix,
                            range: first_nk,
                        });
                    }
                    decomposed.push(LockKey::Key(Self::scoped_key(tree, keys_vec[0])));

                    for w in keys_vec.windows(2) {
                        let nk = KeyRange::new(
                            KeyBound::Excluded(w[0].clone()),
                            KeyBound::Included(w[1].clone()),
                        );
                        if !nk.is_empty() {
                            decomposed.push(LockKey::NextKey {
                                tree_prefix: prefix,
                                range: nk,
                            });
                        }
                        decomposed.push(LockKey::Key(Self::scoped_key(tree, w[1])));
                    }

                    let last_key = *keys_vec.last().unwrap();
                    let trail_gap =
                        KeyRange::new(KeyBound::Excluded(last_key.clone()), range.end.clone());
                    if !trail_gap.is_empty() {
                        decomposed.push(LockKey::Gap {
                            tree_prefix: prefix,
                            range: trail_gap,
                        });
                    }
                }
                let batch_delta = self.lock_manager.acquire_batch_with_rollback(
                    txn_id,
                    &decomposed,
                    LockMode::Shared,
                )?;
                scan_lock_delta.changes.extend(batch_delta.changes);
            }
            let serializable_locks_elapsed = serializable_locks_started.elapsed();

            let mut visible_entries = Vec::new();
            let ordered_keys: Vec<&Vec<u8>> = if descending {
                candidate_keys.iter().rev().collect()
            } else {
                candidate_keys.iter().collect()
            };

            const MVCC_SCAN_BATCH_SIZE: usize = 128;
            let mut mvcc_time_total = std::time::Duration::ZERO;
            let mut fallback_time_total = std::time::Duration::ZERO;
            let mut fallback_keys = 0usize;

            for key_batch in ordered_keys.chunks(MVCC_SCAN_BATCH_SIZE) {
                let mut resolved: Vec<(Vec<u8>, Option<Vec<u8>>, bool)> =
                    Vec::with_capacity(key_batch.len());

                let mvcc_chunk_started = std::time::Instant::now();
                {
                    let versions = self.mvcc_versions.lock().unwrap();
                    let txns = self.txns.lock().unwrap();
                    let chain_epoch_changed =
                        self.chain_epoch.load(Ordering::SeqCst) != chain_epoch0;
                    for key in key_batch {
                        let skey = Self::scoped_key(tree, (*key).as_slice());
                        if let Some(chain) = versions.get(&skey)
                            && !chain.is_empty()
                        {
                            let visible = chain
                                .iter()
                                .rev()
                                .find(|ver| {
                                    self.is_version_visible(txn_id, isolation, read_ts, ver, &txns)
                                })
                                .and_then(|ver| ver.value.clone());
                            resolved.push(((**key).clone(), visible, true));
                        } else {
                            if chain_epoch_changed {
                                let fresh = tree.get((**key).as_slice())?;
                                resolved.push(((**key).clone(), fresh, true));
                            } else {
                                resolved.push(((**key).clone(), None, false));
                            }
                        }
                    }
                }
                mvcc_time_total += mvcc_chunk_started.elapsed();

                let fallback_chunk_started = std::time::Instant::now();
                for (key, mvcc_value, had_mvcc_chain) in resolved {
                    let value = if had_mvcc_chain {
                        mvcc_value
                    } else {
                        fallback_keys += 1;
                        raw_values.remove(&key)
                    };
                    if let Some(value) = value {
                        visible_entries.push((key, value));
                    }
                }
                fallback_time_total += fallback_chunk_started.elapsed();
            }

            {
                let mut txns = self.txns.lock().unwrap();
                if let Some(ctx) = txns.get_mut(&txn_id) {
                    if isolation == IsolationLevel::Serializable {
                        ctx.read_ranges.push((prefix, range.clone()));
                    }
                    for key in &candidate_keys {
                        ctx.read_keys.insert(Self::scoped_key(tree, key));
                    }
                }
            }

            if profile_scan {
                println!(
                    "[PROFILE SCAN] total={:?} bounds_total={:?} tree_range={:?} tree_page_reads={} tree_buffer_hits={} tree_buffer_misses={} tree_buffer_evictions={} candidate_collect={:?} mvcc_index_merge={:?} direct_lock={:?} serializable_locks={:?} mvcc_resolution={:?} fallback_value_lookup={:?} raw_entries={} mvcc_index_keys_inspected={} candidates={} no_mvcc_chain={}",
                    scan_started.elapsed(),
                    bounds_elapsed,
                    tree_range_elapsed,
                    tree_page_reads,
                    tree_buffer_hits,
                    tree_buffer_misses,
                    tree_buffer_evictions,
                    candidate_collect_elapsed,
                    mvcc_merge_elapsed,
                    direct_lock_elapsed,
                    serializable_locks_elapsed,
                    mvcc_time_total,
                    fallback_time_total,
                    raw_entry_count,
                    mvcc_index_keys_inspected,
                    candidate_keys.len(),
                    fallback_keys,
                );
            }

            Ok(visible_entries)
        })();

        if scan_res.is_err() {
            self.lock_manager.rollback_delta(txn_id, scan_lock_delta);
        }
        GLOBAL_METRICS
            .record_mvcc_scan_ns(scan_started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        scan_res
    }

    pub fn scan(&self, txn_id: TxnId, tree: &Arc<BPlusTree>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_scan(txn_id, tree, &KeyRange::full())
    }

    pub fn scan_unlocked(
        &self,
        txn_id: TxnId,
        tree: &Arc<BPlusTree>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (isolation, read_ts) = self.ensure_active_txn_info(txn_id)?;
        let prefix = Self::tree_scope_prefix(tree);

        let chain_epoch0 = self.chain_epoch.load(Ordering::SeqCst);
        let mut candidate_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut raw_values: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for (key, value) in tree.range_scan_bounds(&KeyRange::full())? {
            candidate_keys.insert(key.clone());
            raw_values.insert(key, value);
        }

        {
            let versions = self.mvcc_versions.lock().unwrap();
            let inspected =
                versions.extend_keys_in_range(prefix, &KeyRange::full(), &mut candidate_keys);
            GLOBAL_METRICS.record_mvcc_entries_inspected(inspected as u64);
        }

        let mut visible_entries = Vec::new();
        let ordered_keys: Vec<&Vec<u8>> = candidate_keys.iter().collect();

        const MVCC_SCAN_BATCH_SIZE: usize = 128;
        for key_batch in ordered_keys.chunks(MVCC_SCAN_BATCH_SIZE) {
            let mut resolved: Vec<(Vec<u8>, Option<Vec<u8>>, bool)> =
                Vec::with_capacity(key_batch.len());
            {
                let versions = self.mvcc_versions.lock().unwrap();
                let txns = self.txns.lock().unwrap();
                let chain_epoch_changed = self.chain_epoch.load(Ordering::SeqCst) != chain_epoch0;
                for key in key_batch {
                    let skey = Self::scoped_key(tree, key.as_slice());
                    if let Some(chain) = versions.get(&skey)
                        && !chain.is_empty()
                    {
                        let visible = chain
                            .iter()
                            .rev()
                            .find(|ver| {
                                self.is_version_visible(txn_id, isolation, read_ts, ver, &txns)
                            })
                            .and_then(|ver| ver.value.clone());
                        resolved.push(((**key).clone(), visible, true));
                    } else {
                        if chain_epoch_changed {
                            let fresh = tree.get((**key).as_slice())?;
                            resolved.push(((**key).clone(), fresh, true));
                        } else {
                            resolved.push(((**key).clone(), None, false));
                        }
                    }
                }
            }

            for (key, mvcc_value, had_mvcc_chain) in resolved {
                let value = if had_mvcc_chain {
                    mvcc_value
                } else {
                    raw_values.remove(&key)
                };
                if let Some(value) = value {
                    visible_entries.push((key, value));
                }
            }
        }
        Ok(visible_entries)
    }

    fn is_version_visible(
        &self,
        reader_txn: TxnId,
        isolation: IsolationLevel,
        read_ts: u64,
        ver: &VersionedValue,
        txns: &HashMap<TxnId, TransactionContext>,
    ) -> bool {
        let created_visible = if ver.xmin == reader_txn || ver.xmin == 0 {
            true
        } else if let Some(creator) = txns.get(&ver.xmin) {
            match creator.state {
                TxnState::Committed { commit_ts } => match isolation {
                    IsolationLevel::ReadCommitted => true,
                    IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable => {
                        commit_ts <= read_ts
                    }
                },
                _ => false,
            }
        } else {
            false
        };

        if !created_visible {
            return false;
        }

        if let Some(xmax) = ver.xmax {
            if xmax == reader_txn {
                return false;
            }
            if let Some(deleter) = txns.get(&xmax)
                && let TxnState::Committed { commit_ts } = deleter.state
            {
                return match isolation {
                    IsolationLevel::ReadCommitted => false,
                    IsolationLevel::SnapshotIsolation | IsolationLevel::Serializable => {
                        commit_ts > read_ts
                    }
                };
            }
        }

        true
    }

    fn has_concurrent_committed_modification(
        chain: &[VersionedValue],
        txn_id: TxnId,
        read_ts: u64,
        txns: &HashMap<TxnId, TransactionContext>,
    ) -> bool {
        for ver in chain {
            if ver.xmin != 0
                && ver.xmin != txn_id
                && let Some(creator) = txns.get(&ver.xmin)
                && let TxnState::Committed { commit_ts } = creator.state
                && commit_ts > read_ts
            {
                return true;
            }
            if let Some(xmax) = ver.xmax
                && xmax != txn_id
                && let Some(deleter) = txns.get(&xmax)
                && let TxnState::Committed { commit_ts } = deleter.state
                && commit_ts > read_ts
            {
                return true;
            }
        }
        false
    }

    pub fn acquire_exclusive_keys(
        &self,
        txn_id: TxnId,
        targets: &[(&Arc<BPlusTree>, Vec<u8>)],
    ) -> Result<()> {
        let _ = self.ensure_active_txn_info(txn_id)?;
        let lock_keys: Vec<LockKey> = targets
            .iter()
            .map(|(tree, k)| LockKey::Key(Self::scoped_key(tree, k)))
            .collect();
        self.lock_manager
            .acquire_batch(txn_id, &lock_keys, LockMode::Exclusive)
    }

    pub fn put(
        &self,
        txn_id: TxnId,
        tree: &Arc<BPlusTree>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let result = self.write_key_internal(txn_id, tree, key, Some(value));
        if let Some(started) = started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::TxnPut,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    pub fn delete(&self, txn_id: TxnId, tree: &Arc<BPlusTree>, key: &[u8]) -> Result<bool> {
        let (isolation, read_ts) = self.ensure_active_txn_info(txn_id)?;
        let skey = Self::scoped_key(tree, key);
        let lock_delta = self.lock_manager.acquire_with_rollback(
            txn_id,
            LockKey::Key(skey.clone()),
            LockMode::Exclusive,
        )?;

        let existing =
            match self.read_key_visible_value(txn_id, isolation, read_ts, tree, key, &skey) {
                Ok(v) => v,
                Err(e) => {
                    self.lock_manager.rollback_delta(txn_id, lock_delta);
                    return Err(e);
                }
            };

        {
            let mut txns = self.txns.lock().unwrap();
            if let Some(ctx) = txns.get_mut(&txn_id) {
                ctx.read_keys.insert(skey);
            }
        }

        if existing.is_none() {
            return Ok(false);
        }
        if let Err(e) = self.write_key_internal(txn_id, tree, key, None) {
            self.lock_manager.rollback_delta(txn_id, lock_delta);
            return Err(e);
        }
        Ok(true)
    }

    fn write_key_internal(
        &self,
        txn_id: TxnId,
        tree: &Arc<BPlusTree>,
        key: &[u8],
        new_value: Option<&[u8]>,
    ) -> Result<()> {
        let profiling = mitrdb_observe::profiling_enabled();
        let _ = self.ensure_active_txn_info(txn_id)?;
        let skey = Self::scoped_key(tree, key);

        let lock_started = profiling.then(std::time::Instant::now);
        let lock_delta = self.lock_manager.acquire_with_rollback(
            txn_id,
            LockKey::Key(skey.clone()),
            LockMode::Exclusive,
        )?;
        if let Some(started) = lock_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::LockAcquire,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }

        let res = (|| -> Result<()> {
            let mvcc_lock_started = profiling.then(std::time::Instant::now);
            let mut versions = self.mvcc_versions.lock().unwrap();
            if let Some(started) = mvcc_lock_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::MvccMutexWait,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            let txn_lock_started = profiling.then(std::time::Instant::now);
            let mut txns = self.txns.lock().unwrap();
            if let Some(started) = txn_lock_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::TxnMutexWait,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            let (isolation, read_ts, mut last_lsn) = {
                let ctx = txns.get(&txn_id).ok_or_else(|| {
                    Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
                })?;
                if ctx.state != TxnState::Active {
                    return Err(Error::TransactionError(format!(
                        "Transaction {} is not active",
                        txn_id
                    )));
                }
                (ctx.isolation, ctx.read_ts, ctx.last_lsn)
            };

            if let Some(chain) = versions.get(&skey)
                && let Some(latest) = chain.last()
                && latest.xmin != txn_id
                && let Some(writer) = txns.get(&latest.xmin)
            {
                match writer.state {
                    TxnState::Active | TxnState::Committing => {
                        return Err(Error::WriteConflict(format!(
                            "Key modified by active/committing transaction {}",
                            latest.xmin
                        )));
                    }
                    TxnState::Aborted if writer.in_rollback => {
                        return Err(Error::WriteConflict(format!(
                            "Key modified by rolling-back transaction {}",
                            latest.xmin
                        )));
                    }
                    TxnState::Committed { commit_ts }
                        if isolation != IsolationLevel::ReadCommitted && commit_ts > read_ts =>
                    {
                        return Err(Error::WriteConflict(format!(
                            "First-committer-wins conflict with transaction {}",
                            latest.xmin
                        )));
                    }
                    _ => {}
                }
            }

            let btree_started = profiling.then(std::time::Instant::now);
            let old_btree_val = match new_value {
                Some(val) => tree.insert_or_replace_logged(txn_id, &mut last_lsn, key, val)?,
                None => {
                    let old = tree.get(key)?;
                    let _ = tree.delete_logged(txn_id, &mut last_lsn, key)?;
                    old
                }
            };
            if let Some(started) = btree_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::BtreeMutation,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            let version_chain_started = profiling.then(std::time::Instant::now);
            {
                let chain = versions.entry(skey).or_default();
                if chain.is_empty()
                    && let Some(ref baseline) = old_btree_val
                {
                    chain.push(VersionedValue {
                        xmin: 0,
                        xmax: Some(txn_id),
                        value: Some(baseline.clone()),
                    });
                    chain.push(VersionedValue {
                        xmin: txn_id,
                        xmax: None,
                        value: new_value.map(|v| v.to_vec()),
                    });
                } else if let Some(prev) = chain.last_mut()
                    && prev.xmax.is_none()
                {
                    if prev.xmin == txn_id {
                        prev.value = new_value.map(|v| v.to_vec());
                    } else {
                        prev.xmax = Some(txn_id);
                        chain.push(VersionedValue {
                            xmin: txn_id,
                            xmax: None,
                            value: new_value.map(|v| v.to_vec()),
                        });
                    }
                } else {
                    chain.push(VersionedValue {
                        xmin: txn_id,
                        xmax: None,
                        value: new_value.map(|v| v.to_vec()),
                    });
                }
            }
            versions.index_tree_key(Self::tree_scope_prefix(tree), key);
            if let Some(started) = version_chain_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::VersionChainMutation,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }

            let ctx = txns.get_mut(&txn_id).unwrap();
            ctx.last_lsn = last_lsn;
            ctx.write_set.push(UndoAction::BTreeKey {
                tree: tree.clone(),
                key: key.to_vec(),
                old_value: old_btree_val,
            });

            Ok(())
        })();

        if res.is_err() {
            self.lock_manager.rollback_delta(txn_id, lock_delta);
        }
        res
    }
    pub fn commit(&self, txn_id: TxnId) -> Result<u64> {
        let profiling = mitrdb_observe::profiling_enabled();
        let commit_started = profiling.then(std::time::Instant::now);
        let versions = self.mvcc_versions.lock().unwrap();
        let txn_lock_started = std::time::Instant::now();
        let mut txns = self.txns.lock().unwrap();
        GLOBAL_METRICS.record_txn_mutex_wait_ns(
            txn_lock_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let txn_lock_hold_started = std::time::Instant::now();

        let (isolation, read_ts, has_writes, read_keys, read_ranges, last_lsn) = {
            let ctx = txns.get(&txn_id).ok_or_else(|| {
                Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
            })?;
            if ctx.state != TxnState::Active {
                return Err(Error::TransactionError(format!(
                    "Cannot commit non-active transaction {}",
                    txn_id
                )));
            }
            (
                ctx.isolation,
                ctx.read_ts,
                !ctx.write_set.is_empty(),
                ctx.read_keys.clone(),
                ctx.read_ranges.clone(),
                ctx.last_lsn,
            )
        };

        if isolation == IsolationLevel::Serializable && has_writes {
            for skey in &read_keys {
                if let Some(chain) = versions.get(skey)
                    && Self::has_concurrent_committed_modification(chain, txn_id, read_ts, &txns)
                {
                    return Err(Error::WriteConflict(format!(
                        "Serializable read-set conflict on transaction {}",
                        txn_id
                    )));
                }
            }
            for (prefix, range) in &read_ranges {
                for raw_key in versions.keys_in_range(*prefix, range) {
                    let mut scoped_key = Vec::with_capacity(8 + raw_key.len());
                    scoped_key.extend_from_slice(prefix);
                    scoped_key.extend_from_slice(&raw_key);
                    if let Some(chain) = versions.get(&scoped_key)
                        && Self::has_concurrent_committed_modification(
                            chain, txn_id, read_ts, &txns,
                        )
                    {
                        return Err(Error::WriteConflict(format!(
                            "Serializable predicate-range conflict on transaction {}",
                            txn_id
                        )));
                    }
                }
            }
        }
        drop(versions);

        let ctx = txns.get_mut(&txn_id).expect("transaction validated above");
        ctx.state = TxnState::Committing;
        GLOBAL_METRICS.record_txn_commit_lock_hold_ns(
            txn_lock_hold_started
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );
        drop(txns);

        let commit_lsn = if let Some(ref wal) = self.wal {
            let lsn = match wal.append_record(LogRecord::new_commit(0, txn_id, last_lsn)) {
                Ok(lsn) => lsn,
                Err(err) => {
                    #[allow(clippy::collapsible_if)]
                    if let Some(ctx) = self.txns.lock().unwrap().get_mut(&txn_id) {
                        if ctx.state == TxnState::Committing && !ctx.commit_poisoned {
                            ctx.state = TxnState::Active;
                        }
                    }
                    return Err(err);
                }
            };
            // Read-only txn ke paas durable karne ko kuch nahi: commit record buffer mein jata hai, fsync skip.
            let mut flush_res = if has_writes { wal.flush_up_to(lsn) } else { Ok(()) };
            // Error branch only: WAL flush error sticky nahi hai aur pending bytes bache rehte hain,
            // to bounded retry transient I/O failure ko poison hone se pehle resolve kar deta hai.
            if flush_res.is_err() {
                for attempt in 0..5u32 {
                    std::thread::sleep(std::time::Duration::from_millis(10u64 << attempt));
                    flush_res = wal.flush_up_to(lsn);
                    if flush_res.is_ok() {
                        break;
                    }
                }
            }
            if let Err(err) = flush_res {
                if wal.flushed_lsn() < lsn {
                    if let Some(ctx) = self.txns.lock().unwrap().get_mut(&txn_id) {
                        ctx.commit_poisoned = true;
                        ctx.poisoned_commit_lsn = Some(lsn);
                        self.poisoned_commits.fetch_add(1, Ordering::SeqCst);
                    }
                    return Err(Error::TransactionError(format!(
                        "Transaction {} commit is poisoned after WAL flush failure; restart/crash recovery required to resolve in-doubt state: {}",
                        txn_id, err
                    )));
                }
            }
            Some(lsn)
        } else {
            None
        };

        self.finish_commit(txn_id, commit_lsn, commit_started)
    }

    /// Poisoned commit (WAL flush kept failing) ko aage badhane ka raasta: commit record dobara
    /// flush karo, durable ho jaye to commit pura karo (commit_ts, locks release).
    /// Abort kabhi nahi: record durable ho sakta hai (doosre txn ka flush use likh deta hai).
    pub fn resolve_poisoned_commit(&self, txn_id: TxnId) -> Result<u64> {
        let lsn = {
            let txns = self.txns.lock().unwrap();
            let ctx = txns.get(&txn_id).ok_or_else(|| {
                Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
            })?;
            if ctx.state != TxnState::Committing || !ctx.commit_poisoned {
                return Err(Error::TransactionError(format!(
                    "Transaction {} has no poisoned commit to resolve",
                    txn_id
                )));
            }
            ctx.poisoned_commit_lsn.ok_or_else(|| {
                Error::TransactionError(format!("Transaction {} has no recorded commit LSN", txn_id))
            })?
        };
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| Error::TransactionError("no WAL attached".to_string()))?;
        if let Err(err) = wal.flush_up_to(lsn) {
            if wal.flushed_lsn() < lsn {
                return Err(Error::TransactionError(format!(
                    "Transaction {} commit is still poisoned: {}",
                    txn_id, err
                )));
            }
        }
        self.finish_commit(txn_id, Some(lsn), None)
    }

    /// Bulk statement ke beech WAL arena `limit_bytes` se bada ho to abhi flush karo. Uncommitted records
    /// flush karna safe hai (recovery loser txn ko undo karta hai); memory N se independent rehti hai.
    pub fn relieve_wal_pressure(&self, limit_bytes: usize) -> Result<()> {
        if let Some(ref wal) = self.wal
            && wal.pending_bytes_len() > limit_bytes
        {
            let last = wal.next_lsn().saturating_sub(1);
            if last > 0 {
                wal.flush_up_to(last)?;
            }
        }
        Ok(())
    }

    pub fn poisoned_commit_count(&self) -> usize {
        self.poisoned_commits.load(Ordering::Relaxed)
    }

    /// Sweep: har poisoned commit par resolve_poisoned_commit ek baar try karta hai.
    /// Flush `txns` lock ke bahar hota hai. Kitne resolve hue wo count lautata hai.
    pub fn resolve_poisoned_commits(&self) -> usize {
        if self.poisoned_commits.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let ids: Vec<TxnId> = {
            let txns = self.txns.lock().unwrap();
            txns.iter()
                .filter(|(_, c)| c.state == TxnState::Committing && c.commit_poisoned)
                .map(|(id, _)| *id)
                .collect()
        };
        let mut done = 0;
        for id in ids {
            if self.resolve_poisoned_commit(id).is_ok() {
                done += 1;
            }
        }
        done
    }

    fn finish_commit(
        &self,
        txn_id: TxnId,
        commit_lsn: Option<Lsn>,
        commit_started: Option<std::time::Instant>,
    ) -> Result<u64> {
        let txn_lock_started = std::time::Instant::now();
        let mut txns = self.txns.lock().unwrap();
        GLOBAL_METRICS.record_txn_mutex_wait_ns(
            txn_lock_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let ctx = txns.get_mut(&txn_id).ok_or_else(|| {
            Error::TransactionError(format!("Transaction {} disappeared during commit", txn_id))
        })?;
        if let TxnState::Committed { commit_ts } = ctx.state {
            // already completed (e.g. concurrent resolve_poisoned_commit)
            return Ok(commit_ts);
        }
        if let Some(lsn) = commit_lsn {
            ctx.last_lsn = lsn;
        }
        let commit_ts = self.oracle_ts.fetch_add(1, Ordering::SeqCst) + 1;
        ctx.state = TxnState::Committed { commit_ts };
        ctx.commit_ts = Some(commit_ts);
        if ctx.commit_poisoned {
            self.poisoned_commits.fetch_sub(1, Ordering::SeqCst);
        }
        ctx.commit_poisoned = false;
        ctx.poisoned_commit_lsn = None;
        ctx.write_set.clear();
        ctx.read_keys.clear();
        ctx.read_ranges.clear();
        drop(txns);

        let free_res = if let Some(ref bpm) = self.bpm {
            bpm.finalize_free_pages(txn_id)
        } else if let Some(ref wal) = self.wal {
            wal.notify_txn_finalized(txn_id)
        } else {
            Ok(())
        };
        self.lock_manager.release_all(txn_id);
        free_res?;
        if let Some(started) = commit_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::TxnCommit,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        Ok(commit_ts)
    }

    /// Number of undo entries so far; used as a statement-rollback mark.
    pub fn write_mark(&self, txn_id: TxnId) -> Result<usize> {
        let txns = self.txns.lock().unwrap();
        let ctx = txns.get(&txn_id).ok_or_else(|| {
            Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
        })?;
        Ok(ctx.write_set.len())
    }

    /// Statement-level rollback: undo only writes made after `mark`; txn stays Active.
    /// Caller must abort the whole txn if this returns Err.
    pub fn rollback_to_mark(&self, txn_id: TxnId, mark: usize) -> Result<()> {
        let (mut last_lsn, earlier) = {
            let txns = self.txns.lock().unwrap();
            let ctx = txns.get(&txn_id).ok_or_else(|| {
                Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
            })?;
            if ctx.state != TxnState::Active || ctx.rollback_poisoned || ctx.commit_poisoned {
                return Err(Error::TransactionError(format!(
                    "Transaction {} cannot roll back a statement in its current state",
                    txn_id
                )));
            }
            let earlier: HashSet<Vec<u8>> = ctx
                .write_set
                .iter()
                .take(mark)
                .filter_map(|a| match a {
                    UndoAction::BTreeKey { tree, key, .. } => Some(Self::scoped_key(tree, key)),
                    _ => None,
                })
                .collect();
            (ctx.last_lsn, earlier)
        };

        let mut undone = false;
        loop {
            let next_action = {
                let txns = self.txns.lock().unwrap();
                txns.get(&txn_id).and_then(|ctx| {
                    if ctx.write_set.len() > mark {
                        ctx.write_set.last().cloned()
                    } else {
                        None
                    }
                })
            };
            let Some(action) = next_action else {
                break;
            };
            match action {
                UndoAction::PageSlice { .. } => {
                    return Err(Error::TransactionError(
                        "statement rollback does not support page-slice undo".to_string(),
                    ));
                }
                UndoAction::BTreeKey {
                    tree,
                    key,
                    old_value,
                } => {
                    let lsn_before = last_lsn;
                    let undo_res = match &old_value {
                        Some(val) => tree
                            .insert_or_replace_logged(txn_id, &mut last_lsn, &key, val)
                            .map(|_| ()),
                        None => tree.delete_logged(txn_id, &mut last_lsn, &key).map(|_| ()),
                    };
                    if let Err(e) = undo_res {
                        let mut txns = self.txns.lock().unwrap();
                        if let Some(ctx) = txns.get_mut(&txn_id) {
                            ctx.last_lsn = last_lsn;
                            if last_lsn != lsn_before || self.wal.is_none() {
                                ctx.rollback_poisoned = true;
                            }
                        }
                        return Err(e);
                    }
                    let skey = Self::scoped_key(&tree, &key);
                    {
                        let mut versions = self.mvcc_versions.lock().unwrap();
                        if let Some(chain) = versions.get_mut(&skey) {
                            if earlier.contains(&skey) {
                                if let Some(last) = chain.last_mut()
                                    && last.xmin == txn_id
                                {
                                    last.value = old_value.clone();
                                }
                            } else {
                                chain.retain(|v| v.xmin != txn_id);
                                for v in chain.iter_mut() {
                                    if v.xmax == Some(txn_id) {
                                        v.xmax = None;
                                    }
                                }
                            }
                        }
                    }
                    undone = true;
                }
            }
            {
                let mut txns = self.txns.lock().unwrap();
                if let Some(ctx) = txns.get_mut(&txn_id) {
                    ctx.last_lsn = last_lsn;
                    ctx.write_set.pop();
                }
            }
        }

        if undone {
            let mut versions = self.mvcc_versions.lock().unwrap();
            versions.retain(|_, chain| !chain.is_empty());
            self.chain_epoch.fetch_add(1, Ordering::SeqCst);
            versions.rebuild_tree_index();
        }
        Ok(())
    }

    pub fn abort(&self, txn_id: TxnId) -> Result<()> {
        let mut last_lsn = {
            let mut txns = self.txns.lock().unwrap();
            let ctx = txns.get_mut(&txn_id).ok_or_else(|| {
                Error::TransactionError(format!("Unknown transaction ID {}", txn_id))
            })?;
            if ctx.rollback_poisoned {
                return Err(Error::TransactionError(format!(
                    "Transaction {} rollback is poisoned after partial undo failure; crash recovery required",
                    txn_id
                )));
            }
            match ctx.state {
                TxnState::Active => {
                    ctx.state = TxnState::Aborted;
                    ctx.in_rollback = true;
                }
                TxnState::Aborted if ctx.in_rollback => {}
                _ => {
                    return Err(Error::TransactionError(format!(
                        "Cannot abort non-active transaction {}",
                        txn_id
                    )));
                }
            }
            ctx.last_lsn
        };

        loop {
            let next_action = {
                let txns = self.txns.lock().unwrap();
                txns.get(&txn_id)
                    .and_then(|ctx| ctx.write_set.last().cloned())
            };
            let Some(action) = next_action else {
                break;
            };

            match action {
                UndoAction::PageSlice {
                    page_id,
                    offset,
                    old_data,
                    prev_lsn,
                    ..
                } => {
                    let _op_guard = self.bpm.as_ref().map(|b| b.acquire_op_shared());
                    if let Some(ref bpm) = self.bpm {
                        bpm.unmark_page_free(page_id);
                        let _ = bpm.fetch_page(page_id)?;
                    }
                    let clr_lsn = if let Some(ref wal) = self.wal {
                        match wal.append_record(LogRecord::new_clr(
                            0,
                            txn_id,
                            last_lsn,
                            page_id,
                            offset,
                            prev_lsn,
                            old_data.clone(),
                        )) {
                            Ok(lsn) => {
                                last_lsn = lsn;
                                lsn
                            }
                            Err(e) => {
                                if let Some(ref bpm) = self.bpm {
                                    let _ = bpm.unpin_page(page_id, false);
                                }
                                return Err(e);
                            }
                        }
                    } else {
                        INVALID_LSN
                    };

                    if let Some(ref bpm) = self.bpm {
                        let off = offset as usize;
                        let len = old_data.len();
                        let update_res = bpm.update_page_data(page_id, |page| {
                            page.data[off..off + len].copy_from_slice(&old_data);
                            if clr_lsn != INVALID_LSN {
                                page.set_page_lsn(clr_lsn);
                            } else {
                                page.update_checksum();
                            }
                            Ok(())
                        });
                        let _ = bpm.unpin_page(page_id, update_res.is_ok());
                        drop(_op_guard);
                        if let Err(e) = update_res {
                            let mut txns = self.txns.lock().unwrap();
                            if let Some(ctx) = txns.get_mut(&txn_id) {
                                ctx.last_lsn = last_lsn;
                                if clr_lsn != INVALID_LSN {
                                    ctx.rollback_poisoned = true;
                                }
                            }
                            return Err(e);
                        }
                    } else {
                        drop(_op_guard);
                    }
                }
                UndoAction::BTreeKey {
                    tree,
                    key,
                    old_value,
                } => {
                    let lsn_before = last_lsn;
                    let undo_res = match old_value {
                        Some(val) => tree
                            .insert_or_replace_logged(txn_id, &mut last_lsn, &key, &val)
                            .map(|_| ()),
                        None => tree.delete_logged(txn_id, &mut last_lsn, &key).map(|_| ()),
                    };
                    if let Err(e) = undo_res {
                        let mut txns = self.txns.lock().unwrap();
                        if let Some(ctx) = txns.get_mut(&txn_id) {
                            ctx.last_lsn = last_lsn;
                            if last_lsn != lsn_before || self.wal.is_none() {
                                ctx.rollback_poisoned = true;
                            }
                        }
                        return Err(e);
                    }
                }
            }

            {
                let mut txns = self.txns.lock().unwrap();
                if let Some(ctx) = txns.get_mut(&txn_id) {
                    ctx.last_lsn = last_lsn;
                    ctx.write_set.pop();
                }
            }
        }

        if let Some(ref wal) = self.wal {
            let already_logged = {
                let txns = self.txns.lock().unwrap();
                txns.get(&txn_id).and_then(|ctx| ctx.abort_lsn_logged)
            };
            let abort_lsn = match already_logged {
                Some(lsn) => lsn,
                None => {
                    let lsn = wal.append_record(LogRecord::new_abort(0, txn_id, last_lsn))?;
                    let mut txns = self.txns.lock().unwrap();
                    if let Some(ctx) = txns.get_mut(&txn_id) {
                        ctx.last_lsn = lsn;
                        ctx.abort_lsn_logged = Some(lsn);
                    }
                    lsn
                }
            };
            wal.flush_up_to(abort_lsn)?;
        }

        {
            let mut versions = self.mvcc_versions.lock().unwrap();
            let mut txns = self.txns.lock().unwrap();
            for chain in versions.values_mut() {
                chain.retain(|v| v.xmin != txn_id);
                for v in chain.iter_mut() {
                    if v.xmax == Some(txn_id) {
                        v.xmax = None;
                    }
                }
                // if chain.len() == 1 && chain[0].xmin == 0 && chain[0].xmax.is_none() {
                //     chain.clear();
                // }
            }
            versions.retain(|_, chain| !chain.is_empty());
            self.chain_epoch.fetch_add(1, Ordering::SeqCst);
            versions.rebuild_tree_index();

            if let Some(ctx) = txns.get_mut(&txn_id) {
                ctx.read_keys.clear();
                ctx.read_ranges.clear();
                ctx.write_set.clear();
                ctx.in_rollback = false;
            }
        }

        let free_res = if let Some(ref bpm) = self.bpm {
            bpm.finalize_free_pages(txn_id)
        } else if let Some(ref wal) = self.wal {
            wal.notify_txn_finalized(txn_id)
        } else {
            Ok(())
        };
        self.lock_manager.release_all(txn_id);
        free_res?;
        Ok(())
    }
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod mvcc_tree_index_tests {
    use super::*;

    fn scoped(prefix: [u8; 8], key: &[u8]) -> Vec<u8> {
        let mut out = prefix.to_vec();
        out.extend_from_slice(key);
        out
    }

    fn version(xmin: TxnId, value: u8) -> Vec<VersionedValue> {
        vec![VersionedValue {
            xmin,
            xmax: None,
            value: Some(vec![value]),
        }]
    }

    #[test]
    fn tree_index_scopes_keys_and_applies_inclusive_range_bounds() {
        let tree_a = 1u64.to_le_bytes();
        let tree_b = 2u64.to_le_bytes();
        let mut store = MvccVersionStore::default();
        store.versions.insert(scoped(tree_a, b"a"), version(1, 1));
        store.versions.insert(scoped(tree_a, b"b"), version(2, 2));
        store.versions.insert(scoped(tree_a, b"c"), version(3, 3));
        store.versions.insert(scoped(tree_b, b"b"), version(4, 4));
        store.rebuild_tree_index();

        assert_eq!(
            store.keys_in_range(tree_a, &KeyRange::inclusive(b"b".to_vec(), b"c".to_vec())),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            store.keys_in_range(tree_b, &KeyRange::full()),
            vec![b"b".to_vec()]
        );
        assert!(
            store
                .keys_in_range(tree_a, &KeyRange::exclusive(b"b".to_vec(), b"b".to_vec()))
                .is_empty()
        );
    }

    #[test]
    fn rebuilding_tree_index_drops_empty_mvcc_chains() {
        let tree = 7u64.to_le_bytes();
        let mut store = MvccVersionStore::default();
        store
            .versions
            .insert(scoped(tree, b"visible"), version(1, 1));
        store.versions.insert(scoped(tree, b"empty"), Vec::new());
        store.rebuild_tree_index();

        assert_eq!(
            store.keys_in_range(tree, &KeyRange::full()),
            vec![b"visible".to_vec()]
        );
    }
}
