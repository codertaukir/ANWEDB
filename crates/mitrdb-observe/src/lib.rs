use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub page_reads: u64,
    pub page_writes: u64,
    pub buffer_hits: u64,
    pub buffer_misses: u64,
    pub buffer_evictions: u64,
    pub wal_appends: u64,
    pub wal_flushes: u64,
    pub wal_sync_count: u64,
    pub wal_sync_total_ns: u64,
    pub wal_sync_max_ns: u64,
    pub wal_flush_lock_wait_count: u64,
    pub wal_flush_lock_wait_total_ns: u64,
    pub wal_flush_lock_wait_max_ns: u64,
    pub wal_flush_total_count: u64,
    pub wal_flush_total_ns: u64,
    pub wal_flush_max_ns: u64,
    pub txn_mutex_wait_count: u64,
    pub txn_mutex_wait_total_ns: u64,
    pub txn_mutex_wait_max_ns: u64,
    pub txn_commit_lock_hold_count: u64,
    pub txn_commit_lock_hold_total_ns: u64,
    pub txn_commit_lock_hold_max_ns: u64,
    pub mvcc_scan_count: u64,
    pub mvcc_scan_total_ns: u64,
    pub mvcc_scan_max_ns: u64,
    pub mvcc_entries_inspected: u64,
    pub btree_root_wait_count: u64,
    pub btree_root_wait_total_ns: u64,
    pub btree_root_wait_max_ns: u64,
    pub recovery_redos: u64,
    pub recovery_undos: u64,
    pub txns_begun: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub queries_executed: u64,
    pub seq_scans: u64,
    pub index_scans: u64,
    pub vacuum_runs: u64,
    pub mvcc_versions_pruned: u64,
    pub mvcc_keys_cleared: u64,
    pub mvcc_txns_reclaimed: u64,
    pub wal_rotations: u64,
    pub wal_segments_reclaimed: u64,
    pub checkpoints_created: u64,
    pub checkpoint_errors: u64,
    pub backups_created: u64,
    pub backups_restored: u64,
}

pub struct Metrics {
    pub page_reads: AtomicU64,
    pub page_writes: AtomicU64,
    pub buffer_hits: AtomicU64,
    pub buffer_misses: AtomicU64,
    pub buffer_evictions: AtomicU64,
    pub wal_appends: AtomicU64,
    pub wal_flushes: AtomicU64,
    pub wal_sync_count: AtomicU64,
    pub wal_sync_total_ns: AtomicU64,
    pub wal_sync_max_ns: AtomicU64,
    pub wal_flush_lock_wait_count: AtomicU64,
    pub wal_flush_lock_wait_total_ns: AtomicU64,
    pub wal_flush_lock_wait_max_ns: AtomicU64,
    pub wal_flush_total_count: AtomicU64,
    pub wal_flush_total_ns: AtomicU64,
    pub wal_flush_max_ns: AtomicU64,
    pub txn_mutex_wait_count: AtomicU64,
    pub txn_mutex_wait_total_ns: AtomicU64,
    pub txn_mutex_wait_max_ns: AtomicU64,
    pub txn_commit_lock_hold_count: AtomicU64,
    pub txn_commit_lock_hold_total_ns: AtomicU64,
    pub txn_commit_lock_hold_max_ns: AtomicU64,
    pub mvcc_scan_count: AtomicU64,
    pub mvcc_scan_total_ns: AtomicU64,
    pub mvcc_scan_max_ns: AtomicU64,
    pub mvcc_entries_inspected: AtomicU64,
    pub btree_root_wait_count: AtomicU64,
    pub btree_root_wait_total_ns: AtomicU64,
    pub btree_root_wait_max_ns: AtomicU64,
    pub recovery_redos: AtomicU64,
    pub recovery_undos: AtomicU64,
    pub txns_begun: AtomicU64,
    pub txns_committed: AtomicU64,
    pub txns_aborted: AtomicU64,
    pub queries_executed: AtomicU64,
    pub seq_scans: AtomicU64,
    pub index_scans: AtomicU64,
    pub vacuum_runs: AtomicU64,
    pub mvcc_versions_pruned: AtomicU64,
    pub mvcc_keys_cleared: AtomicU64,
    pub mvcc_txns_reclaimed: AtomicU64,
    pub wal_rotations: AtomicU64,
    pub wal_segments_reclaimed: AtomicU64,
    pub checkpoints_created: AtomicU64,
    pub checkpoint_errors: AtomicU64,
    pub backups_created: AtomicU64,
    pub backups_restored: AtomicU64,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            page_reads: AtomicU64::new(0),
            page_writes: AtomicU64::new(0),
            buffer_hits: AtomicU64::new(0),
            buffer_misses: AtomicU64::new(0),
            buffer_evictions: AtomicU64::new(0),
            wal_appends: AtomicU64::new(0),
            wal_flushes: AtomicU64::new(0),
            wal_sync_count: AtomicU64::new(0),
            wal_sync_total_ns: AtomicU64::new(0),
            wal_sync_max_ns: AtomicU64::new(0),
            wal_flush_lock_wait_count: AtomicU64::new(0),
            wal_flush_lock_wait_total_ns: AtomicU64::new(0),
            wal_flush_lock_wait_max_ns: AtomicU64::new(0),
            wal_flush_total_count: AtomicU64::new(0),
            wal_flush_total_ns: AtomicU64::new(0),
            wal_flush_max_ns: AtomicU64::new(0),
            txn_mutex_wait_count: AtomicU64::new(0),
            txn_mutex_wait_total_ns: AtomicU64::new(0),
            txn_mutex_wait_max_ns: AtomicU64::new(0),
            txn_commit_lock_hold_count: AtomicU64::new(0),
            txn_commit_lock_hold_total_ns: AtomicU64::new(0),
            txn_commit_lock_hold_max_ns: AtomicU64::new(0),
            mvcc_scan_count: AtomicU64::new(0),
            mvcc_scan_total_ns: AtomicU64::new(0),
            mvcc_scan_max_ns: AtomicU64::new(0),
            mvcc_entries_inspected: AtomicU64::new(0),
            btree_root_wait_count: AtomicU64::new(0),
            btree_root_wait_total_ns: AtomicU64::new(0),
            btree_root_wait_max_ns: AtomicU64::new(0),
            recovery_redos: AtomicU64::new(0),
            recovery_undos: AtomicU64::new(0),
            txns_begun: AtomicU64::new(0),
            txns_committed: AtomicU64::new(0),
            txns_aborted: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            seq_scans: AtomicU64::new(0),
            index_scans: AtomicU64::new(0),
            vacuum_runs: AtomicU64::new(0),
            mvcc_versions_pruned: AtomicU64::new(0),
            mvcc_keys_cleared: AtomicU64::new(0),
            mvcc_txns_reclaimed: AtomicU64::new(0),
            wal_rotations: AtomicU64::new(0),
            wal_segments_reclaimed: AtomicU64::new(0),
            checkpoints_created: AtomicU64::new(0),
            checkpoint_errors: AtomicU64::new(0),
            backups_created: AtomicU64::new(0),
            backups_restored: AtomicU64::new(0),
        }
    }

    pub fn record_read(&self) {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write(&self) {
        self.page_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_hit(&self) {
        self.buffer_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.buffer_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.buffer_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_append(&self) {
        self.wal_appends.fetch_add(1, Ordering::Relaxed);
    }

    fn record_duration(count: &AtomicU64, total: &AtomicU64, max: &AtomicU64, nanos: u64) {
        count.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(nanos, Ordering::Relaxed);
        max.fetch_max(nanos, Ordering::Relaxed);
    }

    pub fn record_wal_sync_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.wal_sync_count,
            &self.wal_sync_total_ns,
            &self.wal_sync_max_ns,
            nanos,
        );
    }

    pub fn record_txn_mutex_wait_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.txn_mutex_wait_count,
            &self.txn_mutex_wait_total_ns,
            &self.txn_mutex_wait_max_ns,
            nanos,
        );
    }

    pub fn record_wal_flush_lock_wait_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.wal_flush_lock_wait_count,
            &self.wal_flush_lock_wait_total_ns,
            &self.wal_flush_lock_wait_max_ns,
            nanos,
        );
    }

    pub fn record_wal_flush_total_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.wal_flush_total_count,
            &self.wal_flush_total_ns,
            &self.wal_flush_max_ns,
            nanos,
        );
    }

    pub fn record_txn_commit_lock_hold_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.txn_commit_lock_hold_count,
            &self.txn_commit_lock_hold_total_ns,
            &self.txn_commit_lock_hold_max_ns,
            nanos,
        );
    }

    pub fn record_mvcc_scan_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.mvcc_scan_count,
            &self.mvcc_scan_total_ns,
            &self.mvcc_scan_max_ns,
            nanos,
        );
    }

    pub fn record_mvcc_entries_inspected(&self, count: u64) {
        self.mvcc_entries_inspected
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_btree_root_wait_ns(&self, nanos: u64) {
        Self::record_duration(
            &self.btree_root_wait_count,
            &self.btree_root_wait_total_ns,
            &self.btree_root_wait_max_ns,
            nanos,
        );
    }

    pub fn record_wal_flush(&self) {
        self.wal_flushes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_rotation(&self) {
        self.wal_rotations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_segments_reclaimed(&self, count: u64) {
        self.wal_segments_reclaimed
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self) {
        self.checkpoints_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backup(&self) {
        self.backups_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_restore(&self) {
        self.backups_restored.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_recovery_redo(&self) {
        self.recovery_redos.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_recovery_undo(&self) {
        self.recovery_undos.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_txn_begin(&self) {
        self.txns_begun.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_txn_commit(&self) {
        self.txns_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_txn_abort(&self) {
        self.txns_aborted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query(&self) {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_seq_scan(&self) {
        self.seq_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_index_scan(&self) {
        self.index_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_vacuum(&self, versions_pruned: u64, keys_cleared: u64, txns_reclaimed: u64) {
        self.vacuum_runs.fetch_add(1, Ordering::Relaxed);
        self.mvcc_versions_pruned
            .fetch_add(versions_pruned, Ordering::Relaxed);
        self.mvcc_keys_cleared
            .fetch_add(keys_cleared, Ordering::Relaxed);
        self.mvcc_txns_reclaimed
            .fetch_add(txns_reclaimed, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            page_reads: self.page_reads.load(Ordering::Relaxed),
            page_writes: self.page_writes.load(Ordering::Relaxed),
            buffer_hits: self.buffer_hits.load(Ordering::Relaxed),
            buffer_misses: self.buffer_misses.load(Ordering::Relaxed),
            buffer_evictions: self.buffer_evictions.load(Ordering::Relaxed),
            wal_appends: self.wal_appends.load(Ordering::Relaxed),
            wal_flushes: self.wal_flushes.load(Ordering::Relaxed),
            wal_sync_count: self.wal_sync_count.load(Ordering::Relaxed),
            wal_sync_total_ns: self.wal_sync_total_ns.load(Ordering::Relaxed),
            wal_sync_max_ns: self.wal_sync_max_ns.load(Ordering::Relaxed),
            wal_flush_lock_wait_count: self.wal_flush_lock_wait_count.load(Ordering::Relaxed),
            wal_flush_lock_wait_total_ns: self.wal_flush_lock_wait_total_ns.load(Ordering::Relaxed),
            wal_flush_lock_wait_max_ns: self.wal_flush_lock_wait_max_ns.load(Ordering::Relaxed),
            wal_flush_total_count: self.wal_flush_total_count.load(Ordering::Relaxed),
            wal_flush_total_ns: self.wal_flush_total_ns.load(Ordering::Relaxed),
            wal_flush_max_ns: self.wal_flush_max_ns.load(Ordering::Relaxed),
            txn_mutex_wait_count: self.txn_mutex_wait_count.load(Ordering::Relaxed),
            txn_mutex_wait_total_ns: self.txn_mutex_wait_total_ns.load(Ordering::Relaxed),
            txn_mutex_wait_max_ns: self.txn_mutex_wait_max_ns.load(Ordering::Relaxed),
            txn_commit_lock_hold_count: self.txn_commit_lock_hold_count.load(Ordering::Relaxed),
            txn_commit_lock_hold_total_ns: self
                .txn_commit_lock_hold_total_ns
                .load(Ordering::Relaxed),
            txn_commit_lock_hold_max_ns: self.txn_commit_lock_hold_max_ns.load(Ordering::Relaxed),
            mvcc_scan_count: self.mvcc_scan_count.load(Ordering::Relaxed),
            mvcc_scan_total_ns: self.mvcc_scan_total_ns.load(Ordering::Relaxed),
            mvcc_scan_max_ns: self.mvcc_scan_max_ns.load(Ordering::Relaxed),
            mvcc_entries_inspected: self.mvcc_entries_inspected.load(Ordering::Relaxed),
            btree_root_wait_count: self.btree_root_wait_count.load(Ordering::Relaxed),
            btree_root_wait_total_ns: self.btree_root_wait_total_ns.load(Ordering::Relaxed),
            btree_root_wait_max_ns: self.btree_root_wait_max_ns.load(Ordering::Relaxed),
            recovery_redos: self.recovery_redos.load(Ordering::Relaxed),
            recovery_undos: self.recovery_undos.load(Ordering::Relaxed),
            txns_begun: self.txns_begun.load(Ordering::Relaxed),
            txns_committed: self.txns_committed.load(Ordering::Relaxed),
            txns_aborted: self.txns_aborted.load(Ordering::Relaxed),
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            seq_scans: self.seq_scans.load(Ordering::Relaxed),
            index_scans: self.index_scans.load(Ordering::Relaxed),
            vacuum_runs: self.vacuum_runs.load(Ordering::Relaxed),
            mvcc_versions_pruned: self.mvcc_versions_pruned.load(Ordering::Relaxed),
            mvcc_keys_cleared: self.mvcc_keys_cleared.load(Ordering::Relaxed),
            mvcc_txns_reclaimed: self.mvcc_txns_reclaimed.load(Ordering::Relaxed),
            wal_rotations: self.wal_rotations.load(Ordering::Relaxed),
            wal_segments_reclaimed: self.wal_segments_reclaimed.load(Ordering::Relaxed),
            checkpoints_created: self.checkpoints_created.load(Ordering::Relaxed),
            checkpoint_errors: self.checkpoint_errors.load(Ordering::Relaxed),
            backups_created: self.backups_created.load(Ordering::Relaxed),
            backups_restored: self.backups_restored.load(Ordering::Relaxed),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

pub static GLOBAL_METRICS: Metrics = Metrics::new();

/// Opt-in phase timing for the resource benchmark. Disabled unless
/// MITRDB_PROFILE_INSERT=1 is present before process startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum ProfilePhase {
    ParsePlan = 0,
    PlanExecution = 1,
    InsertExecution = 2,
    TxnPut = 3,
    TxnCommit = 4,
    WalAppend = 5,
    WalFlush = 6,
    LockAcquire = 7,
    MvccMutexWait = 8,
    TxnMutexWait = 9,
    BtreeMutation = 10,
    VersionChainMutation = 11,
    BtreeTraversal = 12,
    BtreePageWrite = 13,
    BtreeLeafSplit = 14,
    BtreeInternalSplit = 15,
    BufferPoolFetch = 16,
    BufferPoolDiskRead = 17,
    SplitMidSelection = 18,
    SplitPageAllocation = 19,
    SplitRedistribution = 20,
    SplitParentUpdate = 21,
    LockMutexWait = 22,
    LockTableScan = 23,
    LockBookkeeping = 24,
    LockConditionWait = 25,
    BufferPoolNewPage = 26,
    BufferPoolVictimSelect = 27,
    BufferPoolDiskAllocate = 28,
    BufferPoolUnpin = 29,
}

pub const PROFILE_PHASES: usize = 30;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileDuration {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileSnapshot {
    pub phases: [ProfileDuration; PROFILE_PHASES],
}

static PROFILE_COUNTS: [AtomicU64; PROFILE_PHASES] = [const { AtomicU64::new(0) }; PROFILE_PHASES];
static PROFILE_TOTAL_NS: [AtomicU64; PROFILE_PHASES] =
    [const { AtomicU64::new(0) }; PROFILE_PHASES];
static PROFILE_MAX_NS: [AtomicU64; PROFILE_PHASES] = [const { AtomicU64::new(0) }; PROFILE_PHASES];

pub fn profiling_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("MITRDB_PROFILE_INSERT").is_some_and(|value| value.to_str() == Some("1"))
    })
}

pub fn record_profile_duration(phase: ProfilePhase, nanos: u64) {
    if !profiling_enabled() {
        return;
    }
    let index = phase as usize;
    PROFILE_COUNTS[index].fetch_add(1, Ordering::Relaxed);
    PROFILE_TOTAL_NS[index].fetch_add(nanos, Ordering::Relaxed);
    PROFILE_MAX_NS[index].fetch_max(nanos, Ordering::Relaxed);
}

pub fn profile_snapshot() -> ProfileSnapshot {
    ProfileSnapshot {
        phases: std::array::from_fn(|index| ProfileDuration {
            count: PROFILE_COUNTS[index].load(Ordering::Relaxed),
            total_ns: PROFILE_TOTAL_NS[index].load(Ordering::Relaxed),
            max_ns: PROFILE_MAX_NS[index].load(Ordering::Relaxed),
        }),
    }
}

impl ProfileSnapshot {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            phases: std::array::from_fn(|index| ProfileDuration {
                count: self.phases[index]
                    .count
                    .saturating_sub(earlier.phases[index].count),
                total_ns: self.phases[index]
                    .total_ns
                    .saturating_sub(earlier.phases[index].total_ns),
                // Maxima are cumulative; the per-stage max cannot be recovered
                // by subtraction, so callers should use totals/counts for deltas.
                max_ns: self.phases[index].max_ns,
            }),
        }
    }
}

#[cfg(test)]
mod profiling_tests {
    use super::{PROFILE_PHASES, ProfileDuration, ProfileSnapshot};

    #[test]
    fn profile_delta_subtracts_counts_and_totals_without_underflow() {
        let mut before = [ProfileDuration::default(); PROFILE_PHASES];
        before[0] = ProfileDuration {
            count: 2,
            total_ns: 100,
            max_ns: 70,
        };
        let before = ProfileSnapshot { phases: before };
        let mut after = [ProfileDuration::default(); PROFILE_PHASES];
        after[0] = ProfileDuration {
            count: 5,
            total_ns: 460,
            max_ns: 200,
        };
        let delta = ProfileSnapshot { phases: after }.delta_since(before);
        assert_eq!(delta.phases[0].count, 3);
        assert_eq!(delta.phases[0].total_ns, 360);
    }

    #[test]
    fn profile_delta_saturates_when_counters_are_reset() {
        let before = ProfileSnapshot {
            phases: [ProfileDuration {
                count: 9,
                total_ns: 900,
                max_ns: 900,
            }; PROFILE_PHASES],
        };
        let delta = ProfileSnapshot::default().delta_since(before);
        assert_eq!(delta.phases[0].count, 0);
        assert_eq!(delta.phases[0].total_ns, 0);
    }
}
