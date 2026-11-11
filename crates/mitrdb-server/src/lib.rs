#![allow(
    clippy::type_complexity,
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::ptr_arg
)]
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod network;
pub use network::{TcpServerConfig, TcpServerHandle};

pub use mitrdb_catalog::Catalog;

pub use mitrdb_common::{Error, Result, TxnId};
pub use mitrdb_exec::{ExecutionResult, Executor};
use mitrdb_observe::{GLOBAL_METRICS, MetricsSnapshot};
pub use mitrdb_query::{PhysicalPlan, QueryEngine, TableSchema, Value};
pub use mitrdb_storage::{
    BackupFileMeta, BackupManager, BackupManifest, CheckpointMetadata, RestoreReport,
};
use mitrdb_storage::{
    BufferPoolManager, DEFAULT_WAL_SEGMENT_BYTES, DiskManager, RecoveryManager, RecoveryReport,
    WalManager,
};
pub use mitrdb_txn::{
    IsolationLevel, KeyBound, KeyRange, LockKey, LockMode, TxnManager, VacuumReport,
};

pub type SessionId = u64;

pub struct DatabaseSession {
    active_txn: Mutex<Option<TxnId>>,
    isolation: Mutex<IsolationLevel>,
}

impl DatabaseSession {
    pub const fn new() -> Self {
        Self::with_isolation(IsolationLevel::SnapshotIsolation)
    }

    pub const fn with_isolation(isolation: IsolationLevel) -> Self {
        DatabaseSession {
            active_txn: Mutex::new(None),
            isolation: Mutex::new(isolation),
        }
    }
}

impl Default for DatabaseSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Auto-checkpoint: trigger when total WAL bytes (segments * max segment size) reach this.
const DEFAULT_AUTO_CHECKPOINT_WAL_BYTES: u64 = 256 * 1024 * 1024;
/// Only look at the WAL every N statements, so the hot path pays one atomic add.
const AUTO_CHECKPOINT_CHECK_EVERY: u64 = 256;

pub struct Server {
    catalog: Arc<Catalog>,
    txn_mgr: Arc<TxnManager>,
    disk: Option<Arc<DiskManager>>,
    bpm: Option<Arc<BufferPoolManager>>,
    wal_mgr: Option<Arc<WalManager>>,
    recovery_mgr: Option<Arc<RecoveryManager>>,
    query_engine: QueryEngine,
    executor: Executor,
    default_session: DatabaseSession,
    next_session_id: AtomicU64,
    sessions: Mutex<HashMap<SessionId, Arc<DatabaseSession>>>,
    backup_lock: Mutex<()>,
    auto_ckpt_wal_bytes: AtomicU64,
    auto_ckpt_tick: AtomicU64,
    auto_ckpt_floor: AtomicU64,
    auto_ckpt_running: std::sync::atomic::AtomicBool,
}

impl Server {
    pub fn new() -> Self {
        let catalog = Arc::new(Catalog::new());
        let txn_mgr = Arc::new(TxnManager::new());
        let query_engine = QueryEngine::new(catalog.clone());
        let executor = Executor::new();
        Server {
            catalog,
            txn_mgr,
            disk: None,
            bpm: None,
            wal_mgr: None,
            recovery_mgr: None,
            query_engine,
            executor,
            default_session: DatabaseSession::new(),
            next_session_id: AtomicU64::new(1),
            sessions: Mutex::new(HashMap::new()),
            backup_lock: Mutex::new(()),
            auto_ckpt_wal_bytes: AtomicU64::new(0),
            auto_ckpt_tick: AtomicU64::new(0),
            auto_ckpt_floor: AtomicU64::new(0),
            auto_ckpt_running: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn open<P: AsRef<Path>>(
        db_path: P,
        wal_path: P,
        pool_size: usize,
    ) -> Result<(Self, RecoveryReport)> {
        Self::open_with_segment_size(db_path, wal_path, pool_size, DEFAULT_WAL_SEGMENT_BYTES)
    }

    pub fn open_with_segment_size<P: AsRef<Path>>(
        db_path: P,
        wal_path: P,
        pool_size: usize,
        max_segment_bytes: u64,
    ) -> Result<(Self, RecoveryReport)> {
        let db_copy = db_path.as_ref().to_path_buf();
        let wal_copy = wal_path.as_ref().to_path_buf();
        let disk = Arc::new(DiskManager::open_or_create(db_path)?);
        let wal = Arc::new(WalManager::open_with_segment_size(
            wal_path,
            max_segment_bytes,
        )?);
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), pool_size));
        let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));
        let report = recovery.recover()?;
        perms::restrict_db_files(&db_copy, &wal_copy);

        let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));

        // Post-recovery reconciliation: prune uncommitted keys whose physical undo was skipped
        if !report.skipped_undos.is_empty() {
            let trees = catalog.all_table_trees();
            for tree in trees {
                if let Ok(records) =
                    tree.range_scan_bounds(&mitrdb_storage::btree::KeyRange::full())
                {
                    for (k, _) in records {
                        for skipped in &report.skipped_undos {
                            if skipped
                                .redo_data
                                .windows(k.len())
                                .any(|window| window == k.as_slice())
                            {
                                let mut dummy_lsn = 0;
                                let _ = tree.delete_logged(skipped.txn_id, &mut dummy_lsn, &k);
                            }
                        }
                    }
                }
            }
        }

        let txn_mgr = Arc::new(TxnManager::with_storage(wal.clone(), bpm.clone()));
        let query_engine = QueryEngine::new(catalog.clone());
        let executor = Executor::new();

        Ok((
            Server {
                catalog,
                txn_mgr,
                disk: Some(disk),
                bpm: Some(bpm),
                wal_mgr: Some(wal),
                recovery_mgr: Some(recovery),
                query_engine,
                executor,
                default_session: DatabaseSession::new(),
                next_session_id: AtomicU64::new(1),
                sessions: Mutex::new(HashMap::new()),
                backup_lock: Mutex::new(()),
                auto_ckpt_wal_bytes: AtomicU64::new(DEFAULT_AUTO_CHECKPOINT_WAL_BYTES),
                auto_ckpt_tick: AtomicU64::new(0),
                auto_ckpt_floor: AtomicU64::new(0),
                auto_ckpt_running: std::sync::atomic::AtomicBool::new(false),
            },
            report,
        ))
    }

    /// Test hook (fault-injection feature only): allow `n` more page ops, then fail one (-1 = off).
    #[cfg(feature = "fault-injection")]
    pub fn set_fail_page_op_after(&self, n: i64) {
        if let Some(ref bpm) = self.bpm {
            bpm.set_fail_page_op_after(n);
        }
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_fail_wal_append_after(&self, n: i64) {
        if let Some(ref wal) = self.wal_mgr {
            wal.set_fail_append_after(n);
        }
    }

    pub fn start(&self) -> Result<()> {
        Ok(())
    }

    pub fn create_session(&self) -> SessionId {
        self.create_session_with_isolation(IsolationLevel::SnapshotIsolation)
    }

    pub fn create_session_with_isolation(&self, isolation: IsolationLevel) -> SessionId {
        let id = self.next_session_id.fetch_add(1, Ordering::SeqCst);
        self.sessions
            .lock()
            .unwrap()
            .insert(id, Arc::new(DatabaseSession::with_isolation(isolation)));
        id
    }

    pub fn set_default_isolation(&self, isolation: IsolationLevel) {
        *self.default_session.isolation.lock().unwrap() = isolation;
    }

    pub fn set_session_isolation(
        &self,
        session_id: SessionId,
        isolation: IsolationLevel,
    ) -> Result<()> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(&session_id)
            .cloned()
            .ok_or_else(|| Error::TransactionError(format!("Unknown session {}", session_id)))?;
        *session.isolation.lock().unwrap() = isolation;
        Ok(())
    }

    pub fn close_session(&self, session_id: SessionId) -> Result<()> {
        let session_opt = self.sessions.lock().unwrap().remove(&session_id);
        if let Some(session) = session_opt
            && let Some(tid) = session.active_txn.lock().unwrap().take()
        {
            let abort_res = self.txn_mgr.abort(tid);
            let sync_res = self.catalog.sync_roots();
            GLOBAL_METRICS.record_txn_abort();
            abort_res?;
            sync_res?;
        }
        Ok(())
    }

    pub fn execute_sql(&self, sql: &str) -> Result<ExecutionResult> {
        let isolation = *self.default_session.isolation.lock().unwrap();
        let mut active_txn = self.default_session.active_txn.lock().unwrap();
        self.execute_with_txn_slot(sql, &mut active_txn, isolation)
    }

    /// SQL with `$1, $2, ...` placeholders; values are bound, never spliced into the text.
    pub fn execute_sql_params(
        &self,
        sql: &str,
        params: &[mitrdb_query::Value],
    ) -> Result<ExecutionResult> {
        let isolation = *self.default_session.isolation.lock().unwrap();
        let mut active_txn = self.default_session.active_txn.lock().unwrap();
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan_params(sql, params)?;
        self.execute_plan_with_txn_slot(plan, &mut active_txn, isolation)
    }

    pub fn execute_in_session(&self, session_id: SessionId, sql: &str) -> Result<ExecutionResult> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(&session_id)
            .cloned()
            .ok_or_else(|| Error::TransactionError(format!("Unknown session {}", session_id)))?;
        let isolation = *session.isolation.lock().unwrap();
        let mut active_txn = session.active_txn.lock().unwrap();
        self.execute_with_txn_slot(sql, &mut active_txn, isolation)
    }

    pub fn execute_plan(&self, plan: PhysicalPlan) -> Result<ExecutionResult> {
        let isolation = *self.default_session.isolation.lock().unwrap();
        let mut active_txn = self.default_session.active_txn.lock().unwrap();
        self.execute_plan_with_txn_slot(plan, &mut active_txn, isolation)
    }

    pub fn execute_plan_in_session(
        &self,
        session_id: SessionId,
        plan: PhysicalPlan,
    ) -> Result<ExecutionResult> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(&session_id)
            .cloned()
            .ok_or_else(|| Error::TransactionError(format!("Unknown session {}", session_id)))?;
        let isolation = *session.isolation.lock().unwrap();
        let mut active_txn = session.active_txn.lock().unwrap();
        self.execute_plan_with_txn_slot(plan, &mut active_txn, isolation)
    }

    pub fn get_table_schema(&self, name: &str) -> Result<Option<TableSchema>> {
        self.catalog.get_table_schema(name)
    }

    fn execute_with_txn_slot(
        &self,
        sql: &str,
        active_txn: &mut Option<TxnId>,
        default_isolation: IsolationLevel,
    ) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let profiling = mitrdb_observe::profiling_enabled();
        let parse_started = profiling.then(std::time::Instant::now);
        let plan = self.query_engine.parse_and_plan(sql)?;
        if let Some(started) = parse_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::ParsePlan,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        self.execute_plan_with_txn_slot(plan, active_txn, default_isolation)
    }

    fn execute_plan_with_txn_slot(
        &self,
        plan: PhysicalPlan,
        active_txn: &mut Option<TxnId>,
        default_isolation: IsolationLevel,
    ) -> Result<ExecutionResult> {
        let profiling = mitrdb_observe::profiling_enabled();
        match &plan {
            PhysicalPlan::SeqScan { .. } => GLOBAL_METRICS.record_seq_scan(),
            PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexOrderedScan { .. } => GLOBAL_METRICS.record_index_scan(),
            PhysicalPlan::Aggregate { input, .. } | PhysicalPlan::GroupAggregate { input, .. } => {
                match input.as_ref() {
                    PhysicalPlan::SeqScan { .. } => GLOBAL_METRICS.record_seq_scan(),
                    PhysicalPlan::IndexScan { .. }
                    | PhysicalPlan::IndexRangeScan { .. }
                    | PhysicalPlan::IndexOrderedScan { .. } => GLOBAL_METRICS.record_index_scan(),
                    _ => {}
                }
            }
            PhysicalPlan::Begin => GLOBAL_METRICS.record_txn_begin(),
            PhysicalPlan::Commit => GLOBAL_METRICS.record_txn_commit(),
            PhysicalPlan::Rollback => GLOBAL_METRICS.record_txn_abort(),
            PhysicalPlan::Checkpoint => {
                if self.txn_mgr.active_txn_count() == 0 {
                    self.catalog.sync_roots()?;
                }
                if let Some(ref rec) = self.recovery_mgr {
                    rec.create_checkpoint()?;
                }
                return Ok(ExecutionResult::Ok);
            }
            _ => {}
        }
        // Poisoned commit (WAL flush failure) ho to WAL theek hote hi statement se pehle resolve try karo.
        if self.txn_mgr.poisoned_commit_count() > 0 {
            self.txn_mgr.resolve_poisoned_commits();
        }
        let execution_started = profiling.then(std::time::Instant::now);
        let result = self.executor.execute_plan_with_isolation(
            plan,
            &self.catalog,
            &self.txn_mgr,
            active_txn,
            default_isolation,
        );
        if let Some(started) = execution_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::PlanExecution,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        self.maybe_auto_checkpoint();
        result
    }

    /// Auto-checkpoint threshold in total WAL bytes; 0 disables it.
    pub fn set_auto_checkpoint_wal_bytes(&self, bytes: u64) {
        self.auto_ckpt_wal_bytes
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    fn maybe_auto_checkpoint(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let threshold = self.auto_ckpt_wal_bytes.load(Relaxed);
        if threshold == 0 {
            return;
        }
        if !self
            .auto_ckpt_tick
            .fetch_add(1, Relaxed)
            .is_multiple_of(AUTO_CHECKPOINT_CHECK_EVERY)
        {
            return;
        }
        let Some(wal) = self.wal_mgr.as_ref() else {
            return;
        };
        let wal_bytes = (wal.segment_count() as u64).saturating_mul(wal.max_segment_bytes());
        // Back off when a long txn keeps the WAL pinned: next trigger needs real growth.
        let floor = self.auto_ckpt_floor.load(Relaxed);
        if wal_bytes < threshold || wal_bytes < floor.saturating_add(threshold / 2) {
            return;
        }
        if self
            .auto_ckpt_running
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_err()
        {
            return;
        }
        // A failed auto-checkpoint must not fail the user's statement; it is counted instead.
        if self.checkpoint_with_meta().is_err() {
            GLOBAL_METRICS.checkpoint_errors.fetch_add(1, Relaxed);
        }
        let after = (wal.segment_count() as u64).saturating_mul(wal.max_segment_bytes());
        self.auto_ckpt_floor.store(after, Relaxed);
        self.auto_ckpt_running.store(false, Relaxed);
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _ = self.checkpoint_with_meta()?;
        Ok(())
    }

    pub fn checkpoint_with_meta(&self) -> Result<Option<CheckpointMetadata>> {
        if self.txn_mgr.active_txn_count() == 0 {
            self.catalog.sync_roots()?;
        }
        if let Some(ref rec) = self.recovery_mgr {
            let meta = rec.create_checkpoint_with_meta()?;
            return Ok(Some(meta));
        }
        Ok(None)
    }

    /// Finish poisoned commits (WAL flush failed earlier) now, without waiting for the next
    /// statement. Call it from an idle loop. Returns how many were resolved.
    pub fn resolve_poisoned(&self) -> usize {
        self.txn_mgr.resolve_poisoned_commits()
    }

    pub fn vacuum(&self) -> VacuumReport {
        self.txn_mgr.vacuum()
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        GLOBAL_METRICS.snapshot()
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn txn_manager(&self) -> &Arc<TxnManager> {
        &self.txn_mgr
    }

    pub fn wal_manager(&self) -> Option<&Arc<WalManager>> {
        self.wal_mgr.as_ref()
    }

    pub fn backup<P: AsRef<Path>>(&self, dest_dir: P) -> Result<BackupManifest> {
        let _guard = self.backup_lock.lock().unwrap();
        let disk = self.disk.as_ref().ok_or_else(|| {
            Error::BackupError(
                "Cannot backup an in-memory server without persistent storage".to_string(),
            )
        })?;
        let bpm = self.bpm.as_ref().ok_or_else(|| {
            Error::BackupError("Cannot backup an in-memory server without buffer pool".to_string())
        })?;
        let wal = self.wal_mgr.as_ref().ok_or_else(|| {
            Error::BackupError("Cannot backup an in-memory server without WAL".to_string())
        })?;
        let rec = self.recovery_mgr.as_ref().ok_or_else(|| {
            Error::BackupError(
                "Cannot backup an in-memory server without recovery manager".to_string(),
            )
        })?;
        if self.txn_mgr.active_txn_count() == 0 {
            self.catalog.sync_roots()?;
        }
        let result = BackupManager::create_online_backup(disk, bpm, wal, rec, dest_dir.as_ref());
        if result.is_ok() {
            perms::restrict_tree(dest_dir.as_ref());
        }
        result
    }

    pub fn restore<P: AsRef<Path>, Q: AsRef<Path>>(
        backup_dir: P,
        dest_db_dir: Q,
    ) -> Result<RestoreReport> {
        BackupManager::restore_backup(backup_dir.as_ref(), dest_db_dir.as_ref())
    }

    pub fn validate_backup<P: AsRef<Path>>(backup_dir: P) -> Result<BackupManifest> {
        BackupManager::validate_backup(backup_dir.as_ref())
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

mod files;
mod perms;
pub use files::{FileStore, StoredFile};
mod table;
pub use table::{Delete, IntoValue, Query, Rows, Table, Update, val};

mod schema_builder;
pub use schema_builder::{ColumnBuilder, TableBuilder};
