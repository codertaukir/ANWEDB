#![allow(
    clippy::type_complexity,
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::ptr_arg
)]
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod network;
pub use network::{TcpServerConfig, TcpServerHandle};

pub use mitrdb_catalog::Catalog;

pub use mitrdb_common::{Error, Result, TxnId};
pub use mitrdb_exec::{ExecutionResult, Executor};
use mitrdb_observe::{GLOBAL_METRICS, MetricsSnapshot};
pub use mitrdb_query::{F64, PhysicalPlan, QueryEngine, TableSchema, Value};
pub use mitrdb_storage::{
    BackupFileMeta, BackupManager, BackupManifest, CheckpointMetadata, RestoreReport,
};
use mitrdb_storage::{BlobReader, BlobWriter, OverflowDescriptorV2};
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
    blob_writer: Mutex<Option<BlobWriter>>,
    blob_reader: Mutex<Option<BlobReader>>,
}

impl DatabaseSession {
    pub const fn new() -> Self {
        Self::with_isolation(IsolationLevel::SnapshotIsolation)
    }

    pub const fn with_isolation(isolation: IsolationLevel) -> Self {
        DatabaseSession {
            active_txn: Mutex::new(None),
            isolation: Mutex::new(isolation),
            blob_writer: Mutex::new(None),
            blob_reader: Mutex::new(None),
        }
    }
}

impl Default for DatabaseSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Auto-checkpoint: trigger when total WAL bytes (segments * max segment size) reach this.
const DEFAULT_AUTO_CHECKPOINT_WAL_BYTES: u64 = 64 * 1024 * 1024;
/// Only look at the WAL every N statements, so the hot path pays one atomic add.
const AUTO_CHECKPOINT_CHECK_EVERY: u64 = 16;

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
    sessions: RwLock<HashMap<SessionId, Arc<DatabaseSession>>>,
    backup_lock: Mutex<()>,
    auto_ckpt_wal_bytes: AtomicU64,
    auto_ckpt_tick: AtomicU64,
    auto_ckpt_floor: AtomicU64,
    auto_ckpt_running: std::sync::atomic::AtomicBool,
    max_blob_bytes: AtomicU64,
    _dir_lock: Option<std::fs::File>,
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
            sessions: RwLock::new(HashMap::new()),
            backup_lock: Mutex::new(()),
            auto_ckpt_wal_bytes: AtomicU64::new(0),
            auto_ckpt_tick: AtomicU64::new(0),
            auto_ckpt_floor: AtomicU64::new(0),
            auto_ckpt_running: std::sync::atomic::AtomicBool::new(false),
            max_blob_bytes: AtomicU64::new(mitrdb_storage::DEFAULT_MAX_BLOB_BYTES),
            _dir_lock: None,
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
        let (server, report) = Self::open_inner(db_path, wal_path, pool_size, max_segment_bytes)?;
        // Recovery replayed the whole WAL; checkpoint now so the next restart does not
        // replay it again. A failure here must not fail open: it is counted instead.
        if let Some(wal) = server.wal_mgr.as_ref() {
            let wal_bytes = (wal.segment_count() as u64).saturating_mul(wal.max_segment_bytes());
            if wal_bytes > 8 * 1024 * 1024 && server.checkpoint_with_meta().is_err() {
                GLOBAL_METRICS
                    .checkpoint_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok((server, report))
    }

    fn open_inner<P: AsRef<Path>>(
        db_path: P,
        wal_path: P,
        pool_size: usize,
        max_segment_bytes: u64,
    ) -> Result<(Self, RecoveryReport)> {
        let db_copy = db_path.as_ref().to_path_buf();
        let wal_copy = wal_path.as_ref().to_path_buf();
        // Exclusive per-directory lock BEFORE touching db/WAL: recovery truncates the WAL,
        // so a second process must be refused before it gets that far.
        let lock_path = {
            let mut p = db_copy.clone().into_os_string();
            p.push(".lock");
            std::path::PathBuf::from(p)
        };
        if let Some(parent) = db_copy.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Io(format!(
                    "database {} is already open (locked by another Server/process)",
                    db_copy.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }
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
        if !report.uncommitted_leaf_keys.is_empty() {
            for tree in catalog.all_table_trees() {
                for (tid, uk) in &report.uncommitted_leaf_keys {
                    let mut dummy_lsn = 0;
                    let _ = tree.delete_logged(*tid, &mut dummy_lsn, uk);
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
                sessions: RwLock::new(HashMap::new()),
                backup_lock: Mutex::new(()),
                auto_ckpt_wal_bytes: AtomicU64::new(DEFAULT_AUTO_CHECKPOINT_WAL_BYTES),
                auto_ckpt_tick: AtomicU64::new(0),
                auto_ckpt_floor: AtomicU64::new(0),
                auto_ckpt_running: std::sync::atomic::AtomicBool::new(false),
                max_blob_bytes: AtomicU64::new(mitrdb_storage::DEFAULT_MAX_BLOB_BYTES),
                _dir_lock: Some(lock_file),
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
            .write()
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
            .read()
            .unwrap()
            .get(&session_id)
            .cloned()
            .ok_or_else(|| Error::TransactionError(format!("Unknown session {}", session_id)))?;
        *session.isolation.lock().unwrap() = isolation;
        Ok(())
    }

    fn session_or_err(&self, session_id: SessionId) -> Result<Arc<DatabaseSession>> {
        self.sessions
            .read()
            .unwrap()
            .get(&session_id)
            .cloned()
            .ok_or_else(|| Error::TransactionError(format!("Unknown session {}", session_id)))
    }
    fn ensure_session_txn(&self, session: &DatabaseSession) -> Result<TxnId> {
        let mut slot = session.active_txn.lock().unwrap();
        if let Some(t) = *slot {
            return Ok(t);
        }
        let iso = *session.isolation.lock().unwrap();
        let t = self.txn_mgr.begin_with_isolation(iso)?;
        *slot = Some(t);
        Ok(t)
    }
    pub fn begin_blob_write(&self, session_id: SessionId, max_len: u64) -> Result<()> {
        let sess = self.session_or_err(session_id)?;
        let txn = self.ensure_session_txn(&sess)?;
        let w = self.txn_mgr.begin_blob_write(txn, max_len)?;
        *sess.blob_writer.lock().unwrap() = Some(w);
        Ok(())
    }
    pub fn max_blob_bytes(&self) -> u64 {
        self.max_blob_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn set_max_blob_bytes(&self, n: u64) {
        self.max_blob_bytes
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }
    /// Begin a streaming BLOB write capped at the server configured max (default 128 MiB).
    pub fn begin_blob_write_default(&self, session_id: SessionId) -> Result<()> {
        self.begin_blob_write(session_id, self.max_blob_bytes())
    }
    pub fn write_blob_chunk(&self, session_id: SessionId, chunk: &[u8]) -> Result<()> {
        use std::io::Write;
        let sess = self.session_or_err(session_id)?;
        let mut g = sess.blob_writer.lock().unwrap();
        let w = g.as_mut().ok_or_else(|| {
            Error::TransactionError("no blob write open in this session".to_string())
        })?;
        w.write_all(chunk).map_err(|e| {
            if e.kind() == std::io::ErrorKind::StorageFull
                || e.to_string().contains("blob exceeds configured max length")
            {
                Error::OutOfSpace
            } else {
                Error::Io(e.to_string())
            }
        })?;
        Ok(())
    }
    pub fn finish_blob_write(&self, session_id: SessionId) -> Result<OverflowDescriptorV2> {
        let sess = self.session_or_err(session_id)?;
        let mut g = sess.blob_writer.lock().unwrap();
        let w = g.take().ok_or_else(|| {
            Error::TransactionError("no blob write open in this session".to_string())
        })?;
        let txn = sess.active_txn.lock().unwrap().ok_or_else(|| {
            Error::TransactionError("session has no active transaction".to_string())
        })?;
        self.txn_mgr.finish_blob_write(txn, w)
    }
    /// Abort an in-progress BLOB upload: drop writer + abort its txn, keep session alive.
    /// Used when a chunk exceeds max_blob_bytes or any chunk write fails, so FINISH
    /// cannot return a truncated blob.
    pub fn abort_blob_write(&self, session_id: SessionId) -> Result<()> {
        let sess = self.session_or_err(session_id)?;
        let _ = sess.blob_writer.lock().unwrap().take();
        let mut slot = sess.active_txn.lock().unwrap();
        if let Some(tid) = slot.take() {
            let abort_res = self.txn_mgr.abort(tid);
            let sync_res = self.catalog.sync_roots();
            GLOBAL_METRICS.record_txn_abort();
            abort_res?;
            sync_res?;
        }
        Ok(())
    }
    pub fn open_blob_read(&self, session_id: SessionId, desc: OverflowDescriptorV2) -> Result<()> {
        let sess = self.session_or_err(session_id)?;
        let bpm = self
            .bpm
            .as_ref()
            .ok_or_else(|| Error::Io("server has no buffer pool".to_string()))?;
        let r = BlobReader::open(bpm.clone(), desc)?;
        *sess.blob_reader.lock().unwrap() = Some(r);
        Ok(())
    }
    pub fn read_blob_range(
        &self,
        session_id: SessionId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let sess = self.session_or_err(session_id)?;
        let mut g = sess.blob_reader.lock().unwrap();
        let r = g.as_mut().ok_or_else(|| {
            Error::TransactionError("no blob read open in this session".to_string())
        })?;
        r.read_range(offset, len)
    }
    pub fn close_session(&self, session_id: SessionId) -> Result<()> {
        let session_opt = self.sessions.write().unwrap().remove(&session_id);
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
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan(sql)?;
        self.guard_raw_dml(&plan)?;
        self.execute_plan_internal(plan)
    }

    pub(crate) fn execute_sql_internal(&self, sql: &str) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan(sql)?;
        self.run_default_plan(plan)
    }

    /// SQL with `$1, $2, ...` placeholders; values are bound, never spliced into the text.
    pub fn execute_sql_params(
        &self,
        sql: &str,
        params: &[mitrdb_query::Value],
    ) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan_params(sql, params)?;
        self.guard_raw_dml(&plan)?;
        self.execute_plan_internal(plan)
    }

    pub(crate) fn execute_sql_params_internal(
        &self,
        sql: &str,
        params: &[mitrdb_query::Value],
    ) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan_params(sql, params)?;
        self.run_default_plan(plan)
    }

    pub fn execute_in_session(&self, session_id: SessionId, sql: &str) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan(sql)?;
        self.guard_raw_dml(&plan)?;
        self.execute_plan_in_session_internal(session_id, plan)
    }

    pub fn execute_sql_params_in_session(
        &self,
        session_id: SessionId,
        sql: &str,
        params: &[mitrdb_query::Value],
    ) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan_params(sql, params)?;
        self.guard_raw_dml(&plan)?;
        self.execute_plan_in_session_internal(session_id, plan)
    }

    pub(crate) fn execute_sql_params_in_session_internal(
        &self,
        session_id: SessionId,
        sql: &str,
        params: &[mitrdb_query::Value],
    ) -> Result<ExecutionResult> {
        GLOBAL_METRICS.record_query();
        let plan = self.query_engine.parse_and_plan_params(sql, params)?;
        self.execute_plan_in_session_internal(session_id, plan)
    }

    pub(crate) fn session_has_active_txn(&self, session_id: Option<SessionId>) -> bool {
        match session_id {
            None => self.default_session.active_txn.lock().unwrap().is_some(),
            Some(sid) => self
                .sessions
                .read()
                .unwrap()
                .get(&sid)
                .is_some_and(|s| s.active_txn.lock().unwrap().is_some()),
        }
    }

    pub fn execute_plan(&self, plan: PhysicalPlan) -> Result<ExecutionResult> {
        self.guard_raw_dml(&plan)?;
        self.execute_plan_internal(plan)
    }

    pub(crate) fn execute_plan_internal(&self, plan: PhysicalPlan) -> Result<ExecutionResult> {
        self.run_default_plan(plan)
    }

    /// Default-session execution. Txn-control plans and statements inside an explicit txn keep
    /// the old path (default slot, mutex held for the whole statement). A plain autocommit
    /// statement (no txn open in the default session) runs on a local slot without the mutex,
    /// so threads using Table/ORM/execute_sql no longer serialize on it.
    fn run_default_plan(&self, plan: PhysicalPlan) -> Result<ExecutionResult> {
        let isolation = *self.default_session.isolation.lock().unwrap();
        let control = matches!(
            plan,
            PhysicalPlan::Begin
                | PhysicalPlan::Commit
                | PhysicalPlan::Rollback
                | PhysicalPlan::Savepoint(_)
                | PhysicalPlan::RollbackTo(_)
                | PhysicalPlan::ReleaseSavepoint(_)
                | PhysicalPlan::Checkpoint
        );
        let forced = FORCE_AUTOCOMMIT.with(|c| c.get());
        let autocommit = !control
            && (forced
                || match self.default_session.active_txn.try_lock() {
                    Ok(g) => g.is_none(),
                    Err(_) => false,
                });
        if autocommit {
            let mut local: Option<TxnId> = None;
            let r = self.execute_plan_with_txn_slot(plan, &mut local, isolation);
            if let Some(t) = local.take() {
                let _ = self.txn_mgr.abort(t);
            }
            return r;
        }
        let mut active_txn = self.default_session.active_txn.lock().unwrap();
        self.execute_plan_with_txn_slot(plan, &mut active_txn, isolation)
    }

    pub fn execute_plan_in_session(
        &self,
        session_id: SessionId,
        plan: PhysicalPlan,
    ) -> Result<ExecutionResult> {
        self.guard_raw_dml(&plan)?;
        self.execute_plan_in_session_internal(session_id, plan)
    }

    pub(crate) fn execute_plan_in_session_internal(
        &self,
        session_id: SessionId,
        plan: PhysicalPlan,
    ) -> Result<ExecutionResult> {
        let session = self
            .sessions
            .read()
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

    #[allow(dead_code)]
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

impl Drop for Server {
    fn drop(&mut self) {
        if self.txn_mgr.active_txn_count() == 0 {
            let _ = self.catalog.sync_roots();
        }
        if let Some(ref wal) = self.wal_mgr {
            let _ = wal.flush_all();
        }
        if let Some(ref bpm) = self.bpm {
            let _ = bpm.flush_all();
        }
        if let Some(ref wal) = self.wal_mgr {
            let _ = wal.flush_all();
        }
    }
}

mod files;
mod perms;
pub use files::{FileStore, StoredFile};
mod table;
pub use table::{Delete, IntoValue, Query, Rows, Table, Update, val};

mod schema_builder;
pub use schema_builder::{ColumnBuilder, TableBuilder};

thread_local! {
    static FORCE_AUTOCOMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// While alive, default-session statements on this thread run as their own autocommit
/// transaction even if an explicit txn is open (used for the auto-id counter, so a
/// ROLLBACK of the outer txn cannot hand the same id out again).
pub(crate) struct ForceAutocommit(bool);

impl ForceAutocommit {
    pub(crate) fn new() -> Self {
        ForceAutocommit(FORCE_AUTOCOMMIT.with(|c| c.replace(true)))
    }
}

impl Drop for ForceAutocommit {
    fn drop(&mut self) {
        FORCE_AUTOCOMMIT.with(|c| c.set(self.0));
    }
}
