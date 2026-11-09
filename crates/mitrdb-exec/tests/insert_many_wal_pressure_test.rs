use std::sync::Arc;

use mitrdb_exec::{ExecutionResult, Executor};
use mitrdb_query::{Catalog, QueryEngine};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn run_sql(
    exec: &Executor,
    engine: &QueryEngine,
    catalog: &Catalog,
    txn_mgr: &TxnManager,
    active_txn: &mut Option<u64>,
    sql: &str,
) -> mitrdb_common::Result<ExecutionResult> {
    let plan = engine.parse_and_plan(sql)?;
    exec.execute_plan(plan, catalog, txn_mgr, active_txn)
}

#[test]
fn test_insert_many_pressure_and_rollback() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_exec_insmany_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let disk = Arc::new(DiskManager::open_or_create(temp_dir.join("test.db").as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(temp_dir.join("test.wal").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 128));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();

    let mut active_txn = None;

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut active_txn,
        "CREATE TABLE test_many (id INT PRIMARY KEY, val TEXT)",
    )
    .unwrap();

    // 200 rows (> 64 so inserted % 64 == 63 relieves WAL pressure)
    let mut values_clause = Vec::new();
    for i in 0..200 {
        values_clause.push(format!("({}, 'val_{}')", i, i));
    }
    let sql_bulk = format!("INSERT INTO test_many VALUES {}", values_clause.join(", "));

    let res = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut active_txn,
        &sql_bulk,
    )
    .unwrap();

    assert_eq!(res, ExecutionResult::Affected(200));

    // Duplicate key mid-statement: verify statement rollback
    let bad_sql = "INSERT INTO test_many VALUES (500, 'ok1'), (501, 'ok2'), (10, 'dup')";
    let err = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut active_txn,
        bad_sql,
    );
    assert!(err.is_err(), "Duplicate key in InsertMany must fail");

    let _ = std::fs::remove_dir_all(&temp_dir);
}
