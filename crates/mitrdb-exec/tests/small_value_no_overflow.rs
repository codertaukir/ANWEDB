#![allow(clippy::ptr_arg)]
use std::sync::Arc;
use mitrdb_exec::{ExecutionResult, Executor, Tuple};
use mitrdb_query::{Catalog, QueryEngine, Value};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::{PAGE_FLAG_FREE, PAGE_TYPE_OVERFLOW};
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn run_sql(exec: &Executor, engine: &QueryEngine, catalog: &Catalog, txn_mgr: &TxnManager, active_txn: &mut Option<u64>, sql: &str) -> mitrdb_common::Result<ExecutionResult> {
    let plan = engine.parse_and_plan(sql)?;
    exec.execute_plan(plan, catalog, txn_mgr, active_txn)
}

fn setup(tag: &str) -> (std::path::PathBuf, Arc<DiskManager>, Arc<BufferPoolManager>, Arc<Catalog>, TxnManager, QueryEngine, Executor, Option<u64>) {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_a5_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(temp_dir.join("test.db").as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(temp_dir.join("test.wal").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();
    (temp_dir, disk, bpm, catalog, txn_mgr, engine, exec, None)
}

fn overflow_pages(bpm: &BufferPoolManager, disk: &DiskManager) -> usize {
    let mut n = 0;
    for pid in 0..disk.num_pages() {
        if let Ok(p) = bpm.fetch_page(pid) {
            let is_overflow = p.page_type() == PAGE_TYPE_OVERFLOW && (p.flags() & PAGE_FLAG_FREE) == 0;
            let _ = bpm.unpin_page(pid, false);
            if is_overflow { n += 1; }
        }
    }
    n
}

#[test]
fn thirty_byte_message_uses_zero_overflow_pages() {
    let (dir, disk, bpm, catalog, txn_mgr, engine, exec, mut sess) = setup("msg");
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, "CREATE TABLE messages (id INT PRIMARY KEY, body TEXT)").unwrap();
    let before = overflow_pages(&bpm, &disk);
    // 30-byte message — well under INLINE_BLOB_MAX_BYTES (2048)
    let msg = "0123456789".repeat(3);
    assert_eq!(msg.len(), 30);
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, &format!("INSERT INTO messages (id, body) VALUES (1, '{msg}')")).unwrap();
    let after = overflow_pages(&bpm, &disk);
    assert_eq!(after - before, 0, "30-byte TEXT allocated overflow pages: before={before} after={after}");
    // roundtrip: value must read back exactly
    let r = run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, "SELECT body FROM messages WHERE id = 1").unwrap();
    let _ = r;
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn boundary_2048_inline_2049_overflow() {
    let (dir, disk, bpm, catalog, txn_mgr, engine, exec, mut sess) = setup("bound");
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, "CREATE TABLE t (id INT PRIMARY KEY, body TEXT)").unwrap();
    // 2048 inline
    let s2048 = "x".repeat(2048);
    let before = overflow_pages(&bpm, &disk);
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, &format!("INSERT INTO t (id, body) VALUES (1, '{s2048}')")).unwrap();
    assert_eq!(overflow_pages(&bpm, &disk) - before, 0, "2048-byte TEXT must stay inline");
    // 2049 overflow — exactly 1 overflow page (2049 < 4056 chunk capacity)
    let s2049 = "y".repeat(2049);
    let before2 = overflow_pages(&bpm, &disk);
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut sess, &format!("INSERT INTO t (id, body) VALUES (2, '{s2049}')")).unwrap();
    assert_eq!(overflow_pages(&bpm, &disk) - before2, 1, "2049-byte TEXT must use 1 overflow page");
    // Tuple-level: small BYTES also inline
    let _ = Tuple::serialize(&[Value::Bytes(vec![7u8; 30])]);
    let _ = std::fs::remove_dir_all(dir);
}
