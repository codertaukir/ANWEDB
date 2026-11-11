#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::mem::size_of;
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::{IsolationLevel, TxnManager, UndoAction};

#[test]
fn test_bulk_dml_transient_memory_profiling() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_dml_prof_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let disk = Arc::new(DiskManager::open_or_create(temp_dir.join("test.db").as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(temp_dir.join("test.wal").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 512));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    let tid = txn_mgr
        .begin_with_isolation(IsolationLevel::ReadCommitted)
        .unwrap();
    let num_rows: usize = 5000;

    let mut total_key_bytes = 0usize;
    let mut total_val_bytes = 0usize;

    for i in 0..num_rows {
        let key = format!("user_account_key_{:08}", i).into_bytes();
        let val = format!("user_profile_payload_data_column_field_{:016}", i).into_bytes();

        total_key_bytes += key.len();
        total_val_bytes += val.len();

        // Performs point locking, MVCC version recording, and write_set logging
        txn_mgr.put(tid, &tree, &key, &val).unwrap();
    }

    // 1. write_set: UndoAction::BTreeKey heap footprint
    let undo_action_stack = size_of::<UndoAction>();
    let write_set_heap = (num_rows * undo_action_stack) + total_key_bytes + total_val_bytes;

    // 2. LockManager: locks (HashMap entry) + point_by_tree (BTreeMap node) + by_txn (HashSet entry)
    let lock_point_overhead = num_rows * (32 + 24 + 16 + 64);

    // 3. MVCC Version Store: versions (HashMap entry) + VersionedValue + keys_by_tree (BTreeSet node)
    let mvcc_heap = num_rows
        * (32 + 24 + (total_key_bytes / num_rows) + 48 + (total_val_bytes / num_rows) + 64);

    let total_transient_mem = write_set_heap + lock_point_overhead + mvcc_heap;
    let per_row_bytes = total_transient_mem / num_rows;

    println!("\n=======================================================");
    println!("        BULK DML TRANSIENT MEMORY BREAKDOWN           ");
    println!("=======================================================");
    println!(" Rows Simulated           : {}", num_rows);
    println!(
        " Raw Payload Data Size    : {:.2} KB",
        (total_key_bytes + total_val_bytes) as f64 / 1024.0
    );
    println!("-------------------------------------------------------");
    println!(
        " 1. write_set (UndoAction): {} KB (~{} B/row)",
        write_set_heap / 1024,
        write_set_heap / num_rows
    );
    println!(
        " 2. LockManager           : {} KB (~{} B/row)",
        lock_point_overhead / 1024,
        lock_point_overhead / num_rows
    );
    println!(
        " 3. MvccVersionStore      : {} KB (~{} B/row)",
        mvcc_heap / 1024,
        mvcc_heap / num_rows
    );
    println!("-------------------------------------------------------");
    println!(
        " Total Transient Heap     : {} KB",
        total_transient_mem / 1024
    );
    println!(" Average Memory / Row     : {} Bytes", per_row_bytes);
    println!("=======================================================\n");

    assert!(
        per_row_bytes < 1024,
        "Transient memory exceeded 1KB per row budget"
    );

    txn_mgr.commit(tid).unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}
