use std::sync::Arc;

use mitrdb_common::Error;
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_TYPE_RAW;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::{IsolationLevel, LockKey, LockMode, TxnManager, TxnState};

#[test]
fn test_mvcc_snapshot_visibility_and_concurrent_uncommitted_delete() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_group_b_mvcc_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    let t1 = txn_mgr.begin().unwrap();
    txn_mgr.put(t1, &tree, b"row1", b"val1").unwrap();
    txn_mgr.put(t1, &tree, b"row2", b"val2").unwrap();
    txn_mgr.commit(t1).unwrap();

    let t_deleter = txn_mgr.begin().unwrap();
    let t_reader_si = txn_mgr
        .begin_with_isolation(IsolationLevel::SnapshotIsolation)
        .unwrap();
    let t_reader_rc = txn_mgr
        .begin_with_isolation(IsolationLevel::ReadCommitted)
        .unwrap();

    assert!(txn_mgr.delete(t_deleter, &tree, b"row1").unwrap());
    txn_mgr
        .put(t_deleter, &tree, b"row3", b"uncommitted_val3")
        .unwrap();

    assert_eq!(txn_mgr.get(t_deleter, &tree, b"row1").unwrap(), None);
    assert_eq!(
        txn_mgr.get(t_deleter, &tree, b"row3").unwrap(),
        Some(b"uncommitted_val3".to_vec())
    );

    let si_scan = txn_mgr.scan(t_reader_si, &tree).unwrap();
    assert_eq!(
        si_scan,
        vec![
            (b"row1".to_vec(), b"val1".to_vec()),
            (b"row2".to_vec(), b"val2".to_vec()),
        ]
    );
    let rc_scan = txn_mgr.scan(t_reader_rc, &tree).unwrap();
    assert_eq!(
        rc_scan,
        vec![
            (b"row1".to_vec(), b"val1".to_vec()),
            (b"row2".to_vec(), b"val2".to_vec()),
        ]
    );

    txn_mgr.commit(t_deleter).unwrap();

    let si_scan_after_commit = txn_mgr.scan(t_reader_si, &tree).unwrap();
    assert_eq!(
        si_scan_after_commit,
        vec![
            (b"row1".to_vec(), b"val1".to_vec()),
            (b"row2".to_vec(), b"val2".to_vec()),
        ]
    );

    let rc_scan_after_commit = txn_mgr.scan(t_reader_rc, &tree).unwrap();
    assert_eq!(
        rc_scan_after_commit,
        vec![
            (b"row2".to_vec(), b"val2".to_vec()),
            (b"row3".to_vec(), b"uncommitted_val3".to_vec()),
        ]
    );

    txn_mgr.commit(t_reader_si).unwrap();
    txn_mgr.commit(t_reader_rc).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_synthetic_baseline_versions_for_pre_existing_disk_keys() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_b_baseline_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());

    tree.insert(b"disk_k1", b"disk_v1").unwrap();
    tree.insert(b"disk_k2", b"disk_v2").unwrap();

    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    let t_writer = txn_mgr.begin().unwrap();
    let t_reader = txn_mgr.begin().unwrap();

    txn_mgr
        .put(t_writer, &tree, b"disk_k1", b"temp_v1_a")
        .unwrap();
    txn_mgr
        .put(t_writer, &tree, b"disk_k1", b"temp_v1_b")
        .unwrap();
    assert!(txn_mgr.delete(t_writer, &tree, b"disk_k2").unwrap());

    assert_eq!(
        txn_mgr.get(t_writer, &tree, b"disk_k1").unwrap(),
        Some(b"temp_v1_b".to_vec())
    );
    assert_eq!(txn_mgr.get(t_writer, &tree, b"disk_k2").unwrap(), None);

    assert_eq!(
        txn_mgr.get(t_reader, &tree, b"disk_k1").unwrap(),
        Some(b"disk_v1".to_vec())
    );
    assert_eq!(
        txn_mgr.get(t_reader, &tree, b"disk_k2").unwrap(),
        Some(b"disk_v2".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_reader, &tree).unwrap(),
        vec![
            (b"disk_k1".to_vec(), b"disk_v1".to_vec()),
            (b"disk_k2".to_vec(), b"disk_v2".to_vec()),
        ]
    );

    txn_mgr.abort(t_writer).unwrap();
    assert_eq!(txn_mgr.txn_state(t_writer), Some(TxnState::Aborted));

    assert_eq!(
        txn_mgr.get(t_reader, &tree, b"disk_k1").unwrap(),
        Some(b"disk_v1".to_vec())
    );
    assert_eq!(
        txn_mgr.get(t_reader, &tree, b"disk_k2").unwrap(),
        Some(b"disk_v2".to_vec())
    );
    txn_mgr.commit(t_reader).unwrap();
    assert!(!txn_mgr.has_active_tree_references(&tree));

    let t_writer2 = txn_mgr.begin().unwrap();
    let t_old_snap = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t_writer2, &tree, b"disk_k1", b"committed_v1_new")
        .unwrap();
    txn_mgr.commit(t_writer2).unwrap();

    assert_eq!(
        txn_mgr.get(t_old_snap, &tree, b"disk_k1").unwrap(),
        Some(b"disk_v1".to_vec())
    );
    txn_mgr.commit(t_old_snap).unwrap();

    let t_new_snap = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_new_snap, &tree, b"disk_k1").unwrap(),
        Some(b"committed_v1_new".to_vec())
    );
    txn_mgr.commit(t_new_snap).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_tree_scoped_key_isolation_and_monotonic_tree_ids() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_b_treescope_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));

    let tree_a = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let tree_b = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let tree_a_reopened = Arc::new(
        BPlusTree::open(bpm.clone(), Some(wal.clone()), tree_a.root_page_id(), 4).unwrap(),
    );

    assert!(tree_a.tree_id() < tree_b.tree_id());
    assert!(tree_b.tree_id() < tree_a_reopened.tree_id());

    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let t1 = txn_mgr.begin().unwrap();
    let t2 = txn_mgr.begin().unwrap();

    txn_mgr
        .put(t1, &tree_a, b"shared_pk", b"table_a_val")
        .unwrap();
    txn_mgr
        .put(t2, &tree_b, b"shared_pk", b"table_b_val")
        .unwrap();

    assert_eq!(
        txn_mgr.get(t1, &tree_a, b"shared_pk").unwrap(),
        Some(b"table_a_val".to_vec())
    );
    assert_eq!(txn_mgr.get(t1, &tree_b, b"shared_pk").unwrap(), None);
    assert_eq!(
        txn_mgr.get(t2, &tree_b, b"shared_pk").unwrap(),
        Some(b"table_b_val".to_vec())
    );
    assert_eq!(txn_mgr.get(t2, &tree_a, b"shared_pk").unwrap(), None);

    txn_mgr.commit(t1).unwrap();
    txn_mgr.commit(t2).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_lock_conflicts_upgrade_deadlock_and_page_slice_undo() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_b_locks_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let (raw_pid, mut raw_page) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
    raw_page.data[64..72].copy_from_slice(b"origdata");
    disk.write_page(raw_pid, &raw_page).unwrap();

    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    let t_up = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert_eq!(txn_mgr.get(t_up, &tree, b"k_up").unwrap(), None);
    txn_mgr.put(t_up, &tree, b"k_up", b"v_upgraded").unwrap();
    txn_mgr.commit(t_up).unwrap();

    let lm = txn_mgr.lock_manager();
    lm.acquire(101, LockKey::Table(1), LockMode::Shared)
        .unwrap();
    lm.acquire(102, LockKey::Table(1), LockMode::Shared)
        .unwrap();
    let upgrade_conflict = lm.acquire(101, LockKey::Table(1), LockMode::Exclusive);
    assert!(matches!(upgrade_conflict, Err(Error::LockTimeout)));

    lm.acquire(201, LockKey::Record(1, 1), LockMode::Exclusive)
        .unwrap();
    lm.acquire(202, LockKey::Record(1, 2), LockMode::Exclusive)
        .unwrap();
    assert!(matches!(
        lm.acquire(201, LockKey::Record(1, 2), LockMode::Exclusive),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        lm.acquire(202, LockKey::Record(1, 1), LockMode::Exclusive),
        Err(Error::LockTimeout)
    ));
    lm.release_all(101);
    lm.release_all(102);
    lm.release_all(201);
    lm.release_all(202);

    let t_fcw1 = txn_mgr.begin().unwrap();
    let t_fcw2 = txn_mgr.begin().unwrap();
    txn_mgr.put(t_fcw1, &tree, b"k_fcw", b"v1").unwrap();
    txn_mgr.commit(t_fcw1).unwrap();
    let fcw_err = txn_mgr.put(t_fcw2, &tree, b"k_fcw", b"v2").unwrap_err();
    assert!(matches!(fcw_err, Error::WriteConflict(_)));
    txn_mgr.abort(t_fcw2).unwrap();

    let t_slice = txn_mgr.begin().unwrap();
    txn_mgr
        .update_page(t_slice, raw_pid, 64, b"newdata!")
        .unwrap();
    let modified = bpm.fetch_page(raw_pid).unwrap();
    assert_eq!(&modified.data[64..72], b"newdata!");
    bpm.unpin_page(raw_pid, false).unwrap();

    txn_mgr.abort(t_slice).unwrap();
    let restored = bpm.fetch_page(raw_pid).unwrap();
    assert_eq!(&restored.data[64..72], b"origdata");
    bpm.unpin_page(raw_pid, false).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_active_tree_references_and_purge_tree_state() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_b_purge_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let tree1 = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let tree2 = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    let t_read = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let _ = txn_mgr.get(t_read, &tree1, b"k1").unwrap();
    assert!(txn_mgr.has_active_tree_references(&tree1));
    assert!(!txn_mgr.has_active_tree_references(&tree2));
    assert!(matches!(
        txn_mgr.purge_tree_state(&tree1),
        Err(Error::WriteConflict(_))
    ));
    txn_mgr.abort(t_read).unwrap();
    assert!(!txn_mgr.has_active_tree_references(&tree1));

    let t_write = txn_mgr.begin().unwrap();
    let t_other = txn_mgr.begin().unwrap();
    txn_mgr.put(t_write, &tree1, b"k1", b"v1").unwrap();
    txn_mgr.put(t_other, &tree2, b"k2", b"v2").unwrap();
    assert!(txn_mgr.has_active_tree_references(&tree1));
    assert!(matches!(
        txn_mgr.purge_tree_state(&tree1),
        Err(Error::WriteConflict(_))
    ));

    txn_mgr.commit(t_write).unwrap();
    assert!(!txn_mgr.has_active_tree_references(&tree1));
    assert!(txn_mgr.purge_tree_state(&tree1).is_ok());

    assert_eq!(
        txn_mgr.get(t_other, &tree2, b"k2").unwrap(),
        Some(b"v2".to_vec())
    );
    txn_mgr.commit(t_other).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}
