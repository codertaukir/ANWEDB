use std::sync::{Arc, Barrier};
use std::thread;

use mitrdb_common::Error;
use mitrdb_observe::GLOBAL_METRICS;
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_TYPE_RAW;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::{IsolationLevel, TxnManager, TxnState};

#[test]
fn test_vacuum_long_running_snapshot_horizon_and_partial_pruning() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_vacuum_horizon_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    // T1: insert v1
    let t1 = txn_mgr.begin().unwrap();
    txn_mgr.put(t1, &tree, b"acct:1", b"v1").unwrap();
    txn_mgr.commit(t1).unwrap();

    // T2: update to v2
    let t2 = txn_mgr.begin().unwrap();
    txn_mgr.put(t2, &tree, b"acct:1", b"v2").unwrap();
    txn_mgr.commit(t2).unwrap();

    // T3: update to v3
    let t3 = txn_mgr.begin().unwrap();
    txn_mgr.put(t3, &tree, b"acct:1", b"v3").unwrap();
    let t3_commit_ts = txn_mgr.commit(t3).unwrap();

    // Start a long-running snapshot reader right after T3 commits (sees v3)
    let t_snap = txn_mgr
        .begin_with_isolation(IsolationLevel::SnapshotIsolation)
        .unwrap();
    assert_eq!(txn_mgr.oldest_active_snapshot_ts(), t3_commit_ts);

    // T4: update to v4 after t_snap began
    let t4 = txn_mgr.begin().unwrap();
    txn_mgr.put(t4, &tree, b"acct:1", b"v4").unwrap();
    txn_mgr.commit(t4).unwrap();

    // T5: update to v5 after t_snap began
    let t5 = txn_mgr.begin().unwrap();
    txn_mgr.put(t5, &tree, b"acct:1", b"v5").unwrap();
    txn_mgr.commit(t5).unwrap();

    assert_eq!(txn_mgr.mvcc_key_count(), 1);
    assert_eq!(txn_mgr.mvcc_version_count(), 5);
    assert_eq!(txn_mgr.tracked_txn_count(), 6); // t1..t5 + t_snap

    // Run vacuum while t_snap is still active:
    // Versions v1 (xmax=t2 <= horizon) and v2 (xmax=t3 <= horizon) must be pruned,
    // while v3 (needed by t_snap), v4, and v5 must be preserved.
    // Transactions t1 and t2 are no longer referenced by any version and must be reclaimed.
    let rep1 = txn_mgr.vacuum();
    assert_eq!(rep1.horizon_ts, t3_commit_ts);
    assert_eq!(rep1.versions_pruned, 2);
    assert_eq!(rep1.keys_cleared, 0);
    assert_eq!(rep1.txns_reclaimed, 2);

    assert_eq!(txn_mgr.mvcc_key_count(), 1);
    assert_eq!(txn_mgr.mvcc_version_count(), 3); // v3, v4, v5
    assert_eq!(txn_mgr.tracked_txn_count(), 4); // t3, t4, t5, t_snap

    // Verify t_snap still sees v3 via both get and scan
    assert_eq!(
        txn_mgr.get(t_snap, &tree, b"acct:1").unwrap(),
        Some(b"v3".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_snap, &tree).unwrap(),
        vec![(b"acct:1".to_vec(), b"v3".to_vec())]
    );

    // Verify a fresh reader sees v5
    let t_fresh = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_fresh, &tree, b"acct:1").unwrap(),
        Some(b"v5".to_vec())
    );
    txn_mgr.commit(t_fresh).unwrap();

    // Finish t_snap and run vacuum again:
    // All remaining versions (v3, v4, and v5) can now be collapsed into the B+Tree state ("v5"),
    // clearing the key chain and reclaiming all completed transaction contexts.
    txn_mgr.commit(t_snap).unwrap();

    let rep2 = txn_mgr.vacuum();
    assert_eq!(rep2.versions_pruned, 3);
    assert_eq!(rep2.keys_cleared, 1);
    assert_eq!(rep2.txns_reclaimed, 5); // t3, t4, t5, t_fresh, t_snap

    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    // Verify subsequent reads/scans read "v5" cleanly from the B+Tree
    let t_after = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_after, &tree, b"acct:1").unwrap(),
        Some(b"v5".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_after, &tree).unwrap(),
        vec![(b"acct:1".to_vec(), b"v5".to_vec())]
    );
    txn_mgr.commit(t_after).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_vacuum_tombstone_pruning_never_resurrects_deleted_btree_records() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_vacuum_tombstone_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());

    // Pre-existing baseline record on disk before TxnManager starts
    tree.insert(b"k_disk", b"disk_val").unwrap();

    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    // Transactionally inserted record
    let t_ins = txn_mgr.begin().unwrap();
    txn_mgr.put(t_ins, &tree, b"k_txn", b"txn_val_1").unwrap();
    txn_mgr.put(t_ins, &tree, b"k_keep", b"keep_val").unwrap();
    txn_mgr.commit(t_ins).unwrap();

    let t_upd = txn_mgr.begin().unwrap();
    txn_mgr.put(t_upd, &tree, b"k_txn", b"txn_val_2").unwrap();
    txn_mgr.commit(t_upd).unwrap();

    // Snapshot reader started before deletions
    let t_pre_delete = txn_mgr.begin().unwrap();

    // Delete both k_disk and k_txn
    let t_del = txn_mgr.begin().unwrap();
    assert!(txn_mgr.delete(t_del, &tree, b"k_disk").unwrap());
    assert!(txn_mgr.delete(t_del, &tree, b"k_txn").unwrap());
    txn_mgr.commit(t_del).unwrap();

    // Vacuum while t_pre_delete is active:
    // - k_txn's first version ("txn_val_1", overwritten by t_upd <= horizon) is pruned,
    //   but "txn_val_2" and its tombstone must remain.
    // - k_disk's synthetic baseline version ("disk_val") and its tombstone must remain.
    let rep_active = txn_mgr.vacuum();
    assert_eq!(rep_active.versions_pruned, 2); // k_txn v1 pruned + k_keep single version collapsed
    assert_eq!(rep_active.keys_cleared, 1); // k_keep collapsed into B+Tree

    // t_pre_delete still sees all 3 keys
    assert_eq!(
        txn_mgr.scan(t_pre_delete, &tree).unwrap(),
        vec![
            (b"k_disk".to_vec(), b"disk_val".to_vec()),
            (b"k_keep".to_vec(), b"keep_val".to_vec()),
            (b"k_txn".to_vec(), b"txn_val_2".to_vec()),
        ]
    );

    // Post-delete reader sees only k_keep
    let t_post = txn_mgr.begin().unwrap();
    assert_eq!(txn_mgr.get(t_post, &tree, b"k_disk").unwrap(), None);
    assert_eq!(txn_mgr.get(t_post, &tree, b"k_txn").unwrap(), None);
    assert_eq!(
        txn_mgr.scan(t_post, &tree).unwrap(),
        vec![(b"k_keep".to_vec(), b"keep_val".to_vec())]
    );
    txn_mgr.commit(t_post).unwrap();

    // Now finish t_pre_delete and run vacuum:
    // Both tombstones and their pre-delete versions must be completely pruned and cleared.
    txn_mgr.commit(t_pre_delete).unwrap();
    let rep_final = txn_mgr.vacuum();
    assert_eq!(rep_final.keys_cleared, 2); // k_disk and k_txn chains cleared
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    // Verify deleted keys are NOT resurrected after tombstone chains are removed
    let t_verify = txn_mgr.begin().unwrap();
    assert_eq!(txn_mgr.get(t_verify, &tree, b"k_disk").unwrap(), None);
    assert_eq!(txn_mgr.get(t_verify, &tree, b"k_txn").unwrap(), None);
    assert_eq!(
        txn_mgr.get(t_verify, &tree, b"k_keep").unwrap(),
        Some(b"keep_val".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_verify, &tree).unwrap(),
        vec![(b"k_keep".to_vec(), b"keep_val".to_vec())]
    );
    txn_mgr.commit(t_verify).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_vacuum_preserves_uncommitted_writers_and_handles_aborted_txns() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_vacuum_abort_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    // T1 commits initial value
    let t1 = txn_mgr.begin().unwrap();
    txn_mgr.put(t1, &tree, b"key1", b"committed_v1").unwrap();
    txn_mgr.commit(t1).unwrap();

    // T2 commits second value
    let t2 = txn_mgr.begin().unwrap();
    txn_mgr.put(t2, &tree, b"key1", b"committed_v2").unwrap();
    txn_mgr.commit(t2).unwrap();

    // T3 begins and overwrites key1 + inserts key2, but does NOT commit yet
    let t3_uncommitted = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t3_uncommitted, &tree, b"key1", b"uncommitted_v3")
        .unwrap();
    txn_mgr
        .put(t3_uncommitted, &tree, b"key2", b"uncommitted_k2")
        .unwrap();

    // Run vacuum while T3 is active:
    // - key1's v1 (overwritten by committed T2 <= T3.read_ts) CAN be pruned, and T1 reclaimed.
    // - key1's v2 (overwritten by uncommitted T3) and T2's context MUST be preserved!
    // - T3's uncommitted versions on key1 and key2 MUST be preserved!
    let rep_during_uncommitted = txn_mgr.vacuum();
    assert_eq!(rep_during_uncommitted.versions_pruned, 1); // v1 pruned
    assert_eq!(rep_during_uncommitted.keys_cleared, 0);
    assert_eq!(rep_during_uncommitted.txns_reclaimed, 1); // t1 reclaimed
    assert_eq!(
        txn_mgr.txn_state(t2),
        Some(TxnState::Committed { commit_ts: 3 })
    );
    assert_eq!(txn_mgr.txn_state(t3_uncommitted), Some(TxnState::Active));

    // Concurrent reader must still see committed_v2 on key1 and None on key2
    let t_reader = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_reader, &tree, b"key1").unwrap(),
        Some(b"committed_v2".to_vec())
    );
    assert_eq!(txn_mgr.get(t_reader, &tree, b"key2").unwrap(), None);
    txn_mgr.commit(t_reader).unwrap();

    // Now T3 aborts
    txn_mgr.abort(t3_uncommitted).unwrap();
    assert_eq!(txn_mgr.txn_state(t3_uncommitted), Some(TxnState::Aborted));

    // Run vacuum after T3 aborted:
    // key1's remaining committed_v2 version is collapsed into B+Tree, and T2, T3, t_reader are reclaimed.
    let rep_after_abort = txn_mgr.vacuum();
    assert_eq!(rep_after_abort.versions_pruned, 1); // committed_v2 collapsed
    assert_eq!(rep_after_abort.keys_cleared, 1);
    assert_eq!(rep_after_abort.txns_reclaimed, 3); // t2, t3_uncommitted, t_reader
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    // Verify key1 still has committed_v2 in B+Tree and key2 is absent
    let t_check = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_check, &tree, b"key1").unwrap(),
        Some(b"committed_v2".to_vec())
    );
    assert_eq!(txn_mgr.get(t_check, &tree, b"key2").unwrap(), None);
    txn_mgr.commit(t_check).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_vacuum_protects_new_insert_when_older_snapshot_is_active() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_v2_vacuum_new_insert_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    // Start t_old before any key exists
    let t_old = txn_mgr.begin().unwrap();

    // t_writer inserts a brand-new key and commits AFTER t_old's snapshot
    let t_writer = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t_writer, &tree, b"brand_new", b"val_new")
        .unwrap();
    txn_mgr.commit(t_writer).unwrap();

    // Run vacuum while t_old is still active:
    // Because t_writer.commit_ts > t_old.read_ts (horizon_ts), vacuum MUST NOT collapse
    // "brand_new" into the B+Tree yet; otherwise t_old would fall through to B+Tree and see it!
    let rep1 = txn_mgr.vacuum();
    assert_eq!(rep1.versions_pruned, 0);
    assert_eq!(rep1.keys_cleared, 0);
    assert_eq!(rep1.txns_reclaimed, 0);

    // t_old must still see None!
    assert_eq!(txn_mgr.get(t_old, &tree, b"brand_new").unwrap(), None);
    assert!(txn_mgr.scan(t_old, &tree).unwrap().is_empty());

    txn_mgr.commit(t_old).unwrap();

    // Now that t_old has finished, vacuum can collapse "brand_new" and reclaim both txns
    let rep2 = txn_mgr.vacuum();
    assert_eq!(rep2.versions_pruned, 1);
    assert_eq!(rep2.keys_cleared, 1);
    assert_eq!(rep2.txns_reclaimed, 2);
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_vacuum_repeated_idempotence_and_bounded_memory_growth() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_vacuum_bounded_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    let metrics_before = GLOBAL_METRICS.snapshot();

    // Perform 10 rounds of 25 transactions each (updates, deletes, aborts), vacuuming after each round
    for round in 0..10u32 {
        for i in 0..25u32 {
            let key = format!("k_{:02}", i % 10).into_bytes();
            let val = format!("v_r{}_i{}", round, i).into_bytes();
            let tid = txn_mgr.begin().unwrap();
            let _ = txn_mgr.get(tid, &tree, &key).unwrap();
            if i % 7 == 0 {
                let _ = txn_mgr.delete(tid, &tree, &key).unwrap();
                txn_mgr.commit(tid).unwrap();
            } else if i % 5 == 0 {
                txn_mgr.put(tid, &tree, &key, &val).unwrap();
                txn_mgr.abort(tid).unwrap();
            } else {
                txn_mgr.put(tid, &tree, &key, &val).unwrap();
                txn_mgr.commit(tid).unwrap();
            }
        }

        // Before vacuum, bounded by at most one round of activity
        assert!(txn_mgr.mvcc_key_count() <= 10);
        assert!(txn_mgr.tracked_txn_count() <= 25);

        let v_before = txn_mgr.mvcc_version_count();
        let k_before = txn_mgr.mvcc_key_count();
        let t_before = txn_mgr.tracked_txn_count();

        let rep = txn_mgr.vacuum();
        assert_eq!(rep.versions_pruned, v_before - txn_mgr.mvcc_version_count());
        assert_eq!(rep.keys_cleared, k_before - txn_mgr.mvcc_key_count());
        assert_eq!(rep.txns_reclaimed, t_before - txn_mgr.tracked_txn_count());

        // With no active transactions at the end of the round, memory usage drops to 0!
        assert_eq!(txn_mgr.mvcc_key_count(), 0);
        assert_eq!(txn_mgr.mvcc_version_count(), 0);
        assert_eq!(txn_mgr.tracked_txn_count(), 0);

        // Immediate repeated vacuum must be a strict no-op
        let noop_rep = txn_mgr.vacuum();
        assert_eq!(noop_rep.versions_pruned, 0);
        assert_eq!(noop_rep.keys_cleared, 0);
        assert_eq!(noop_rep.txns_reclaimed, 0);
    }

    let metrics_after = GLOBAL_METRICS.snapshot();
    assert!(metrics_after.vacuum_runs >= metrics_before.vacuum_runs + 20);
    assert!(metrics_after.mvcc_versions_pruned > metrics_before.mvcc_versions_pruned);
    assert!(metrics_after.mvcc_keys_cleared > metrics_before.mvcc_keys_cleared);
    assert!(metrics_after.mvcc_txns_reclaimed >= metrics_before.mvcc_txns_reclaimed + 250);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_concurrent_vacuum_reads_writes_and_tree_reference_checks() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_v2_vacuum_concurrent_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = Arc::new(TxnManager::with_storage(wal.clone(), bpm.clone()));

    // Seed initial keys k0..k7
    let t_seed = txn_mgr.begin().unwrap();
    for k in 0..8u32 {
        let key = format!("k{}", k).into_bytes();
        txn_mgr.put(t_seed, &tree, &key, b"seed").unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    // Thread 1: Writer thread updating keys and committing/aborting
    {
        let tm = txn_mgr.clone();
        let tr = tree.clone();
        let bar = barrier.clone();
        handles.push(thread::spawn(move || {
            bar.wait();
            for i in 0..60u32 {
                let key = format!("k{}", i % 8).into_bytes();
                let val = format!("val_{}", i).into_bytes();
                let tid = tm.begin().unwrap();
                if tm.put(tid, &tr, &key, &val).is_ok() {
                    if i % 6 == 0 {
                        let _ = tm.abort(tid);
                    } else {
                        let _ = tm.commit(tid);
                    }
                } else {
                    let _ = tm.abort(tid);
                }
            }
        }));
    }

    // Thread 2: Snapshot reader verifying snapshot consistency across repeated reads
    {
        let tm = txn_mgr.clone();
        let tr = tree.clone();
        let bar = barrier.clone();
        handles.push(thread::spawn(move || {
            bar.wait();
            for _ in 0..60u32 {
                let tid = tm
                    .begin_with_isolation(IsolationLevel::SnapshotIsolation)
                    .unwrap();
                let first_read = tm.get(tid, &tr, b"k0").unwrap();
                assert!(
                    first_read.is_some(),
                    "k0 must always be visible to every snapshot"
                );
                // Re-reading under the same snapshot even as vacuum runs concurrently must return the identical value
                let second_read = tm.get(tid, &tr, b"k0").unwrap();
                assert_eq!(first_read, second_read);
                tm.commit(tid).unwrap();
            }
        }));
    }

    // Thread 3: Vacuum runner
    {
        let tm = txn_mgr.clone();
        let bar = barrier.clone();
        handles.push(thread::spawn(move || {
            bar.wait();
            for _ in 0..60u32 {
                let _ = tm.vacuum();
            }
        }));
    }

    // Thread 4: Concurrent has_active_tree_references inspector (testing lock order mvcc_versions -> txns)
    {
        let tm = txn_mgr.clone();
        let tr = tree.clone();
        let bar = barrier.clone();
        handles.push(thread::spawn(move || {
            bar.wait();
            for _ in 0..60u32 {
                let _ = tm.has_active_tree_references(&tr);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final vacuum after all threads finish must reclaim all MVCC versions and txn contexts
    let _ = txn_mgr.vacuum();
    assert_eq!(txn_mgr.active_txn_count(), 0);
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_tombstone_cleanup_followed_by_reinsertion_and_vacuum() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_vacuum_reinsert_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

    // 1. Insert key and commit
    let t_insert = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t_insert, &tree, b"k_reinsert", b"v_initial")
        .unwrap();
    txn_mgr.commit(t_insert).unwrap();

    // 2. Delete key and commit
    let t_delete = txn_mgr.begin().unwrap();
    assert!(txn_mgr.delete(t_delete, &tree, b"k_reinsert").unwrap());
    txn_mgr.commit(t_delete).unwrap();

    // 3. Vacuum until tombstone is completely cleared
    let rep_clear = txn_mgr.vacuum();
    assert_eq!(rep_clear.versions_pruned, 2);
    assert_eq!(rep_clear.keys_cleared, 1);
    assert_eq!(rep_clear.txns_reclaimed, 2);
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    // 4. Start an old snapshot after tombstone is cleared but before reinsertion
    let t_old_snap = txn_mgr.begin().unwrap();
    assert_eq!(txn_mgr.get(t_old_snap, &tree, b"k_reinsert").unwrap(), None);
    assert!(txn_mgr.scan(t_old_snap, &tree).unwrap().is_empty());

    // 5. Reinsert the same key in a newer committed transaction
    let t_reinsert = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t_reinsert, &tree, b"k_reinsert", b"v_reinserted")
        .unwrap();
    txn_mgr.commit(t_reinsert).unwrap();

    // 6. Run vacuum while t_old_snap is still active:
    // Because t_reinsert.commit_ts > t_old_snap.read_ts (horizon_ts), vacuum MUST NOT
    // collapse the reinserted version into the B+Tree yet!
    let rep_during_old = txn_mgr.vacuum();
    assert_eq!(rep_during_old.versions_pruned, 0);
    assert_eq!(rep_during_old.keys_cleared, 0);
    assert_eq!(rep_during_old.txns_reclaimed, 0);

    // 7. Verify both old-snapshot (None) and new-snapshot (Some) visibility and scan results
    let t_new_snap = txn_mgr.begin().unwrap();

    assert_eq!(txn_mgr.get(t_old_snap, &tree, b"k_reinsert").unwrap(), None);
    assert!(txn_mgr.scan(t_old_snap, &tree).unwrap().is_empty());

    assert_eq!(
        txn_mgr.get(t_new_snap, &tree, b"k_reinsert").unwrap(),
        Some(b"v_reinserted".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_new_snap, &tree).unwrap(),
        vec![(b"k_reinsert".to_vec(), b"v_reinserted".to_vec())]
    );

    txn_mgr.commit(t_old_snap).unwrap();
    txn_mgr.commit(t_new_snap).unwrap();

    // 8. Final vacuum after old and new snapshots finish collapses the reinserted key
    let rep_final = txn_mgr.vacuum();
    assert_eq!(rep_final.versions_pruned, 1);
    assert_eq!(rep_final.keys_cleared, 1);
    assert_eq!(rep_final.txns_reclaimed, 3);
    assert_eq!(txn_mgr.mvcc_key_count(), 0);
    assert_eq!(txn_mgr.mvcc_version_count(), 0);
    assert_eq!(txn_mgr.tracked_txn_count(), 0);

    let t_post = txn_mgr.begin().unwrap();
    assert_eq!(
        txn_mgr.get(t_post, &tree, b"k_reinsert").unwrap(),
        Some(b"v_reinserted".to_vec())
    );
    assert_eq!(
        txn_mgr.scan(t_post, &tree).unwrap(),
        vec![(b"k_reinsert".to_vec(), b"v_reinserted".to_vec())]
    );
    txn_mgr.commit(t_post).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_abort_rollback_failure_protects_state_from_vacuum_and_recovers() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_v2_vacuum_abort_fail_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let root_id;
    let crashed_tid;
    {
        // Small 2-frame buffer pool to deterministically trigger BufferPoolError during abort undo
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 2));
        let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
        root_id = tree.root_page_id();
        let (aux_p1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (aux_p2, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();

        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

        // 1. Commit baseline value on k1
        let t_base = txn_mgr.begin().unwrap();
        txn_mgr.put(t_base, &tree, b"k1", b"v_committed").unwrap();
        txn_mgr.commit(t_base).unwrap();
        let _ = txn_mgr.vacuum();

        // 2. Start t_abort and overwrite k1 -> v_uncommitted
        let t_abort = txn_mgr.begin().unwrap();
        txn_mgr
            .put(t_abort, &tree, b"k1", b"v_uncommitted")
            .unwrap();

        // Pin both frames in the 2-frame buffer pool with aux_p1 and aux_p2 so tree's leaf page is evicted
        let _ = bpm.fetch_page(aux_p1).unwrap();
        let _ = bpm.fetch_page(aux_p2).unwrap();

        // 3. Call abort(t_abort): B+Tree undo fails because all buffer pool frames are pinned
        let abort_err = txn_mgr.abort(t_abort);
        assert!(matches!(abort_err, Err(Error::BufferPoolExhausted)));
        assert_eq!(txn_mgr.txn_state(t_abort), Some(TxnState::Aborted));
        assert!(txn_mgr.is_in_rollback(t_abort));
        assert!(!txn_mgr.is_rollback_poisoned(t_abort));

        // 4. Verify vacuum CANNOT prune MVCC state or reclaim t_abort while in_rollback is true
        let vac_rep = txn_mgr.vacuum();
        assert_eq!(vac_rep.versions_pruned, 0);
        assert_eq!(vac_rep.keys_cleared, 0);
        assert_eq!(vac_rep.txns_reclaimed, 0);
        assert_eq!(txn_mgr.mvcc_key_count(), 1);
        assert_eq!(txn_mgr.tracked_txn_count(), 1);

        // Verify tree references and purge protection remain active
        assert!(txn_mgr.has_active_tree_references(&tree));
        assert!(matches!(
            txn_mgr.purge_tree_state(&tree),
            Err(Error::WriteConflict(_))
        ));

        // Verify exclusive lock on k1 is STILL held by t_abort
        let t_conflict = txn_mgr.begin().unwrap();
        assert!(matches!(
            txn_mgr.put(t_conflict, &tree, b"k1", b"v_conflict"),
            Err(Error::LockTimeout)
        ));
        txn_mgr.abort(t_conflict).unwrap();

        // 5. Unpin aux_p1 so a reader can fetch the leaf page: reader must still see v_committed via MVCC chain
        bpm.unpin_page(aux_p1, false).unwrap();
        let t_reader = txn_mgr.begin().unwrap();
        assert_eq!(
            txn_mgr.get(t_reader, &tree, b"k1").unwrap(),
            Some(b"v_committed".to_vec())
        );
        txn_mgr.commit(t_reader).unwrap();

        // 6. Unpin aux_p2 and retry abort(t_abort): rollback now completes cleanly!
        bpm.unpin_page(aux_p2, false).unwrap();
        txn_mgr.abort(t_abort).unwrap();
        assert!(!txn_mgr.is_in_rollback(t_abort));
        assert!(!txn_mgr.has_active_tree_references(&tree));

        let vac_after_retry = txn_mgr.vacuum();
        assert!(vac_after_retry.txns_reclaimed >= 2);
        assert_eq!(txn_mgr.mvcc_key_count(), 0);
        assert_eq!(txn_mgr.mvcc_version_count(), 0);
        assert_eq!(txn_mgr.tracked_txn_count(), 0);

        // 7. Now test crash recovery when process restarts after a failed abort before Abort record is logged
        let t_crash = txn_mgr.begin().unwrap();
        crashed_tid = t_crash;
        txn_mgr
            .put(t_crash, &tree, b"k1", b"v_crashed_uncommitted")
            .unwrap();
        let _ = bpm.fetch_page(aux_p1).unwrap();
        let _ = bpm.fetch_page(aux_p2).unwrap();
        assert!(txn_mgr.abort(t_crash).is_err());
        assert!(txn_mgr.is_in_rollback(t_crash));
        // Vacuum while t_crash is stuck in failed rollback must not prune anything
        let vac_before_crash = txn_mgr.vacuum();
        assert_eq!(vac_before_crash.versions_pruned, 0);
        assert_eq!(vac_before_crash.txns_reclaimed, 0);

        bpm.unpin_page(aux_p1, false).unwrap();
        bpm.unpin_page(aux_p2, false).unwrap();
        wal.flush_all().unwrap();
        bpm.flush_all().unwrap();
    }

    // 8. Reopen storage and run ARIES RecoveryManager::recover()
    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert!(report.aborted_txns.contains(&crashed_tid));
        assert!(report.undone_records >= 1);

        let tree = Arc::new(BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap());
        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
        let t_verify = txn_mgr.begin().unwrap();
        assert_eq!(
            txn_mgr.get(t_verify, &tree, b"k1").unwrap(),
            Some(b"v_committed".to_vec())
        );
        txn_mgr.commit(t_verify).unwrap();
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
