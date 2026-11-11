#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mitrdb_common::Error;
use mitrdb_storage::{BPlusTree, BufferPoolManager, DiskManager, KeyBound, KeyRange, WalManager};
use mitrdb_txn::{IsolationLevel, LockKey, LockMode, TxnManager};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> Result<TempDir, std::io::Error> {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("mitrdb_m2_4_txn_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(TempDir(dir))
}

fn i64_key(v: i64) -> Vec<u8> {
    let mut out = vec![1u8];
    let biased = (v as u64) ^ (1u64 << 63);
    out.extend_from_slice(&biased.to_be_bytes());
    out
}

#[test]
fn m2_4_range_semantics_bounds_inclusive_exclusive_empty_and_point() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_bounds.db");
    let wal_path = dir.path().join("m2_4_bounds.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    // Seed keys: 10, 20, 30, 40
    let t_seed = txn_mgr.begin().unwrap();
    for k in [10, 20, 30, 40] {
        txn_mgr
            .put(t_seed, &tree, &i64_key(k), format!("v{}", k).as_bytes())
            .unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    // 1. Empty ranges: [30, 10], (20, 20), [20, 20), (20, 20]
    let t_empty = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    for empty_range in [
        KeyRange::new(
            KeyBound::Included(i64_key(30)),
            KeyBound::Included(i64_key(10)),
        ),
        KeyRange::new(
            KeyBound::Excluded(i64_key(20)),
            KeyBound::Excluded(i64_key(20)),
        ),
        KeyRange::new(
            KeyBound::Included(i64_key(20)),
            KeyBound::Excluded(i64_key(20)),
        ),
        KeyRange::new(
            KeyBound::Excluded(i64_key(20)),
            KeyBound::Included(i64_key(20)),
        ),
    ] {
        assert!(empty_range.is_empty());
        let rows = txn_mgr.range_scan(t_empty, &tree, &empty_range).unwrap();
        assert!(rows.is_empty());
    }
    // Empty range scans must not block writes on key 20
    let t_writer = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    txn_mgr
        .put(t_writer, &tree, &i64_key(20), b"v20_updated")
        .unwrap();
    txn_mgr.abort(t_writer).unwrap();
    txn_mgr.commit(t_empty).unwrap();

    // 2. Half-open range [10, 30): includes 10 and 20, excludes 30
    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let r_half_open = KeyRange::new(
        KeyBound::Included(i64_key(10)),
        KeyBound::Excluded(i64_key(30)),
    );
    let rows = txn_mgr.range_scan(t1, &tree, &r_half_open).unwrap();
    let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![i64_key(10), i64_key(20)]);

    // Boundary check: inserting/updating 30 (excluded upper bound) or 9 (below lower bound) succeeds!
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    txn_mgr.put(t2, &tree, &i64_key(9), b"v9").unwrap();
    txn_mgr.put(t2, &tree, &i64_key(30), b"v30_new").unwrap();
    txn_mgr.commit(t2).unwrap();

    // Boundary check: inserting 10 (included lower bound) or 29 (inside [10, 30)) fails!
    let t3 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t3, &tree, &i64_key(10), b"conflict"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t3, &tree, &i64_key(29), b"conflict"),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t3).unwrap();
    txn_mgr.commit(t1).unwrap();

    // 3. Half-open range (10, 30]: excludes 10, includes 20 and 30
    let t4 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let r_open_closed = KeyRange::new(
        KeyBound::Excluded(i64_key(10)),
        KeyBound::Included(i64_key(30)),
    );
    let rows = txn_mgr.range_scan(t4, &tree, &r_open_closed).unwrap();
    let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![i64_key(20), i64_key(30)]);

    // Updating 10 (excluded lower bound) or inserting 31 (above upper bound) succeeds!
    let t5 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    txn_mgr.put(t5, &tree, &i64_key(10), b"v10_new").unwrap();
    txn_mgr.put(t5, &tree, &i64_key(31), b"v31").unwrap();
    txn_mgr.commit(t5).unwrap();

    // Inserting 11 or updating 30 (included upper bound) fails!
    let t6 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t6, &tree, &i64_key(11), b"conflict"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t6, &tree, &i64_key(30), b"conflict"),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t6).unwrap();
    txn_mgr.commit(t4).unwrap();
    assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_gap_and_next_key_locking_prevents_phantoms() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_gap.db");
    let wal_path = dir.path().join("m2_4_gap.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    let t_seed = txn_mgr.begin().unwrap();
    for k in [10, 20, 30] {
        txn_mgr
            .put(t_seed, &tree, &i64_key(k), format!("val_{}", k).as_bytes())
            .unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    // T1 (Serializable) scans [12, 28], which spans inside [12, 20], key 20, and into gap (20, 28]
    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let scan_range = KeyRange::new(
        KeyBound::Included(i64_key(12)),
        KeyBound::Included(i64_key(28)),
    );
    let initial_rows = txn_mgr.range_scan(t1, &tree, &scan_range).unwrap();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].0, i64_key(20));

    // Verify Range, NextKey, and Gap locks are held by T1 in LockManager
    let held = txn_mgr.lock_manager().held_locks_for_txn(t1);
    assert!(held.iter().any(|(k, m)| *m == LockMode::Shared
        && matches!(
            k,
            LockKey::NextKey { range, .. }
                if range.start == KeyBound::Included(i64_key(12))
                    && range.end == KeyBound::Included(i64_key(20))
        )));
    assert!(held.iter().any(|(k, m)| *m == LockMode::Shared
        && matches!(
            k,
            LockKey::Gap { range, .. }
                if range.start == KeyBound::Excluded(i64_key(20))
                    && range.end == KeyBound::Included(i64_key(28))
        )));

    // T2 attempts phantom inserts into gap [12, 20) and gap (20, 28], or update/delete on key 20
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(15), b"phantom_15"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(25), b"phantom_25"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(20), b"modify_20"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.delete(t2, &tree, &i64_key(20)),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t2).unwrap();

    // Verify rejected writes did not mutate BPlusTree or MVCC state before acquiring locks
    assert_eq!(tree.get(&i64_key(15)).unwrap(), None);
    assert_eq!(tree.get(&i64_key(25)).unwrap(), None);

    // T3 writes outside [12, 28] (e.g., key 5, key 10, key 30, key 35) -> succeeds!
    let t3 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    txn_mgr.put(t3, &tree, &i64_key(5), b"val_5").unwrap();
    txn_mgr
        .put(t3, &tree, &i64_key(10), b"val_10_updated")
        .unwrap();
    txn_mgr
        .put(t3, &tree, &i64_key(30), b"val_30_updated")
        .unwrap();
    txn_mgr.put(t3, &tree, &i64_key(35), b"val_35").unwrap();
    txn_mgr.commit(t3).unwrap();

    // T1 re-scans [12, 28] -> zero phantoms, identical result!
    let repeat_rows = txn_mgr.range_scan(t1, &tree, &scan_range).unwrap();
    assert_eq!(repeat_rows, initial_rows);
    txn_mgr.commit(t1).unwrap();

    assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_point_reads_and_writes_interact_with_range_locks() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_point_range.db");
    let wal_path = dir.path().join("m2_4_point_range.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    let t_seed = txn_mgr.begin().unwrap();
    txn_mgr.put(t_seed, &tree, &i64_key(25), b"v25").unwrap();
    txn_mgr.commit(t_seed).unwrap();

    // Case A: Active writer holds Exclusive point lock on 25 -> Serializable range scan [20, 30] must conflict
    let t_writer = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    txn_mgr
        .put(t_writer, &tree, &i64_key(25), b"v25_uncommitted")
        .unwrap();

    let t_scanner = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let r_conflict = KeyRange::new(
        KeyBound::Included(i64_key(20)),
        KeyBound::Included(i64_key(30)),
    );
    assert!(matches!(
        txn_mgr.range_scan(t_scanner, &tree, &r_conflict),
        Err(Error::LockTimeout)
    ));

    // Disjoint range [1, 19] in t_scanner succeeds even while t_writer holds Exclusive lock on 25
    let r_disjoint = KeyRange::new(
        KeyBound::Included(i64_key(1)),
        KeyBound::Included(i64_key(19)),
    );
    assert!(txn_mgr.range_scan(t_scanner, &tree, &r_disjoint).is_ok());

    txn_mgr.abort(t_scanner).unwrap();
    txn_mgr.abort(t_writer).unwrap();

    // Case B: Active Serializable range scan [20, 30] allows concurrent Serializable point read get(25)
    // but blocks concurrent point write put(25) / delete(25)
    let t_range_holder = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let rows = txn_mgr
        .range_scan(t_range_holder, &tree, &r_conflict)
        .unwrap();
    assert_eq!(rows.len(), 1);

    let t_point_reader = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert_eq!(
        txn_mgr.get(t_point_reader, &tree, &i64_key(25)).unwrap(),
        Some(b"v25".to_vec())
    );
    txn_mgr.commit(t_point_reader).unwrap();

    let t_point_writer = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t_point_writer, &tree, &i64_key(25), b"blocked"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.delete(t_point_writer, &tree, &i64_key(25)),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t_point_writer).unwrap();
    txn_mgr.commit(t_range_holder).unwrap();

    assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_btree_leaf_splits_and_structural_changes_preserve_range_locks() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_splits.db");
    let wal_path = dir.path().join("m2_4_splits.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 128));
    // max_keys = 3 forces frequent leaf splits and parent separator insertions
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 3).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    let t_seed = txn_mgr.begin().unwrap();
    for k in [100, 200, 300, 400] {
        txn_mgr
            .put(t_seed, &tree, &i64_key(k), format!("v{}", k).as_bytes())
            .unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    // T1 (Serializable) locks range [150, 250]
    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let locked_range = KeyRange::new(
        KeyBound::Included(i64_key(150)),
        KeyBound::Included(i64_key(250)),
    );
    let r1 = txn_mgr.range_scan(t1, &tree, &locked_range).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].0, i64_key(200));

    // T2 inserts many keys < 150 and > 250, triggering multiple leaf splits and internal node splits
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    for k in [
        10, 20, 30, 40, 50, 60, 70, 80, 90, 110, 120, 130, 140, 260, 270, 280, 290, 310, 320, 330,
    ] {
        txn_mgr
            .put(t2, &tree, &i64_key(k), format!("v{}", k).as_bytes())
            .unwrap();
    }
    txn_mgr.commit(t2).unwrap();

    // After heavy B+Tree structural splits, T1's logical range protection on [150, 250] must still hold!
    let t3 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t3, &tree, &i64_key(175), b"phantom_after_split"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t3, &tree, &i64_key(225), b"phantom_after_split"),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t3).unwrap();

    // T1 re-scans [150, 250] across the split tree -> still sees only [200]
    let r2 = txn_mgr.range_scan(t1, &tree, &locked_range).unwrap();
    assert_eq!(r2, r1);
    txn_mgr.commit(t1).unwrap();
}

#[test]
fn m2_4_delete_and_reinsert_within_locked_range_and_isolation_comparison() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_del_reinsert.db");
    let wal_path = dir.path().join("m2_4_del_reinsert.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    // Insert 10, 20, 30 then delete 20
    let t0 = txn_mgr.begin().unwrap();
    txn_mgr.put(t0, &tree, &i64_key(10), b"v10").unwrap();
    txn_mgr.put(t0, &tree, &i64_key(20), b"v20").unwrap();
    txn_mgr.put(t0, &tree, &i64_key(30), b"v30").unwrap();
    txn_mgr.commit(t0).unwrap();

    let t_del = txn_mgr.begin().unwrap();
    assert!(txn_mgr.delete(t_del, &tree, &i64_key(20)).unwrap());
    txn_mgr.commit(t_del).unwrap();

    // T1 (Serializable) scans [15, 25] -> sees 0 rows, locks gap covering deleted key 20
    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let range = KeyRange::new(
        KeyBound::Included(i64_key(15)),
        KeyBound::Included(i64_key(25)),
    );
    let rows = txn_mgr.range_scan(t1, &tree, &range).unwrap();
    assert!(rows.is_empty());

    // T2 attempts to reinsert deleted key 20 -> blocked by T1's Serializable gap/range lock!
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(20), b"reinserted_20"),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t2).unwrap();
    txn_mgr.commit(t1).unwrap();

    // Contrast with SnapshotIsolation and ReadCommitted: scanning [15, 25] does NOT block reinsert of 20
    let t_si = txn_mgr
        .begin_with_isolation(IsolationLevel::SnapshotIsolation)
        .unwrap();
    let t_rc = txn_mgr
        .begin_with_isolation(IsolationLevel::ReadCommitted)
        .unwrap();
    assert!(txn_mgr.range_scan(t_si, &tree, &range).unwrap().is_empty());
    assert!(txn_mgr.range_scan(t_rc, &tree, &range).unwrap().is_empty());

    let t_reinsert = txn_mgr.begin().unwrap();
    txn_mgr
        .put(t_reinsert, &tree, &i64_key(20), b"reinserted_20")
        .unwrap();
    txn_mgr.commit(t_reinsert).unwrap();

    // SI still sees empty range from its snapshot; RC sees the newly committed key 20
    assert!(txn_mgr.range_scan(t_si, &tree, &range).unwrap().is_empty());
    assert_eq!(txn_mgr.range_scan(t_rc, &tree, &range).unwrap().len(), 1);
    txn_mgr.commit(t_si).unwrap();
    txn_mgr.commit(t_rc).unwrap();
}

#[test]
fn m2_4_deadlock_conversion_and_failed_rollback_lock_retention() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_deadlock.db");
    let wal_path = dir.path().join("m2_4_deadlock.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    // 1-frame buffer pool to also test failed rollback retention of range locks
    let bpm = Arc::new(BufferPoolManager::new(disk, 1));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm.clone());

    // 1. Overlapping Shared range locks upgraded to Exclusive writes (classic conversion deadlock pattern)
    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();

    let r1 = KeyRange::new(
        KeyBound::Included(i64_key(10)),
        KeyBound::Included(i64_key(30)),
    );
    let r2 = KeyRange::new(
        KeyBound::Included(i64_key(20)),
        KeyBound::Included(i64_key(40)),
    );
    txn_mgr.range_scan(t1, &tree, &r1).unwrap();
    txn_mgr.range_scan(t2, &tree, &r2).unwrap();

    // Both T1 and T2 try to insert 25 into the overlapping region [20, 30] -> both fail deterministically
    assert!(matches!(
        txn_mgr.put(t1, &tree, &i64_key(25), b"from_t1"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(25), b"from_t2"),
        Err(Error::LockTimeout)
    ));

    // Abort T1 -> releases T1's range locks, allowing T2 to insert 25 and commit!
    txn_mgr.abort(t1).unwrap();
    txn_mgr.put(t2, &tree, &i64_key(25), b"from_t2").unwrap();
    txn_mgr.commit(t2).unwrap();
    assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);

    // 2. Failed rollback must retain Serializable range locks so concurrent writers cannot corrupt state
    let t3 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let r3 = KeyRange::new(
        KeyBound::Included(i64_key(100)),
        KeyBound::Included(i64_key(200)),
    );
    txn_mgr.range_scan(t3, &tree, &r3).unwrap();
    txn_mgr.put(t3, &tree, &i64_key(5), b"dirty_5").unwrap();

    // Pin an auxiliary page in the 1-frame buffer pool so T3's physical rollback fails with BufferPoolFull
    let (aux_pid, _) = bpm.new_page(0).unwrap();

    assert!(txn_mgr.abort(t3).is_err());

    // While T3's rollback is incomplete, its Serializable range lock on [100, 200] MUST remain held!
    let t4 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    assert!(matches!(
        txn_mgr.put(t4, &tree, &i64_key(150), b"must_be_blocked"),
        Err(Error::LockTimeout)
    ));
    txn_mgr.abort(t4).unwrap();
    bpm.unpin_page(aux_pid, false).unwrap();
}

#[test]
fn m2_4_1_audit_adjacent_ranges_and_immediate_byte_successor_bounds() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_1_adjacent.db");
    let wal_path = dir.path().join("m2_4_1_adjacent.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 64));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap());
    let txn_mgr = TxnManager::with_storage(wal, bpm);

    // 1. Verify immediate byte-successor empty open range: (b"abc", b"abc\0") has no valid byte slice between them
    let empty_succ = KeyRange::new(
        KeyBound::Excluded(b"abc".to_vec()),
        KeyBound::Excluded(b"abc\0".to_vec()),
    );
    assert!(empty_succ.is_empty());
    assert!(!empty_succ.contains(b"abc"));
    assert!(!empty_succ.contains(b"abc\0"));

    // 2. Verify adjacent ranges [10, 20] and (20, 30] do NOT overlap, while [10, 20] and [20, 30] DO overlap at 20
    let r_left_closed = KeyRange::new(
        KeyBound::Included(i64_key(10)),
        KeyBound::Included(i64_key(20)),
    );
    let r_right_open = KeyRange::new(
        KeyBound::Excluded(i64_key(20)),
        KeyBound::Included(i64_key(30)),
    );
    let r_right_closed = KeyRange::new(
        KeyBound::Included(i64_key(20)),
        KeyBound::Included(i64_key(30)),
    );
    assert!(!r_left_closed.overlaps(&r_right_open));
    assert!(r_left_closed.overlaps(&r_right_closed));

    let t1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let t2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();

    // T1 locks [10, 20] via range_scan; T2 locks adjacent (20, 30] via range_scan
    txn_mgr.range_scan(t1, &tree, &r_left_closed).unwrap();
    txn_mgr.range_scan(t2, &tree, &r_right_open).unwrap();

    // T1 can write 20 (since T2's range excludes 20), and T2 can write 21 (since T1's range ends at 20)
    txn_mgr.put(t1, &tree, &i64_key(20), b"t1_at_20").unwrap();
    txn_mgr.put(t2, &tree, &i64_key(21), b"t2_at_21").unwrap();

    // Neither can write into the other's range
    assert!(matches!(
        txn_mgr.put(t1, &tree, &i64_key(25), b"cross_into_t2"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        txn_mgr.put(t2, &tree, &i64_key(20), b"cross_into_t1"),
        Err(Error::LockTimeout)
    ));

    txn_mgr.commit(t1).unwrap();
    txn_mgr.commit(t2).unwrap();
    assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_1_audit_concurrent_threads_barrier_range_scans_and_conflicting_writes() {
    use std::sync::Barrier;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_1_conc.db");
    let wal_path = dir.path().join("m2_4_1_conc.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 128));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = Arc::new(TxnManager::with_storage(wal, bpm));

    let t_seed = txn_mgr.begin().unwrap();
    for k in [10, 30, 50, 70] {
        txn_mgr
            .put(t_seed, &tree, &i64_key(k), format!("v{}", k).as_bytes())
            .unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    // Run 8 deterministic rounds with a 4-thread Barrier:
    // - 2 Serializable range-scanner threads scanning [10, 50] twice and verifying repeatable results
    // - 1 conflicting writer thread attempting to insert a phantom key into [10, 50]
    // - 1 disjoint writer thread inserting outside [10, 50] (key >= 100 + round)
    for round in 0..8i64 {
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _scanner_id in 0..2 {
            let tm = txn_mgr.clone();
            let tr = tree.clone();
            let bar = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let tid = tm
                    .begin_with_isolation(IsolationLevel::Serializable)
                    .unwrap();
                let range = KeyRange::new(
                    KeyBound::Included(i64_key(10)),
                    KeyBound::Included(i64_key(50)),
                );
                let first = tm.range_scan(tid, &tr, &range).unwrap();
                bar.wait();
                bar.wait();
                let second = tm.range_scan(tid, &tr, &range).unwrap();
                assert_eq!(first, second, "Phantom detected during Serializable scan");
                tm.commit(tid).unwrap();
            }));
        }

        {
            let tm = txn_mgr.clone();
            let tr = tree.clone();
            let bar = barrier.clone();
            let phantom_key = 20 + round;
            handles.push(std::thread::spawn(move || {
                bar.wait();
                let tid = tm
                    .begin_with_isolation(IsolationLevel::Serializable)
                    .unwrap();
                let res = tm.put(tid, &tr, &i64_key(phantom_key), b"phantom");
                assert!(
                    matches!(res, Err(Error::LockTimeout)),
                    "Conflicting insert inside active Serializable range must fail with LockTimeout"
                );
                tm.abort(tid).unwrap();
                bar.wait();
            }));
        }

        {
            let tm = txn_mgr.clone();
            let tr = tree.clone();
            let bar = barrier.clone();
            let outside_key = 100 + round;
            handles.push(std::thread::spawn(move || {
                bar.wait();
                let tid = tm
                    .begin_with_isolation(IsolationLevel::Serializable)
                    .unwrap();
                tm.put(tid, &tr, &i64_key(outside_key), b"disjoint_ok")
                    .unwrap();
                tm.commit(tid).unwrap();
                bar.wait();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(txn_mgr.lock_manager().active_lock_count(), 0);
    }
}
