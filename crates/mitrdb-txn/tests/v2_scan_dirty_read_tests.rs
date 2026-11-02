//! Reproducer: scans must never return values written by aborted transactions.
//!
//! Writers insert brand-new "ghost" keys and ALWAYS abort (never commit), so no
//! snapshot may ever observe a ghost key. If a scan reads the B+Tree outside the
//! MVCC lock and the writer's abort removes the MVCC chain in between, the scan
//! falls back to the stale tree value and leaks the ghost key (dirty read).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::{IsolationLevel, KeyRange, TxnManager};

const STABLE_KEYS: u32 = 1000;
const WRITERS: usize = 3;
const READER_ITERATIONS: u32 = 60;

fn is_ghost(key: &[u8]) -> bool {
    key.starts_with(b"zz_ghost")
}

fn check_rows(label: &str, iter: u32, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let ghosts: Vec<String> = rows
        .iter()
        .filter(|(k, _)| is_ghost(k))
        .map(|(k, v)| {
            format!(
                "{} => {}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            )
        })
        .collect();
    if !ghosts.is_empty() {
        return Err(format!(
            "DIRTY READ via {} at iteration {}: aborted-txn keys leaked into snapshot: {:?}",
            label, iter, ghosts
        ));
    }
    let stable = rows.iter().filter(|(k, _)| !is_ghost(k)).count();
    if stable != STABLE_KEYS as usize {
        return Err(format!(
            "{} at iteration {}: expected {} stable keys, got {}",
            label, iter, STABLE_KEYS, stable
        ));
    }
    Ok(())
}

#[test]
fn test_scans_never_leak_aborted_writer_keys() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_scan_dirty_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 4096));
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    let txn_mgr = Arc::new(TxnManager::with_storage(wal.clone(), bpm.clone()));

    // Seed committed, stable keys.
    let t_seed = txn_mgr.begin().unwrap();
    for k in 0..STABLE_KEYS {
        let key = format!("k_{:05}", k).into_bytes();
        txn_mgr.put(t_seed, &tree, &key, b"stable").unwrap();
    }
    txn_mgr.commit(t_seed).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // writers + reader + vacuum thread
    let barrier = Arc::new(Barrier::new(WRITERS + 2));
    let mut writer_handles = Vec::new();

    for w in 0..WRITERS {
        let tm = txn_mgr.clone();
        let tr = tree.clone();
        let bar = barrier.clone();
        let stop = stop.clone();
        writer_handles.push(thread::spawn(move || {
            bar.wait();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("zz_ghost_{}_{}", w, i).into_bytes();
                let tid = tm.begin().unwrap();
                let _ = tm.put(tid, &tr, &key, b"ghost");
                // Never commit: this key must never become visible to anyone.
                let _ = tm.abort(tid);
                i += 1;
            }
        }));
    }

    let vacuum_handle = {
        let tm = txn_mgr.clone();
        let bar = barrier.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            bar.wait();
            while !stop.load(Ordering::Relaxed) {
                let _ = tm.vacuum();
                thread::yield_now();
            }
        })
    };

    let reader_handle = {
        let tm = txn_mgr.clone();
        let tr = tree.clone();
        let bar = barrier.clone();
        thread::spawn(move || -> Result<(), String> {
            bar.wait();
            for iter in 0..READER_ITERATIONS {
                let tid = tm
                    .begin_with_isolation(IsolationLevel::SnapshotIsolation)
                    .unwrap();

                let r1 = tm.scan(tid, &tr).unwrap();
                check_rows("scan()", iter, &r1)?;

                let r2 = tm.scan_unlocked(tid, &tr).unwrap();
                check_rows("scan_unlocked()", iter, &r2)?;

                let r3 = tm
                    .range_scan_directed(tid, &tr, &KeyRange::full(), true)
                    .unwrap();
                check_rows("range_scan_directed(desc)", iter, &r3)?;

                tm.commit(tid).unwrap();
            }
            Ok(())
        })
    };

    let reader_result = reader_handle.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    for h in writer_handles {
        h.join().unwrap();
    }
    vacuum_handle.join().unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);

    if let Err(msg) = reader_result {
        panic!("{}", msg);
    }
}
