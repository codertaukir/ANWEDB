#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// A mid-statement WAL flush of uncommitted deletes (plus dirty pages on disk) must be undone by recovery.
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn k(i: u32) -> Vec<u8> {
    format!("k{:05}", i).into_bytes()
}

#[test]
fn flushed_uncommitted_deletes_are_undone_by_recovery() {
    let dir = std::env::temp_dir().join(format!("mitrdb_walpress_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let root_id;
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 128));
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
        let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
        let mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

        let t0 = mgr.begin().unwrap();
        for i in 0..300u32 {
            mgr.put(t0, &tree, &k(i), b"v").unwrap();
        }
        mgr.commit(t0).unwrap();
        root_id = tree.root_page_id();

        let t = mgr.begin().unwrap();
        for i in 0..300u32 {
            assert!(mgr.delete(t, &tree, &k(i)).unwrap());
        }
        assert!(wal.pending_bytes_len() > 0);
        mgr.relieve_wal_pressure(0).unwrap();
        assert_eq!(
            wal.pending_bytes_len(),
            0,
            "pressure flush must drain the arena"
        );
        bpm.flush_all().unwrap();
        // crash: no commit, no abort
    }
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 128));
        RecoveryManager::new(wal.clone(), bpm.clone())
            .recover()
            .unwrap();
        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
        for i in 0..300u32 {
            assert!(
                matches!(tree.get(&k(i)), Ok(Some(_))),
                "key {} lost: uncommitted flushed delete was not undone",
                i
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
