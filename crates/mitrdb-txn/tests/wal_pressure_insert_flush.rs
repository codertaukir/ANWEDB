// A mid-statement WAL flush of uncommitted INSERTs (plus dirty pages on disk) must be undone by recovery.
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
fn flushed_uncommitted_inserts_are_undone_by_recovery() {
    let dir = std::env::temp_dir().join(format!("mitrdb_walpress_ins_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let val = vec![b'x'; 200];
    let root_id;
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 128));
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
        let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
        let mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

        root_id = tree.root_page_id();

        let t = mgr.begin().unwrap();
        for i in 0..300u32 {
            mgr.put(t, &tree, &k(i), &val).unwrap();
        }
        assert!(wal.pending_bytes_len() > 0, "WAL arena must hold uncommitted inserts");
        mgr.relieve_wal_pressure(0).unwrap();
        assert_eq!(wal.pending_bytes_len(), 0, "pressure flush must drain the arena");
        bpm.flush_all().unwrap();
        // crash simulation: no commit, no abort
    }
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 128));
        RecoveryManager::new(wal.clone(), bpm.clone()).recover().unwrap();
        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
        for i in 0..300u32 {
            assert!(
                matches!(tree.get(&k(i)), Ok(None)),
                "uncommitted flushed insert key {} was visible after crash recovery",
                i
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
