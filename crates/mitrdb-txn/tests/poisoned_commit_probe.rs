// Probe: pehla WAL flush fail (segment path par directory) -> poisoned commit ka behaviour.
// Run: cargo test --release -p mitrdb-txn --test poisoned_commit_probe -- --ignored --nocapture
use std::sync::Arc;
use std::time::Instant;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn s<T, E: std::fmt::Display>(r: &Result<T, E>) -> String {
    match r {
        Ok(_) => "Ok".to_string(),
        Err(e) => format!("Err({})", e),
    }
}

#[test]
#[ignore]
fn probe_poisoned_commit() {
    let dir = std::env::temp_dir().join(format!("mitrdb_poison_probe_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let root_id;

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
        root_id = tree.root_page_id();
        bpm.flush_all().unwrap();
        let mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

        if wal_path.is_file() {
            let len = std::fs::metadata(&wal_path).unwrap().len();
            assert_eq!(len, 0, "wal file already has data, probe invalid");
            std::fs::remove_file(&wal_path).unwrap();
        }
        std::fs::create_dir(&wal_path).unwrap();
        println!("PROBE wal path {:?} is now a directory", wal_path);

        let a = mgr.begin().unwrap();
        let put_a = mgr.put(a, &tree, b"key_a", b"val_a");
        println!("PROBE A put -> {}", s(&put_a));
        let commit_a = mgr.commit(a);
        println!("PROBE A commit -> {}", s(&commit_a));
        println!("PROBE A is_commit_poisoned = {}", mgr.is_commit_poisoned(a));
        println!("PROBE wal flushed_lsn={} next_lsn={}", wal.flushed_lsn(), wal.next_lsn());

        let ab = mgr.abort(a);
        println!("PROBE A abort -> {}", s(&ab));

        let c = mgr.begin().unwrap();
        let t0 = Instant::now();
        let put_c = mgr.put(c, &tree, b"key_a", b"val_c");
        println!("PROBE C put same key -> {} after {:?}", s(&put_c), t0.elapsed());
        let _ = mgr.abort(c);

        std::fs::remove_dir(&wal_path).unwrap();
        println!("PROBE directory removed, flush can work again");
        let b = mgr.begin().unwrap();
        let put_b = mgr.put(b, &tree, b"key_b", b"val_b");
        println!("PROBE B put -> {}", s(&put_b));
        let commit_b = mgr.commit(b);
        println!("PROBE B commit -> {}", s(&commit_b));
        println!("PROBE wal flushed_lsn={} next_lsn={}", wal.flushed_lsn(), wal.next_lsn());
        println!("PROBE A is_commit_poisoned (memory) = {}", mgr.is_commit_poisoned(a));
        // crash: bpm ko flush nahi karte
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
        let _ = recovery.recover().unwrap();
        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
        println!("PROBE after recovery: key_a present = {:?}", tree.get(b"key_a").map(|v| v.is_some()));
        println!("PROBE after recovery: key_b present = {:?}", tree.get(b"key_b").map(|v| v.is_some()));
    }
    let _ = std::fs::remove_dir_all(&dir);
}
