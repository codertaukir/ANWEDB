// resolve_poisoned_commit: persistent flush failure poisons; once WAL works again, resolve completes the commit.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mitrdb_common::Error;
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_prc_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(dir: &Path) -> (Arc<WalManager>, Arc<BufferPoolManager>, Arc<BPlusTree>, TxnManager) {
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    bpm.flush_all().unwrap();
    let mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    (wal, bpm, tree, mgr)
}

fn block_wal(wal_path: &Path) {
    if wal_path.is_file() {
        assert_eq!(std::fs::metadata(wal_path).unwrap().len(), 0, "wal already has data");
        std::fs::remove_file(wal_path).unwrap();
    }
    std::fs::create_dir(wal_path).unwrap();
}

#[test]
fn resolve_completes_poisoned_commit_and_releases_locks() {
    let dir = tmp("resolve");
    let wal_path = dir.join("t.wal");
    let root_id;
    {
        let (wal, _bpm, tree, mgr) = open(&dir);
        root_id = tree.root_page_id();
        block_wal(&wal_path);

        let a = mgr.begin().unwrap();
        mgr.put(a, &tree, b"key_a", b"val_a").unwrap();
        assert!(mgr.commit(a).is_err());
        assert!(mgr.is_commit_poisoned(a));

        // locks still held while poisoned
        let c = mgr.begin().unwrap();
        let r = mgr.put(c, &tree, b"key_a", b"val_c");
        assert!(matches!(r, Err(Error::LockTimeout)), "expected LockTimeout, got {:?}", r.err().map(|e| e.to_string()));
        let _ = mgr.abort(c);

        // WAL still broken: resolve fails and txn stays poisoned
        assert!(mgr.resolve_poisoned_commit(a).is_err());
        assert!(mgr.is_commit_poisoned(a));

        // WAL healthy again
        std::fs::remove_dir(&wal_path).unwrap();
        let ts = mgr.resolve_poisoned_commit(a).expect("resolve should complete the commit");
        assert!(ts > 0);
        assert!(!mgr.is_commit_poisoned(a));
        assert!(wal.flushed_lsn() > 0);
        // resolving again is rejected (no longer poisoned)
        assert!(mgr.resolve_poisoned_commit(a).is_err());

        // locks released and value visible to a new txn
        let d = mgr.begin().unwrap();
        assert_eq!(mgr.get(d, &tree, b"key_a").unwrap().as_deref(), Some(&b"val_a"[..]));
        mgr.put(d, &tree, b"key_a", b"val_d").unwrap();
        mgr.commit(d).unwrap();
    }
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        RecoveryManager::new(wal.clone(), bpm.clone()).recover().unwrap();
        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
        assert_eq!(tree.get(b"key_a").unwrap().as_deref(), Some(&b"val_d"[..]));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resolve_rejects_non_poisoned_txn() {
    let dir = tmp("nonpoisoned");
    {
        let (_wal, _bpm, tree, mgr) = open(&dir);
        let a = mgr.begin().unwrap();
        mgr.put(a, &tree, b"k", b"v").unwrap();
        assert!(mgr.resolve_poisoned_commit(a).is_err(), "active txn is not resolvable");
        mgr.commit(a).unwrap();
        assert!(mgr.resolve_poisoned_commit(a).is_err(), "committed txn is not resolvable");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
