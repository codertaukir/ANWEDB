#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// resolve_poisoned_commits: sweep completes poisoned commits once the WAL heals, releasing their locks.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_sweep_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(dir: &Path) -> (Arc<BPlusTree>, TxnManager) {
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    bpm.flush_all().unwrap();
    let mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    (tree, mgr)
}

#[test]
fn sweep_resolves_poisoned_commits_after_wal_heals() {
    let dir = tmp("sweep");
    let wal_path = dir.join("t.wal");
    let (tree, mgr) = open(&dir);
    if wal_path.is_file() {
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            0,
            "wal already has data"
        );
        std::fs::remove_file(&wal_path).unwrap();
    }
    std::fs::create_dir(&wal_path).unwrap();

    assert_eq!(mgr.poisoned_commit_count(), 0);
    assert_eq!(mgr.resolve_poisoned_commits(), 0);

    let a = mgr.begin().unwrap();
    mgr.put(a, &tree, b"key_a", b"val_a").unwrap();
    assert!(mgr.commit(a).is_err());
    assert_eq!(mgr.poisoned_commit_count(), 1);

    // WAL still broken: sweep resolves nothing, txn stays poisoned
    assert_eq!(mgr.resolve_poisoned_commits(), 0);
    assert_eq!(mgr.poisoned_commit_count(), 1);

    std::fs::remove_dir(&wal_path).unwrap();
    assert_eq!(mgr.resolve_poisoned_commits(), 1);
    assert_eq!(mgr.poisoned_commit_count(), 0);
    assert!(!mgr.is_commit_poisoned(a));
    assert_eq!(mgr.resolve_poisoned_commits(), 0);

    // locks released: another txn can write the same key
    let c = mgr.begin().unwrap();
    mgr.put(c, &tree, b"key_a", b"val_c").unwrap();
    mgr.commit(c).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
