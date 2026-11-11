#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// commit(): transient WAL flush failure is retried; persistent failure still poisons.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_cfr_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(
    dir: &Path,
) -> (
    Arc<WalManager>,
    Arc<BufferPoolManager>,
    Arc<BPlusTree>,
    TxnManager,
) {
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
        assert_eq!(
            std::fs::metadata(wal_path).unwrap().len(),
            0,
            "wal already has data"
        );
        std::fs::remove_file(wal_path).unwrap();
    }
    std::fs::create_dir(wal_path).unwrap();
}

#[test]
fn transient_flush_failure_is_retried_and_commit_succeeds() {
    let dir = tmp("transient");
    let wal_path = dir.join("t.wal");
    let root_id;
    {
        let (wal, _bpm, tree, mgr) = open(&dir);
        root_id = tree.root_page_id();
        block_wal(&wal_path);
        let p = wal_path.clone();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            std::fs::remove_dir(&p).unwrap();
        });
        let a = mgr.begin().unwrap();
        mgr.put(a, &tree, b"key_a", b"val_a").unwrap();
        let r = mgr.commit(a);
        h.join().unwrap();
        assert!(
            r.is_ok(),
            "commit should survive a transient flush failure, got {:?}",
            r.as_ref().err().map(|e| e.to_string())
        );
        assert!(!mgr.is_commit_poisoned(a));
        assert!(wal.flushed_lsn() > 0);
    }
    {
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        RecoveryManager::new(wal.clone(), bpm.clone())
            .recover()
            .unwrap();
        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
        assert!(
            tree.get(b"key_a").unwrap().is_some(),
            "committed key lost after recovery"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_flush_failure_poisons_and_abort_is_rejected() {
    let dir = tmp("persistent");
    let wal_path = dir.join("t.wal");
    {
        let (_wal, _bpm, tree, mgr) = open(&dir);
        block_wal(&wal_path);
        let a = mgr.begin().unwrap();
        mgr.put(a, &tree, b"key_a", b"val_a").unwrap();
        assert!(mgr.commit(a).is_err());
        assert!(mgr.is_commit_poisoned(a));
        assert!(
            mgr.abort(a).is_err(),
            "abort of a poisoned commit must be rejected"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
