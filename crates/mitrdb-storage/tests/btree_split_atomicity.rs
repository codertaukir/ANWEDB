#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// Regression: split_leaf ko old_next leaf ka read fail hone par tree/WAL ko chhuna nahi chahiye.
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

#[test]
fn split_leaf_old_next_read_failure_leaves_tree_and_wal_untouched() {
    let dir = std::env::temp_dir().join(format!("mitrdb_split_atomic_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let max_keys = 4u16;

    let root_id;
    let l2_id;
    let children_before;
    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        let tree = BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), max_keys, 1, &mut prev)
            .unwrap();
        for i in 0..20u64 {
            tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8])
                .unwrap();
        }
        let c = wal
            .append_record(LogRecord::new_commit(0, 1, prev))
            .unwrap();
        wal.flush_up_to(c).unwrap();
        root_id = tree.root_page_id();
        let root = tree.read_node(root_id).unwrap();
        assert!(root.children.len() >= 3, "expected >=3 leaves under root");
        children_before = root.children.clone();
        l2_id = root.children[2];
        bpm.flush_all().unwrap();
    }

    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        f.seek(SeekFrom::Start(l2_id as u64 * 4096)).unwrap();
        f.write_all(&[0xFF; 4]).unwrap();
        f.sync_all().unwrap();
    }

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, max_keys).unwrap();
    let mut prev = wal.append_record(LogRecord::new_begin(0, 2)).unwrap();
    let prev_before = prev;
    let next_before = wal.next_lsn();

    // Key 9 L1 (8,10,12,14) ko split karwata hai; L1.next = corrupt L2.
    let res = tree.insert_logged(2, &mut prev, &key(9), &[9u8; 8]);
    assert!(res.is_err(), "insert should fail on corrupt next leaf");
    assert_eq!(prev, prev_before, "failed split must not advance prev_lsn");
    assert_eq!(
        wal.next_lsn(),
        next_before,
        "failed split must not append WAL records"
    );

    for i in 0..20u64 {
        // L2 ke keys corrupt page mein hain; unhe skip karo (sirf L2 ke keys 16..22 range ke)
        let k = 2 * i;
        if (16..=22).contains(&k) {
            continue;
        }
        assert!(
            tree.get(&key(k)).unwrap().is_some(),
            "committed key {} lost after failed split",
            k
        );
    }
    assert!(
        tree.get(&key(9)).unwrap().is_none(),
        "failed insert must not be visible"
    );
    let root = tree.read_node(tree.root_page_id()).unwrap();
    assert_eq!(
        root.children, children_before,
        "tree structure must be unchanged"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
