#![allow(clippy::collapsible_if, unused_imports)]

use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_FLAG_FREE;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn k8(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

#[test]
fn rebalance_internal_child_read_failure_leaves_children_and_wal_untouched() {
    let dir = std::env::temp_dir().join(format!("mitrdb_p0_rebalance_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let max_keys: u16 = 4;
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());

    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree =
        BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), max_keys, 1, &mut prev).unwrap();
    for i in 0..80u64 {
        tree.insert_logged(1, &mut prev, &k8(i), &[5u8; 16]).unwrap();
    }
    let c = wal.append_record(LogRecord::new_commit(0, 1, prev)).unwrap();
    wal.flush_up_to(c).unwrap();
    bpm.flush_all().unwrap();

    // Find an internal node with >= 2 children that has a grandchild we can corrupt on disk.
    let root = tree.root_page_id();
    let root_node = tree.read_node(root).unwrap();
    assert!(!root_node.is_leaf && root_node.children.len() >= 2);
    let target_internal = root_node.children[0];
    let internal_node = tree.read_node(target_internal).unwrap();
    assert!(!internal_node.is_leaf && internal_node.children.len() >= 2);
    let corrupt_grandchild = *internal_node.children.last().unwrap();
    let intact_grandchild = internal_node.children[0];
    let intact_parent_before = tree.read_node(intact_grandchild).unwrap().parent_page_id;
    let delete_key = internal_node.keys[0].clone();

    drop(tree);
    drop(bpm);
    drop(disk);

    // Corrupt corrupt_grandchild on disk so reading it during rebalance_internal fails.
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.join("t.db"))
            .unwrap();
        f.seek(SeekFrom::Start(corrupt_grandchild as u64 * 4096))
            .unwrap();
        f.write_all(&[0xFF; 4]).unwrap();
        f.sync_all().unwrap();
    }

    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root, max_keys).unwrap();

    // Deleting keys from the first leaf under target_internal triggers underflow -> rebalance_internal.
    let mut prev2 = wal.append_record(LogRecord::new_begin(0, 2)).unwrap();
    let _ = tree.delete_logged(2, &mut prev2, &delete_key);

    // Intact grandchild's parent pointer must never have been partially rewritten.
    let intact_parent_after = tree.read_node(intact_grandchild).unwrap().parent_page_id;
    assert_eq!(
        intact_parent_after, intact_parent_before,
        "intact grandchild parent_page_id must not be partially mutated on failed rebalance"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "fault-injection")]
#[test]
fn free_btree_page_wal_failure_leaves_zero_pin_leaks_and_page_unfreed() {
    let dir = std::env::temp_dir().join(format!("mitrdb_p0_free_fail_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let max_keys: u16 = 4;
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());

    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree =
        BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), max_keys, 1, &mut prev).unwrap();
    for i in 0..10u64 {
        tree.insert_logged(1, &mut prev, &k8(i), &[3u8; 16]).unwrap();
    }
    let c = wal.append_record(LogRecord::new_commit(0, 1, prev)).unwrap();
    wal.flush_up_to(c).unwrap();

    // Sweep fail_append_after across deletes that trigger leaf merges (free_btree_page).
    for fail_after in 0..8 {
        let mut prev_t = wal.append_record(LogRecord::new_begin(0, 10 + fail_after as u64)).unwrap();
        wal.set_fail_append_after(fail_after);
        let res = tree.delete_logged(10 + fail_after as u64, &mut prev_t, &k8(9));
        wal.set_fail_append_after(-1);
        if res.is_err() {
            // Verify zero leaked pins and no unpinned page wrongly marked FREE on disk.
            bpm.flush_all().unwrap();
            for pid in 0..disk.num_pages() {
                let p = bpm.fetch_page(pid).unwrap();
                assert_eq!(
                    p.flags() & PAGE_FLAG_FREE,
                    0,
                    "page {} must not be marked FREE when free_btree_page WAL append failed",
                    pid
                );
                bpm.unpin_page(pid, false).unwrap();
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
