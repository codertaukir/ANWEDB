// Probe (ignored): B+tree depth / page count vs number of keys at a given max_keys.
// Run: MAXK=64 MITRDB_DIR=$HOME/x cargo test --release -p mitrdb-storage --test btree_depth_probe -- --ignored --nocapture
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn depth(tree: &BPlusTree) -> (usize, usize) {
    let mut node = tree.read_node(tree.root_page_id()).unwrap();
    let root_keys = node.keys.len();
    let mut d = 1usize;
    while !node.is_leaf {
        let c = node.children[0];
        node = tree.read_node(c).unwrap();
        d += 1;
    }
    (d, root_keys)
}

#[test]
#[ignore]
fn btree_depth_probe() {
    let maxk: u16 = std::env::var("MAXK").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let base = std::env::var("MITRDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let marks: [u64; 6] = [256, 2048, 4000, 8000, 16000, 40000];
    for mode in ["seq", "rand"] {
        let dir = base.join(format!("depth_{}_{}", std::process::id(), mode));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 4096));
        let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
        let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        let tree = BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), maxk, 1, &mut prev).unwrap();
        let mut done = 0u64;
        for &m in marks.iter() {
            while done < m {
                let k = if mode == "seq" { done } else { (done.wrapping_mul(2654435761)) & 0xFFFF_FFFF };
                tree.insert_logged(1, &mut prev, &key(k), &[7u8; 8]).unwrap();
                done += 1;
            }
            let (d, rk) = depth(&tree);
            let pages = tree.collect_all_pages().unwrap().len();
            println!("DEPTH maxk={maxk} mode={mode} keys={m} depth={d} root_keys={rk} pages={pages}");
        }
        drop(tree);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
