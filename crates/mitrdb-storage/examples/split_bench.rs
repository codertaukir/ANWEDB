// A/B bench for the split path: no fsync, insert_logged loop. args: dir seq|rand n
use std::sync::Arc;
use std::time::Instant;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let mode = a[2].as_str();
    let n: u64 = a[3].parse().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 8192));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = BPlusTree::create_logged(bpm, Some(wal), 64, 1, &mut prev).unwrap();
    let keys: Vec<u64> = (0..n)
        .map(|i| if mode == "rand" { (i * 7919) % n } else { i })
        .collect();
    let t0 = Instant::now();
    for k in &keys {
        tree.insert_logged(1, &mut prev, &k.to_be_bytes(), &[7u8; 8])
            .unwrap();
    }
    let el = t0.elapsed().as_nanos() as f64 / n as f64;
    println!(
        "SPLITBENCH mode={mode} ns_per_insert={el:.1} root={}",
        tree.root_page_id()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
