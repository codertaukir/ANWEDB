//! Probe: T threads insert directly into ONE BPlusTree (no TxnManager), max_keys=64.
//! Isolates the tree's root write lock. Usage: tree_only_scale <fresh-dir> <threads> <puts-per-thread>
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn main() -> Result<(), String> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() != 3 {
        return Err("Usage: tree_only_scale <fresh-dir> <threads> <puts-per-thread>".into());
    }
    let dir = Path::new(&a[0]);
    if dir.exists() {
        return Err("Refusing to use an existing directory".into());
    }
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let t: usize = a[1].parse().map_err(|e| format!("threads: {e}"))?;
    let k: usize = a[2].parse().map_err(|e| format!("puts: {e}"))?;
    let disk = Arc::new(
        DiskManager::open_or_create(dir.join("t.db").as_path()).map_err(|e| format!("{e:?}"))?,
    );
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 4096));
    let wal = Arc::new(
        WalManager::open_or_create(dir.join("t.wal").as_path()).map_err(|e| format!("{e:?}"))?,
    );
    let tree = Arc::new(
        BPlusTree::create(bpm.clone(), Some(wal.clone()), 64).map_err(|e| format!("{e:?}"))?,
    );
    let barrier = Arc::new(Barrier::new(t + 1));
    let mut handles = Vec::new();
    for ti in 0..t {
        let (tree, wal, barrier) = (tree.clone(), wal.clone(), barrier.clone());
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let tid = ti as u64 + 1;
            let mut prev = wal
                .append_record(LogRecord::new_begin(0, tid))
                .map_err(|e| format!("{e:?}"))?;
            let keys: Vec<Vec<u8>> = (0..k)
                .map(|i| format!("t{ti:02}k{i:08}").into_bytes())
                .collect();
            let val = [7u8; 16];
            barrier.wait();
            for key in &keys {
                tree.insert_or_replace_logged(tid, &mut prev, key, &val)
                    .map_err(|e| format!("ins: {e:?}"))?;
            }
            Ok(())
        }));
    }
    barrier.wait();
    let start = Instant::now();
    for h in handles {
        h.join().map_err(|_| "thread panicked".to_string())??;
    }
    let wall = start.elapsed().as_secs_f64();
    println!(
        "TREE_ONLY threads={t} wall={wall:.3}s ops_per_s={:.0}",
        (t * k) as f64 / wall
    );
    drop(tree);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
