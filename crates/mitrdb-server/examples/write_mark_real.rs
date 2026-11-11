//! Probe (variant b): write_mark next to real puts. T threads, each with its own explicit txn and
//! unique keys, doing K puts. mode=mark calls write_mark before every put (as a statement would),
//! mode=nomark does only puts. Compare total ops/s. Use a fresh directory.
use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn main() -> Result<(), String> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() != 4 {
        return Err(
            "Usage: write_mark_real <fresh-dir> <threads> <mark|nomark> <puts-per-thread>".into(),
        );
    }
    let dir = Path::new(&a[0]);
    if dir.exists() {
        return Err("Refusing to use an existing directory".into());
    }
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let t: usize = a[1].parse().map_err(|e| format!("threads: {e}"))?;
    let mark = match a[2].as_str() {
        "mark" => true,
        "nomark" => false,
        _ => return Err("mode must be mark or nomark".into()),
    };
    let k: usize = a[3].parse().map_err(|e| format!("puts: {e}"))?;
    let disk = Arc::new(
        DiskManager::open_or_create(dir.join("t.db").as_path()).map_err(|e| format!("{e:?}"))?,
    );
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 4096));
    let wal = Arc::new(
        WalManager::open_or_create(dir.join("t.wal").as_path()).map_err(|e| format!("{e:?}"))?,
    );
    let tree = Arc::new(
        BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).map_err(|e| format!("{e:?}"))?,
    );
    let mgr = Arc::new(TxnManager::with_storage(wal.clone(), bpm.clone()));
    let mut tids = Vec::new();
    for _ in 0..t {
        tids.push(mgr.begin().map_err(|e| format!("begin: {e:?}"))?);
    }
    let barrier = Arc::new(Barrier::new(t + 1));
    let mut handles = Vec::new();
    for (ti, &tid) in tids.iter().enumerate() {
        let (mgr, tree, barrier) = (mgr.clone(), tree.clone(), barrier.clone());
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let keys: Vec<Vec<u8>> = (0..k)
                .map(|i| format!("t{ti:02}k{i:08}").into_bytes())
                .collect();
            let val = [7u8; 16];
            barrier.wait();
            for key in &keys {
                if mark {
                    black_box(mgr.write_mark(tid).map_err(|e| format!("mark: {e:?}"))?);
                }
                mgr.put(tid, &tree, key, &val)
                    .map_err(|e| format!("put: {e:?}"))?;
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
    let total = (t * k) as f64;
    println!(
        "RESULT threads={t} mode={} wall={wall:.3}s ops_per_s={:.0} ns_per_op_per_thread={:.0}",
        a[2],
        total / wall,
        wall * 1e9 / k as f64
    );
    for tid in tids {
        let _ = mgr.abort(tid);
    }
    drop(tree);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
