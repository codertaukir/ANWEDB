//! Probe: T threads, each its own explicit txn, unique keys, K puts, max_keys=64 (real default).
//! Prints ops/s and, if MITRDB_PROFILE_INSERT=1, the per-phase profile delta. Fresh dir required.
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

const NAMES: [&str; 30] = [
    "ParsePlan",
    "PlanExecution",
    "InsertExecution",
    "TxnPut",
    "TxnCommit",
    "WalAppend",
    "WalFlush",
    "LockAcquire",
    "MvccMutexWait",
    "TxnMutexWait",
    "BtreeMutation",
    "VersionChainMutation",
    "BtreeTraversal",
    "BtreePageWrite",
    "BtreeLeafSplit",
    "BtreeInternalSplit",
    "BufferPoolFetch",
    "BufferPoolDiskRead",
    "SplitMidSelection",
    "SplitPageAllocation",
    "SplitRedistribution",
    "SplitParentUpdate",
    "LockMutexWait",
    "LockTableScan",
    "LockBookkeeping",
    "LockConditionWait",
    "BufferPoolNewPage",
    "BufferPoolVictimSelect",
    "BufferPoolDiskAllocate",
    "BufferPoolUnpin",
];

fn main() -> Result<(), String> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() != 3 {
        return Err("Usage: write_put_prof <fresh-dir> <threads> <puts-per-thread>".into());
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
                mgr.put(tid, &tree, key, &val)
                    .map_err(|e| format!("put: {e:?}"))?;
            }
            Ok(())
        }));
    }
    let before = mitrdb_observe::profile_snapshot();
    barrier.wait();
    let start = Instant::now();
    for h in handles {
        h.join().map_err(|_| "thread panicked".to_string())??;
    }
    let wall = start.elapsed().as_secs_f64();
    let after = mitrdb_observe::profile_snapshot();
    let total = (t * k) as f64;
    println!(
        "RESULT threads={t} wall={wall:.3}s ops_per_s={:.0} ns_per_op_per_thread={:.0} profiling={}",
        total / wall,
        wall * 1e9 / k as f64,
        mitrdb_observe::profiling_enabled()
    );
    if mitrdb_observe::profiling_enabled() {
        let d = after.delta_since(before);
        for (i, p) in d.phases.iter().enumerate() {
            if p.count > 0 {
                println!(
                    "PHASE t={t} {:<24} count={:<8} total_ms={:<9.1} avg_ns={:<8} max_us={}",
                    NAMES[i],
                    p.count,
                    p.total_ns as f64 / 1e6,
                    p.total_ns / p.count,
                    p.max_ns / 1000
                );
            }
        }
    }
    for tid in tids {
        let _ = mgr.abort(tid);
    }
    drop(tree);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
