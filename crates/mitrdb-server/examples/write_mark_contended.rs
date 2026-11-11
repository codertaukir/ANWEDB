//! Probe: contended cost of TxnManager::write_mark. T threads, each with its own explicit txn,
//! all hammering write_mark (worst case: pure `txns` mutex contention, no real writes).
//! Timing is per batch of B calls, reported as ns/call (median / p99 over batches). Use fresh paths.
use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use mitrdb_server::Server;

const B: u64 = 1000;
const WARM_BATCHES: u64 = 100;
const BATCHES: u64 = 2000;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err(
            "Usage: write_mark_contended <fresh-db-path> <fresh-wal-path> <threads>".into(),
        );
    }
    if Path::new(&args[0]).exists() || Path::new(&args[1]).exists() {
        return Err("Refusing to use an existing DB/WAL path".into());
    }
    let t: usize = args[2].parse().map_err(|e| format!("threads: {e}"))?;
    let (server, _) = Server::open(&args[0], &args[1], 256).map_err(|e| format!("open: {e:?}"))?;
    let tm = server.txn_manager().clone();
    let mut tids = Vec::new();
    for _ in 0..t {
        tids.push(tm.begin().map_err(|e| format!("begin: {e:?}"))?);
    }
    let barrier = Arc::new(Barrier::new(t + 1));
    let mut handles = Vec::new();
    for &tid in &tids {
        let tm = tm.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || -> Result<Vec<f64>, String> {
            for _ in 0..WARM_BATCHES * B {
                black_box(
                    tm.write_mark(black_box(tid))
                        .map_err(|e| format!("mark: {e:?}"))?,
                );
            }
            barrier.wait();
            let mut per_call = Vec::with_capacity(BATCHES as usize);
            for _ in 0..BATCHES {
                let s = Instant::now();
                for _ in 0..B {
                    black_box(
                        tm.write_mark(black_box(tid))
                            .map_err(|e| format!("mark: {e:?}"))?,
                    );
                }
                per_call.push(s.elapsed().as_nanos() as f64 / B as f64);
            }
            Ok(per_call)
        }));
    }
    barrier.wait();
    let start = Instant::now();
    let mut all: Vec<f64> = Vec::new();
    for h in handles {
        all.extend(h.join().map_err(|_| "thread panicked".to_string())??);
    }
    let wall = start.elapsed().as_secs_f64();
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = all[all.len() / 2];
    let p99 = all[all.len() * 99 / 100];
    let total_calls = t as u64 * BATCHES * B;
    println!(
        "threads={t} median={med:.1} ns/call p99={p99:.1} ns/call wall={wall:.2}s throughput={:.2} Mcalls/s",
        total_calls as f64 / wall / 1e6
    );
    for tid in tids {
        let _ = tm.abort(tid);
    }
    Ok(())
}
