//! MITRDB resource and concurrency benchmark.
//!
//! Measures this process's memory/CPU while the database grows and while
//! end-to-end SQL point-read workloads run. Use fresh DB/WAL paths.
//!
//! Usage:
//! cargo run --release -p mitrdb-server --example resource_bench --
//!   <fresh-db-path> <fresh-wal-path> [large-rows=250000] [buffer-pages=256]
//!   [read-ops-per-worker=500] [rounds=3]
//!
//! This benchmark reports process resource use, not just buffer-pool memory.
//! The buffer pool is configured in pages via `buffer-pages`. Data insertion
//! is chunked into 1,000-row transactions so one enormous transaction does not
//! dominate the memory result. `large-rows` may be any positive row count;
//! only stage boundaries that fit in the requested dataset are reported.

use std::env;
use std::path::Path;
use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use mitrdb_server::Server;

const MIB: f64 = 1024.0 * 1024.0;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const SETTLE_TIME: Duration = Duration::from_secs(1);
const SEED_CHUNK_ROWS: usize = 1_000;
const CONCURRENCY_LEVELS: [usize; 4] = [1, 4, 8, 16];

const PROFILE_PHASE_NAMES: [&str; 30] = [
    "parse_plan",
    "plan_execution",
    "insert_execution",
    "txn_put",
    "txn_commit",
    "wal_append",
    "wal_flush",
    "lock_acquire",
    "mvcc_mutex_wait",
    "txn_mutex_wait",
    "btree_mutation",
    "version_chain_mutation",
    "btree_traversal",
    "btree_page_write",
    "btree_leaf_split",
    "btree_internal_split",
    "buffer_pool_fetch",
    "buffer_pool_disk_read",
    "split_mid_selection",
    "split_page_allocation",
    "split_redistribution",
    "split_parent_update",
    "lock_mutex_wait",
    "lock_table_scan",
    "lock_bookkeeping",
    "lock_condition_wait",
    "buffer_pool_new_page",
    "buffer_pool_victim_select",
    "buffer_pool_disk_allocate",
    "buffer_pool_unpin",
];

fn print_profile_delta(
    before: mitrdb_observe::ProfileSnapshot,
    after: mitrdb_observe::ProfileSnapshot,
) {
    if !mitrdb_observe::profiling_enabled() {
        return;
    }
    let delta = after.delta_since(before);
    println!("  Insert profiling (phase timers are inclusive; do not sum them):");
    for (name, phase) in PROFILE_PHASE_NAMES.iter().zip(delta.phases.iter()) {
        if phase.count == 0 {
            continue;
        }
        let total_ms = phase.total_ns as f64 / 1_000_000.0;
        let avg_ms = phase.total_ns as f64 / phase.count as f64 / 1_000_000.0;
        println!(
            "    {name}: calls={} total={total_ms:.2}ms avg={avg_ms:.3}ms",
            phase.count
        );
    }
}

#[derive(Clone, Copy, Debug)]
struct ProcessSample {
    rss_bytes: u64,
    private_or_virtual_bytes: u64,
    process_peak_rss_bytes: u64,
    cpu_time_ns: u64,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    at: Instant,
    process: ProcessSample,
    cpu_percent: f64,
}

mod process_stats {
    use super::ProcessSample;
    use std::fs;

    unsafe extern "C" {
        fn sysconf(name: i32) -> i64;
    }

    fn status_kib_value(status: &str, key: &str) -> Option<u64> {
        status.lines().find_map(|line| {
            let (name, rest) = line.split_once(':')?;
            if name != key {
                return None;
            }
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
    }

    pub fn snapshot() -> Result<ProcessSample, String> {
        let status = fs::read_to_string("/proc/self/status")
            .map_err(|e| format!("read /proc/self/status: {e}"))?;
        let rss = status_kib_value(&status, "VmRSS")
            .ok_or_else(|| "VmRSS missing from /proc/self/status".to_string())?;
        let virtual_size = status_kib_value(&status, "VmSize").unwrap_or(rss);
        let peak_rss = status_kib_value(&status, "VmHWM").unwrap_or(rss);

        let stat = fs::read_to_string("/proc/self/stat")
            .map_err(|e| format!("read /proc/self/stat: {e}"))?;
        let close = stat
            .rfind(')')
            .ok_or_else(|| "invalid /proc/self/stat".to_string())?;
        let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
        // After the comm field, fields[0] is process state (field 3);
        // utime/stime are fields 14/15, hence indexes 11/12 here.
        let user_ticks: u64 = fields
            .get(11)
            .ok_or("utime missing")?
            .parse()
            .map_err(|_| "invalid utime")?;
        let system_ticks: u64 = fields
            .get(12)
            .ok_or("stime missing")?
            .parse()
            .map_err(|_| "invalid stime")?;
        let ticks_per_second = unsafe { sysconf(2) };
        if ticks_per_second <= 0 {
            return Err("sysconf(_SC_CLK_TCK) failed".into());
        }
        let cpu_time_ns = (user_ticks.saturating_add(system_ticks) as u128 * 1_000_000_000u128
            / ticks_per_second as u128) as u64;

        Ok(ProcessSample {
            rss_bytes: rss * 1024,
            private_or_virtual_bytes: virtual_size * 1024,
            process_peak_rss_bytes: peak_rss * 1024,
            cpu_time_ns,
        })
    }
}

struct Sampler {
    samples: Arc<Mutex<Vec<Sample>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Sampler {
    fn start() -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_samples = Arc::clone(&samples);
        let thread_stop = Arc::clone(&stop);

        let worker = thread::spawn(move || {
            let mut previous: Option<(Instant, u64)> = None;
            while !thread_stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                if let Ok(process) = process_stats::snapshot() {
                    let cpu_percent = previous.map_or(0.0, |(previous_at, previous_cpu)| {
                        let wall_ns = now.duration_since(previous_at).as_nanos().max(1) as f64;
                        let cpu_ns = process.cpu_time_ns.saturating_sub(previous_cpu) as f64;
                        // 100% means one fully occupied logical CPU. Multi-threaded
                        // work may legitimately exceed 100%.
                        (cpu_ns / wall_ns * 100.0).max(0.0)
                    });
                    thread_samples.lock().unwrap().push(Sample {
                        at: now,
                        process,
                        cpu_percent,
                    });
                    previous = Some((now, process.cpu_time_ns));
                }
                thread::sleep(SAMPLE_INTERVAL);
            }
        });

        Self {
            samples,
            stop,
            worker: Some(worker),
        }
    }

    fn mark(&self) -> usize {
        self.samples.lock().unwrap().len()
    }

    fn report_since(&self, label: &str, mark: usize, phase_end: Instant) {
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            println!("  {label}: resource samples unavailable");
            return;
        }

        let start_index = mark.min(samples.len());
        // Exclude samples captured during the post-workload settling period.
        let phase: Vec<&Sample> = samples[start_index..]
            .iter()
            .filter(|sample| sample.at <= phase_end)
            .collect();
        if phase.is_empty() {
            println!("  {label}: no samples captured; increase workload duration");
            return;
        }

        let first = phase.first().unwrap();
        let last = phase.last().unwrap();
        let sampled_peak_rss = phase
            .iter()
            .map(|s| s.process.rss_bytes)
            .max()
            .unwrap_or(last.process.rss_bytes);
        let sampled_peak_private = phase
            .iter()
            .map(|s| s.process.private_or_virtual_bytes)
            .max()
            .unwrap_or(last.process.private_or_virtual_bytes);
        let avg_cpu = phase.iter().map(|s| s.cpu_percent).sum::<f64>() / phase.len() as f64;
        let peak_cpu = phase.iter().map(|s| s.cpu_percent).fold(0.0, f64::max);
        let elapsed = last.at.saturating_duration_since(first.at).as_secs_f64();

        println!(
            "  {label}: samples={} window={:.2}s RSS start/end/peak={:.1}/{:.1}/{:.1} MiB; private-or-virtual end/peak={:.1}/{:.1} MiB; process peak RSS={:.1} MiB; CPU avg/max={:.1}/{:.1}%",
            phase.len(),
            elapsed,
            first.process.rss_bytes as f64 / MIB,
            last.process.rss_bytes as f64 / MIB,
            sampled_peak_rss as f64 / MIB,
            last.process.private_or_virtual_bytes as f64 / MIB,
            sampled_peak_private as f64 / MIB,
            last.process.process_peak_rss_bytes as f64 / MIB,
            avg_cpu,
            peak_cpu,
        );
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn seed_rows(server: &Server, first_id: usize, last_id: usize) -> Result<(), String> {
    if first_id > last_id {
        return Ok(());
    }

    let mut chunk_start = first_id;
    while chunk_start <= last_id {
        let chunk_end = chunk_start.saturating_add(SEED_CHUNK_ROWS - 1).min(last_id);
        server
            .execute_sql("BEGIN;")
            .map_err(|e| format!("begin seed transaction at row {chunk_start}: {e:?}"))?;

        let mut failure = None;
        for id in chunk_start..=chunk_end {
            let sql = format!(
                "INSERT INTO bench VALUES ({id}, {});",
                id.saturating_mul(10)
            );
            if let Err(error) = server.execute_sql(&sql) {
                failure = Some(format!("insert row {id}: {error:?}"));
                break;
            }
        }

        if let Some(error) = failure {
            let _ = server.execute_sql("ROLLBACK;");
            return Err(error);
        }
        server
            .execute_sql("COMMIT;")
            .map_err(|e| format!("commit seed transaction ending at row {chunk_end}: {e:?}"))?;

        if chunk_end == usize::MAX {
            break;
        }
        chunk_start = chunk_end + 1;
    }
    Ok(())
}

fn report_seed_stage(
    server: &Server,
    sampler: &Sampler,
    label: &str,
    first_id: usize,
    last_id: usize,
) -> Result<(), String> {
    let mark = sampler.mark();
    let profile_before = mitrdb_observe::profile_snapshot();
    let start = Instant::now();
    seed_rows(server, first_id, last_id)?;
    let elapsed = start.elapsed();
    let profile_after = mitrdb_observe::profile_snapshot();
    let phase_end = Instant::now();
    thread::sleep(SETTLE_TIME);
    println!(
        "Seed stage {label}: inserted={} elapsed={:.2}s throughput={:.1} rows/sec",
        if first_id <= last_id {
            last_id - first_id + 1
        } else {
            0
        },
        elapsed.as_secs_f64(),
        if elapsed.as_secs_f64() > 0.0 {
            (if first_id <= last_id {
                last_id - first_id + 1
            } else {
                0
            }) as f64
                / elapsed.as_secs_f64()
        } else {
            0.0
        },
    );
    print_profile_delta(profile_before, profile_after);
    sampler.report_since(label, mark, phase_end);
    Ok(())
}

fn run_read_round(
    server: Arc<Server>,
    rows_available: usize,
    workers: usize,
    ops_per_worker: usize,
    round: usize,
    sampler: &Sampler,
) -> Result<(), String> {
    let sessions: Vec<_> = (0..workers).map(|_| server.create_session()).collect();
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);

    for (worker_id, session_id) in sessions.into_iter().enumerate() {
        let server = Arc::clone(&server);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<usize, String> {
            barrier.wait();
            for op in 0..ops_per_worker {
                let id = 1 + ((worker_id * ops_per_worker + op) % rows_available);
                let sql = format!("SELECT * FROM bench WHERE id = {id};");
                server
                    .execute_in_session(session_id, &sql)
                    .map_err(|e| format!("worker {worker_id} read {id}: {e:?}"))?;
            }
            server
                .close_session(session_id)
                .map_err(|e| format!("close worker session {worker_id}: {e:?}"))?;
            Ok(ops_per_worker)
        }));
    }

    // Start timing only after every worker has been created and is waiting.
    let mark = sampler.mark();
    let start = Instant::now();
    barrier.wait();

    let mut completed = 0usize;
    let mut first_error = None;
    for (worker_id, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(count)) => completed += count,
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(format!("worker {worker_id} panicked"));
                }
            }
        }
    }
    let elapsed = start.elapsed();
    let phase_end = Instant::now();
    thread::sleep(SETTLE_TIME);

    if let Some(error) = first_error {
        return Err(error);
    }

    println!(
        "Concurrent point-read: workers={workers} round={round} ops={} elapsed={:.3}s throughput={:.1} ops/sec",
        completed,
        elapsed.as_secs_f64(),
        completed as f64 / elapsed.as_secs_f64(),
    );
    sampler.report_since(
        &format!("workers={workers}, round={round}"),
        mark,
        phase_end,
    );
    Ok(())
}

fn stage_targets(large_rows: usize) -> Vec<usize> {
    let mut targets = vec![1_000.min(large_rows), 100_000.min(large_rows), large_rows];
    targets.dedup();
    targets
}

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 || args.len() > 6 {
        return Err(
            "Usage: cargo run --release -p mitrdb-server --example resource_bench -- <fresh-db-path> <fresh-wal-path> [large-rows=250000] [buffer-pages=256] [read-ops-per-worker=500] [rounds=3]".into(),
        );
    }

    let db_path = &args[0];
    let wal_path = &args[1];
    let large_rows: usize = args
        .get(2)
        .map_or(Ok(250_000), |v| v.parse())
        .map_err(|e| format!("invalid large-rows: {e}"))?;
    let buffer_pages: usize = args
        .get(3)
        .map_or(Ok(256), |v| v.parse())
        .map_err(|e| format!("invalid buffer-pages: {e}"))?;
    let ops_per_worker: usize = args
        .get(4)
        .map_or(Ok(500), |v| v.parse())
        .map_err(|e| format!("invalid read-ops-per-worker: {e}"))?;
    let rounds: usize = args
        .get(5)
        .map_or(Ok(3), |v| v.parse())
        .map_err(|e| format!("invalid rounds: {e}"))?;

    if large_rows == 0 {
        return Err("large-rows must be greater than zero".into());
    }
    if buffer_pages == 0 || ops_per_worker == 0 || rounds == 0 {
        return Err(
            "buffer-pages, read-ops-per-worker and rounds must be greater than zero".into(),
        );
    }
    if Path::new(db_path).exists() || Path::new(wal_path).exists() {
        return Err("Refusing to use an existing DB/WAL path. Choose fresh paths so existing data is untouched.".into());
    }

    let startup_start = Instant::now();
    let (server, _recovery) = Server::open(db_path, wal_path, buffer_pages)
        .map_err(|e| format!("open database: {e:?}"))?;
    let open_elapsed = startup_start.elapsed();
    server
        .execute_sql("CREATE TABLE bench (id INT PRIMARY KEY, value INT);")
        .map_err(|e| format!("create table: {e:?}"))?;

    let sampler = Sampler::start();
    thread::sleep(SETTLE_TIME);
    println!("MITRDB resource benchmark");
    println!("  startup/open elapsed: {:.3}s", open_elapsed.as_secs_f64());
    println!("  buffer-pool capacity: {buffer_pages} pages");
    let stages = stage_targets(large_rows);
    println!(
        "  row stages: {}",
        stages
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(" -> ")
    );
    println!(
        "  concurrent read ops/worker/round: {ops_per_worker}; rounds per worker count: {rounds}"
    );
    println!("  memory metrics are for the entire benchmark process, not only the buffer pool.");
    println!(
        "  insert profiling: {} (set MITRDB_PROFILE_INSERT=1 before launch to enable)",
        if mitrdb_observe::profiling_enabled() {
            "ON"
        } else {
            "OFF"
        }
    );
    println!("Idle after open/schema:");
    sampler.report_since("idle", 0, Instant::now());

    let mut rows_seeded = 0usize;
    for target_rows in stages {
        if target_rows <= rows_seeded {
            continue;
        }
        report_seed_stage(
            &server,
            &sampler,
            &format!("rows={target_rows}"),
            rows_seeded + 1,
            target_rows,
        )?;
        rows_seeded = target_rows;
    }

    let server = Arc::new(server);
    println!("\nConcurrent read load (end-to-end SQL point reads; fixed dataset; primary key):");
    for workers in CONCURRENCY_LEVELS {
        for round in 1..=rounds {
            run_read_round(
                Arc::clone(&server),
                large_rows,
                workers,
                ops_per_worker,
                round,
                &sampler,
            )?;
        }
    }

    println!("\nNotes:");
    println!(
        "  * CPU percentage is process CPU time / wall time; multi-threaded load can exceed 100%."
    );
    println!(
        "  * RSS is resident memory; Windows private bytes and Linux virtual size are shown as private-or-virtual."
    );
    println!(
        "  * Insert-stage memory naturally grows with the database. Use repeated fixed-dataset read rounds to inspect whether memory stabilizes under load."
    );
    println!(
        "  * Buffer-pool pages are bounded by the configured page count, but total process memory also includes indexes, MVCC state, SQL execution and allocator overhead."
    );
    println!("DB path: {db_path}; WAL path: {wal_path}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::stage_targets;

    #[test]
    fn small_dataset_uses_only_reachable_stages() {
        assert_eq!(stage_targets(10_000), vec![1_000, 10_000]);
    }

    #[test]
    fn original_large_dataset_stages_are_preserved() {
        assert_eq!(stage_targets(250_000), vec![1_000, 100_000, 250_000]);
    }

    #[test]
    fn dataset_at_one_hundred_thousand_has_no_duplicate_stage() {
        assert_eq!(stage_targets(100_000), vec![1_000, 100_000]);
    }

    #[test]
    fn tiny_positive_dataset_is_supported() {
        assert_eq!(stage_targets(500), vec![500]);
    }
}
