//! Small, reproducible single-process performance harness for MITRDB.
//!
//! Use fresh DB/WAL paths. This intentionally refuses to overwrite existing paths.
use std::env;
use std::path::Path;
use std::time::Instant;

use mitrdb_observe::{MetricsSnapshot, ProfileSnapshot, profile_snapshot, profiling_enabled};
use mitrdb_server::{PhysicalPlan, Server, Value};

const PHASE_NAMES: &[&str] = &[
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
];

fn percentile(sorted: &[u128], p: usize) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * p).div_ceil(100);
    sorted[index.min(sorted.len() - 1)]
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn print_latency(label: &str, samples_ns: &mut [u128]) {
    if samples_ns.is_empty() {
        println!("{label}: no samples");
        return;
    }
    samples_ns.sort_unstable();
    println!(
        "{label}: n={} p50={:.3}ms p95={:.3}ms p99={:.3}ms max={:.3}ms",
        samples_ns.len(),
        percentile(samples_ns, 50) as f64 / 1_000_000.0,
        percentile(samples_ns, 95) as f64 / 1_000_000.0,
        percentile(samples_ns, 99) as f64 / 1_000_000.0,
        *samples_ns.last().unwrap() as f64 / 1_000_000.0,
    );
}

fn print_profile_delta(label: &str, delta: &ProfileSnapshot) {
    if !profiling_enabled() {
        return;
    }
    println!("\nProfile Phase Breakdown ({label}):");
    println!(
        "{:<24} | {:>10} | {:>14} | {:>12}",
        "Phase", "Count", "Total (ms)", "Max (ms)"
    );
    println!("{:-<24}-+-{:-<10}-+-{:-<14}-+-{:-<12}", "", "", "", "");
    for (idx, p) in delta.phases.iter().enumerate() {
        if p.count > 0 {
            let name = PHASE_NAMES.get(idx).unwrap_or(&"Unknown");
            let total_ms = p.total_ns as f64 / 1_000_000.0;
            let max_ms = p.max_ns as f64 / 1_000_000.0;
            println!(
                "{:<24} | {:>10} | {:>14.3} | {:>12.3}",
                name, p.count, total_ms, max_ms
            );
        }
    }
}

fn print_metrics(before: MetricsSnapshot, after: MetricsSnapshot) {
    println!("\nMetrics delta (isolated benchmark process recommended):");
    println!(
        "  WAL syncs: {}",
        delta(after.wal_sync_count, before.wal_sync_count)
    );
    println!(
        "  WAL sync time total/max: {:.3}ms / {:.3}ms",
        delta(after.wal_sync_total_ns, before.wal_sync_total_ns) as f64 / 1_000_000.0,
        delta(after.wal_sync_max_ns, before.wal_sync_max_ns) as f64 / 1_000_000.0,
    );
    println!(
        "  txn mutex wait count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(after.txn_mutex_wait_count, before.txn_mutex_wait_count),
        delta(
            after.txn_mutex_wait_total_ns,
            before.txn_mutex_wait_total_ns
        ) as f64
            / 1_000_000.0,
        delta(after.txn_mutex_wait_max_ns, before.txn_mutex_wait_max_ns) as f64 / 1_000_000.0,
    );
    println!(
        "  B-tree root wait count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(after.btree_root_wait_count, before.btree_root_wait_count),
        delta(
            after.btree_root_wait_total_ns,
            before.btree_root_wait_total_ns
        ) as f64
            / 1_000_000.0,
        delta(after.btree_root_wait_max_ns, before.btree_root_wait_max_ns) as f64 / 1_000_000.0,
    );
    println!(
        "  WAL flush lock wait count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(
            after.wal_flush_lock_wait_count,
            before.wal_flush_lock_wait_count
        ),
        delta(
            after.wal_flush_lock_wait_total_ns,
            before.wal_flush_lock_wait_total_ns
        ) as f64
            / 1_000_000.0,
        delta(
            after.wal_flush_lock_wait_max_ns,
            before.wal_flush_lock_wait_max_ns
        ) as f64
            / 1_000_000.0,
    );
    println!(
        "  WAL flush total count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(after.wal_flush_total_count, before.wal_flush_total_count),
        delta(after.wal_flush_total_ns, before.wal_flush_total_ns) as f64 / 1_000_000.0,
        delta(after.wal_flush_max_ns, before.wal_flush_max_ns) as f64 / 1_000_000.0,
    );
    println!(
        "  txn commit lock hold count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(
            after.txn_commit_lock_hold_count,
            before.txn_commit_lock_hold_count
        ),
        delta(
            after.txn_commit_lock_hold_total_ns,
            before.txn_commit_lock_hold_total_ns
        ) as f64
            / 1_000_000.0,
        delta(
            after.txn_commit_lock_hold_max_ns,
            before.txn_commit_lock_hold_max_ns
        ) as f64
            / 1_000_000.0,
    );
    println!(
        "  MVCC scan count/total/max: {} / {:.3}ms / {:.3}ms",
        delta(after.mvcc_scan_count, before.mvcc_scan_count),
        delta(after.mvcc_scan_total_ns, before.mvcc_scan_total_ns) as f64 / 1_000_000.0,
        delta(after.mvcc_scan_max_ns, before.mvcc_scan_max_ns) as f64 / 1_000_000.0,
    );
    println!(
        "  MVCC entries inspected: {}",
        delta(after.mvcc_entries_inspected, before.mvcc_entries_inspected)
    );
    println!(
        "  queries / seq scans / index scans: {} / {} / {}",
        delta(after.queries_executed, before.queries_executed),
        delta(after.seq_scans, before.seq_scans),
        delta(after.index_scans, before.index_scans),
    );
    println!(
        "  page reads/writes: {} / {}; buffer hits/misses/evictions: {} / {} / {}",
        delta(after.page_reads, before.page_reads),
        delta(after.page_writes, before.page_writes),
        delta(after.buffer_hits, before.buffer_hits),
        delta(after.buffer_misses, before.buffer_misses),
        delta(after.buffer_evictions, before.buffer_evictions),
    );
}

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 || args.len() > 7 {
        return Err(
            "Usage: cargo run --release -p mitrdb-server --example perf_bench -- <fresh-db-path> <fresh-wal-path> [rows=1000] [read-scans=10] [autocommit|batch] [warmup-scans=1] [pool-size=4096]".into(),
        );
    }
    let db_path = &args[0];
    let wal_path = &args[1];
    let rows: usize = args
        .get(2)
        .map_or(Ok(1_000), |v| v.parse())
        .map_err(|e| format!("invalid rows: {e}"))?;
    let read_scans: usize = args
        .get(3)
        .map_or(Ok(10), |v| v.parse())
        .map_err(|e| format!("invalid read-scans: {e}"))?;
    let mode = args.get(4).map(String::as_str).unwrap_or("autocommit");
    let warmup_scans: usize = args
        .get(5)
        .map_or(Ok(1), |v| v.parse())
        .map_err(|e| format!("invalid warmup-scans: {e}"))?;
    let pool_size: usize = args
        .get(6)
        .map_or(Ok(4096), |v| v.parse())
        .map_err(|e| format!("invalid pool-size: {e}"))?;

    if rows == 0 || read_scans == 0 {
        return Err("rows and read-scans must both be greater than zero".into());
    }
    if pool_size == 0 {
        return Err("pool-size must be greater than zero".into());
    }
    if mode != "autocommit" && mode != "batch" {
        return Err("mode must be either 'autocommit' or 'batch'".into());
    }
    if Path::new(db_path).exists() || Path::new(wal_path).exists() {
        return Err("Refusing to use an existing DB/WAL path. Choose two fresh paths so existing data is untouched.".into());
    }

    let (server, _recovery) =
        Server::open(db_path, wal_path, pool_size).map_err(|e| format!("open database: {e:?}"))?;
    server
        .execute_sql("CREATE TABLE bench (id INT PRIMARY KEY, value INT);")
        .map_err(|e| format!("create table: {e:?}"))?;

    // Take the baseline after startup/recovery and schema creation so those costs are excluded.
    let before = server.metrics_snapshot();
    let overall_start = Instant::now();
    let bench_schema = server
        .get_table_schema("bench")
        .map_err(|e| format!("get schema: {e:?}"))?
        .ok_or_else(|| "Table bench not found".to_string())?;

    let profile_before = profile_snapshot();
    let insert_start = Instant::now();
    let mut insert_samples = Vec::with_capacity(rows);
    if mode == "batch" {
        server
            .execute_sql("BEGIN;")
            .map_err(|e| format!("begin batch transaction: {e:?}"))?;
    }
    for id in 1..=rows {
        let plan = PhysicalPlan::Insert {
            schema: bench_schema.clone(),
            values: vec![
                Value::Int64(id as i64),
                Value::Int64(id.saturating_mul(10) as i64),
            ],
        };
        let start = Instant::now();
        server
            .execute_plan(plan)
            .map_err(|e| format!("insert row {id}: {e:?}"))?;
        insert_samples.push(start.elapsed().as_nanos());
    }
    if mode == "batch" {
        server
            .execute_sql("COMMIT;")
            .map_err(|e| format!("commit batch transaction: {e:?}"))?;
    }
    let insert_elapsed = insert_start.elapsed();
    let profile_after = profile_snapshot();
    let profile_delta = profile_after.delta_since(profile_before);

    // Warm-up scans intentionally populate the database's buffer pool before
    // measured scans. Their latency is excluded from read_samples and therefore
    // from measured scan percentiles. Set warmup-scans=0 to measure cold-cache
    // behavior. This is benchmark-only; it does not change server startup behavior.
    let warmup_start = Instant::now();
    for _ in 0..warmup_scans {
        let _result = server
            .execute_sql("SELECT * FROM bench;")
            .map_err(|e| format!("warm-up full table scan: {e:?}"))?;
    }
    let warmup_elapsed = warmup_start.elapsed();

    let mut read_samples = Vec::with_capacity(read_scans);
    for _ in 0..read_scans {
        let start = Instant::now();
        let _result = server
            .execute_sql("SELECT * FROM bench;")
            .map_err(|e| format!("full table scan: {e:?}"))?;
        read_samples.push(start.elapsed().as_nanos());
    }
    let workload_elapsed = overall_start.elapsed();
    let after = server.metrics_snapshot();

    println!("MITRDB single-process benchmark");
    println!("  transaction mode: {mode}");
    println!("  rows inserted: {rows}");
    println!(
        "  buffer pool size: {pool_size} frames ({:.2} MB)",
        (pool_size * 4096) as f64 / (1024.0 * 1024.0)
    );
    println!("  warm-up scans (excluded from latency percentiles): {warmup_scans}");
    println!(
        "  warm-up elapsed: {:.3}ms",
        warmup_elapsed.as_secs_f64() * 1_000.0
    );
    println!("  measured full-table read scans: {read_scans}");
    let total_insert_nanos: u128 = insert_samples.iter().sum();
    let pure_insert_secs = (total_insert_nanos as f64) / 1_000_000_000.0;
    println!(
        "  raw insert execution throughput: {:.2} rows/sec",
        rows as f64 / pure_insert_secs
    );
    println!(
        "  durable insert throughput (incl. COMMIT): {:.2} rows/sec",
        rows as f64 / insert_elapsed.as_secs_f64()
    );
    println!(
        "  insert phase elapsed: {:.3}s",
        insert_elapsed.as_secs_f64()
    );
    println!(
        "  workload elapsed (insert + scans): {:.3}s",
        workload_elapsed.as_secs_f64()
    );
    if mode == "batch" {
        print_latency(
            "  per-insert execution latency (commit excluded)",
            &mut insert_samples,
        );
        println!(
            "  note: throughput includes the final durable COMMIT; per-insert samples do not."
        );
    } else {
        print_latency("  per-insert latency", &mut insert_samples);
    }
    print_latency("  full-scan latency", &mut read_samples);
    print_metrics(before, after);
    print_profile_delta("Insert Phase", &profile_delta);
    println!(
        "\nNote: metrics delta includes warm-up activity; measured scan latency percentiles exclude warm-up scans."
    );
    println!("Note: use warmup-scans=0 to measure cold-cache behavior.");
    println!("Note: single-process, single-thread baseline; not a concurrency benchmark.");
    println!("DB path: {db_path}; WAL path: {wal_path}");
    Ok(())
}
