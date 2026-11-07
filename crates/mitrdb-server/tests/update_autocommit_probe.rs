use mitrdb_exec::ExecutionResult;
use mitrdb_server::Server;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn summarize(label: &str, mut lat: Vec<Duration>, total: Duration) {
    let n = lat.len().max(1);
    lat.sort();
    println!(
        "LAT {label} n={n} total={total:?} avg={:.0}us p50={:?} p90={:?} p99={:?} max={:?}",
        total.as_micros() as f64 / n as f64,
        pct(&lat, 0.5),
        pct(&lat, 0.90),
        pct(&lat, 0.99),
        lat.last().copied().unwrap_or_default()
    );
}

#[test]
#[ignore]
fn probe_update_autocommit() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
    let ops: i64 = std::env::var("MITRDB_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(500).min(n);
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("updac_{}_{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, name TEXT)").unwrap();

    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }

    // keys spread over the whole table
    let step = (n / ops).max(1);
    let keys: Vec<i64> = (0..ops).map(|j| (j * step) % n).collect();

    let m0 = s.metrics_snapshot();
    let mut lat = Vec::with_capacity(keys.len());
    let t_all = Instant::now();
    for &k in &keys {
        let t = Instant::now();
        let r = s.execute_sql(&format!("UPDATE t SET g = 7777 WHERE id = {k}")).unwrap();
        lat.push(t.elapsed());
        assert!(matches!(r, ExecutionResult::Affected(1)), "update k={k} -> {r:?}");
    }
    let total = t_all.elapsed();
    let m1 = s.metrics_snapshot();
    summarize(&format!("update_pk_autocommit N={n}"), lat, total);
    println!(
        "METRICS update N={n} wal_appends={} wal_sync_count={} page_writes={}",
        m1.wal_appends - m0.wal_appends,
        m1.wal_sync_count - m0.wal_sync_count,
        m1.page_writes - m0.page_writes
    );

    // correctness: updated rows changed, others untouched
    let updated = match s.execute_sql("SELECT COUNT(*) FROM t WHERE g = 7777").unwrap() {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows[0][0]),
        o => panic!("bad {o:?}"),
    };
    println!("CHECK N={n} rows with g=7777: {updated} (expected {})", keys.len());

    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
