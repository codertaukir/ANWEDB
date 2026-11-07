use mitrdb_server::{val, Server};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn mark(name: &str) {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    println!("MARK {name} {t:.6}");
}

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
    let med = pct(&lat, 0.5);
    let slow = lat.iter().filter(|d| **d > med * 2).count();
    println!(
        "LAT {label} n={n} total={total:?} avg={:.0}us p50={:?} p90={:?} p99={:?} max={:?} slower_than_2x_median={slow}",
        total.as_micros() as f64 / n as f64,
        med,
        pct(&lat, 0.90),
        pct(&lat, 0.99),
        lat.last().copied().unwrap_or_default()
    );
}

fn dir_sizes(d: &PathBuf) {
    let mut v: Vec<(String, u64)> = std::fs::read_dir(d)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let m = e.metadata().ok()?;
            m.is_file().then(|| (e.file_name().to_string_lossy().to_string(), m.len()))
        })
        .collect();
    v.sort();
    println!("FILES {}", v.iter().map(|(n, s)| format!("{n}={s}B")).collect::<Vec<_>>().join(" "));
}

#[test]
#[ignore]
fn probe_delete_autocommit() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("delac_{}_{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    s.table("t").sync("id int primary key, g int, name text").unwrap();

    mark("load_start");
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }
    mark("load_end");
    dir_sizes(&d);

    let m0 = s.metrics_snapshot();
    let mut lat = Vec::with_capacity(n as usize);
    mark("delete_start");
    let t_all = Instant::now();
    for k in 0..n {
        let t = Instant::now();
        let c = s.table("t").delete().filter("id", "=", val(k)).run().unwrap();
        lat.push(t.elapsed());
        assert_eq!(c, 1, "row {k} not deleted");
    }
    let del_total = t_all.elapsed();
    mark("delete_end");
    let m1 = s.metrics_snapshot();
    summarize("delete_autocommit", lat, del_total);
    println!(
        "METRICS delete wal_appends={} wal_flushes={} wal_sync_count={} wal_sync_total_ms={} page_writes={} evictions={}",
        m1.wal_appends - m0.wal_appends,
        m1.wal_flushes - m0.wal_flushes,
        m1.wal_sync_count - m0.wal_sync_count,
        (m1.wal_sync_total_ns - m0.wal_sync_total_ns) / 1_000_000,
        m1.page_writes - m0.page_writes,
        m1.buffer_evictions - m0.buffer_evictions
    );
    assert_eq!(s.table("t").find().count().unwrap(), 0);
    dir_sizes(&d);

    let m2 = s.metrics_snapshot();
    let mut lat = Vec::with_capacity(n as usize);
    mark("reinsert_start");
    let t_all = Instant::now();
    for k in 0..n {
        let t = Instant::now();
        s.execute_sql(&format!("INSERT INTO t VALUES ({k},{},'user_{k}')", k % 1000)).unwrap();
        lat.push(t.elapsed());
    }
    let ins_total = t_all.elapsed();
    mark("reinsert_end");
    let m3 = s.metrics_snapshot();
    summarize("reinsert_autocommit", lat, ins_total);
    println!(
        "METRICS reinsert wal_appends={} wal_flushes={} wal_sync_count={} wal_sync_total_ms={} page_writes={} evictions={}",
        m3.wal_appends - m2.wal_appends,
        m3.wal_flushes - m2.wal_flushes,
        m3.wal_sync_count - m2.wal_sync_count,
        (m3.wal_sync_total_ns - m2.wal_sync_total_ns) / 1_000_000,
        m3.page_writes - m2.page_writes,
        m3.buffer_evictions - m2.buffer_evictions
    );
    assert_eq!(s.table("t").find().count().unwrap(), n as usize);
    dir_sizes(&d);

    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
