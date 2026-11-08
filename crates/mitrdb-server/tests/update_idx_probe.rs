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
fn probe_update_idx() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
    let ops: i64 = std::env::var("MITRDB_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(200).min(n).min(1000);
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("updidx_{}_{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, name TEXT)").unwrap();
    s.execute_sql("CREATE INDEX idx_g ON t(g)").unwrap();

    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }

    // mode 1: SELECT via index path (reference)
    let mut lat = Vec::new();
    let t_all = Instant::now();
    for j in 0..ops {
        let t = Instant::now();
        s.execute_sql(&format!("SELECT COUNT(*) FROM t WHERE g = {j}")).unwrap();
        lat.push(t.elapsed());
    }
    summarize(&format!("select_idx N={n}"), lat, t_all.elapsed());

    // mode 2: UPDATE by PK (reference)
    let step = (n / ops).max(1);
    let mut lat = Vec::new();
    let t_all = Instant::now();
    for j in 0..ops {
        let k = (j * step) % n;
        let t = Instant::now();
        let r = s.execute_sql(&format!("UPDATE t SET name = 'x' WHERE id = {k}")).unwrap();
        lat.push(t.elapsed());
        assert_eq!(format!("{r:?}"), "Affected(1)", "pk k={k}");
    }
    summarize(&format!("update_pk N={n}"), lat, t_all.elapsed());

    // mode 3: UPDATE by indexed column g (expected rows = #k<n with k%1000==j)
    let mut lat = Vec::new();
    let t_all = Instant::now();
    for j in 0..ops {
        let exp = (n - j + 999) / 1000;
        let t = Instant::now();
        let r = s.execute_sql(&format!("UPDATE t SET name = 'y' WHERE g = {j}")).unwrap();
        lat.push(t.elapsed());
        assert_eq!(format!("{r:?}"), format!("Affected({exp})"), "idx j={j}");
    }
    summarize(&format!("update_idx N={n}"), lat, t_all.elapsed());

    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
