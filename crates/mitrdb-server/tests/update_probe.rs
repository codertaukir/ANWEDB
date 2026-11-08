// Probe: bulk UPDATE memory / WAL scaling (ignored; run with --ignored --nocapture).
use mitrdb_server::{val, Server};
use std::path::PathBuf;
use std::time::Instant;

const LONG: &str = "updated_row_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

fn wal_bytes(d: &PathBuf) -> u64 {
    std::fs::read_dir(d)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains("wal"))
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

fn status_mb(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with(key)).map(|l| l.to_string()))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

#[test]
#[ignore]
fn probe_update_scaling() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let set = std::env::var("MITRDB_SET").unwrap_or_else(|_| "g".to_string());
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("upd_{}_{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    s.table("t").sync("id int primary key, g int, name text").unwrap();
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }
    let (w0, hwm0) = (wal_bytes(&d), status_mb("VmHWM"));
    let m0 = s.metrics_snapshot();
    let t = Instant::now();
    let upd = match set.as_str() {
        "name" => s.table("t").update().set("name", val(LONG)).filter("id", ">=", val(0)).run().unwrap(),
        _ => s.table("t").update().set("g", val(7)).filter("id", ">=", val(0)).run().unwrap(),
    };
    let el = t.elapsed();
    let (w1, hwm1) = (wal_bytes(&d), status_mb("VmHWM"));
    let m1 = s.metrics_snapshot();
    println!(
        "UPD set={set} n={n} updated={upd} took={el:?} per_row={:.0}us wal_delta={:.1}MB wal_per_row={:.0}B hwm_before={hwm0}MB hwm_after={hwm1}MB delta={}MB",
        el.as_micros() as f64 / upd as f64,
        (w1 - w0) as f64 / 1_048_576.0,
        (w1 - w0) as f64 / upd as f64,
        hwm1.saturating_sub(hwm0)
    );
    let dd = |a: u64, b: u64| a.saturating_sub(b);
    println!(
        "UMETRICS appends={} flushes={} sync_count={} page_writes={} evictions={}",
        dd(m1.wal_appends, m0.wal_appends),
        dd(m1.wal_flushes, m0.wal_flushes),
        dd(m1.wal_sync_count, m0.wal_sync_count),
        dd(m1.page_writes, m0.page_writes),
        dd(m1.buffer_evictions, m0.buffer_evictions)
    );
    assert_eq!(upd as i64, n);
    assert_eq!(s.table("t").find().count().unwrap(), n as usize);
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
