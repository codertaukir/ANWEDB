use mitrdb_server::{val, Server};
use std::path::PathBuf;
use std::time::Instant;

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
fn probe_delete_scaling() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("del_{}_{}", std::process::id(), n));
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
    let t = Instant::now();
    let pred = std::env::var("MITRDB_PRED").unwrap_or_else(|_| "all".to_string());
    let del = match pred.as_str() {
        "half_g" => s.table("t").delete().filter("g", "<", val(500)).run().unwrap(),
        "upper" => s.table("t").delete().filter("id", ">=", val(n / 2)).run().unwrap(),
        _ => s.table("t").delete().filter("id", ">=", val(0)).run().unwrap(),
    };
    let el = t.elapsed();
    let (w1, hwm1) = (wal_bytes(&d), status_mb("VmHWM"));
    println!(
        "DEL pred={pred} n={n} deleted={del} took={el:?} per_row={:.0}us wal_delta={:.1}MB wal_per_row={:.0}B hwm_before={hwm0}MB hwm_after={hwm1}MB",
        el.as_micros() as f64 / del as f64,
        (w1 - w0) as f64 / 1_048_576.0,
        (w1 - w0) as f64 / del as f64
    );
    assert_eq!(s.table("t").find().count().unwrap(), (n - del as i64) as usize);
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
