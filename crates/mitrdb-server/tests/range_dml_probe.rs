// Probe (ignored): range predicates on an indexed column, UPDATE (full scan today) vs SELECT COUNT.
// Run: MITRDB_N=4000 cargo test --release -p mitrdb-server --test range_dml_probe -- --ignored --nocapture
use mitrdb_server::Server;
use std::path::PathBuf;
use std::time::Instant;

#[test]
#[ignore]
fn probe_range_dml() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(4000);
    let ops: i64 = std::env::var("MITRDB_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let base = std::env::var("MITRDB_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("rngdml_{}_{}", std::process::id(), n));
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
    for pred in ["g > 5000", "g > 990", "g >= 500", "g >= 0"] {
        let t = Instant::now();
        for _ in 0..ops {
            s.execute_sql(&format!("SELECT COUNT(*) FROM t WHERE {pred}")).unwrap();
        }
        let sel = t.elapsed().as_micros() as f64 / ops as f64;
        let t = Instant::now();
        let mut affected = String::new();
        for _ in 0..ops {
            affected = format!("{:?}", s.execute_sql(&format!("UPDATE t SET name = 'z' WHERE {pred}")).unwrap());
        }
        let upd = t.elapsed().as_micros() as f64 / ops as f64;
        println!("RANGE N={n} pred='{pred}' ops={ops} select_count={sel:.0}us update={upd:.0}us result={affected}");
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
