use mitrdb_server::{val, Server};
use std::time::Instant;

#[test]
#[ignore]
fn probe_lookup_cost() {
    let base = std::env::var("MITRDB_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let d = std::path::PathBuf::from(base).join(format!("lk_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    s.table("t").sync("id int primary key, g int, name text").unwrap();
    let n = 100_000i64;
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'u{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }
    let t = s.table("t");
    let t1 = Instant::now();
    for k in 0..50i64 {
        assert!(t.find().filter("id", "=", val((k * 997) % n)).one().unwrap().is_some());
    }
    println!("LK 50 autocommit PK lookups: {:?}", t1.elapsed());
    let t1 = Instant::now();
    for _ in 0..50 {
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("COMMIT").unwrap();
    }
    println!("LK 50 empty BEGIN/COMMIT: {:?}", t1.elapsed());
    s.execute_sql("BEGIN").unwrap();
    let t1 = Instant::now();
    for k in 0..50i64 {
        assert!(t.find().filter("id", "=", val((k * 997) % n)).one().unwrap().is_some());
    }
    println!("LK 50 PK lookups inside one txn: {:?}", t1.elapsed());
    s.execute_sql("COMMIT").unwrap();
    let _ = std::fs::remove_dir_all(&d);
}
