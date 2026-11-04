use mitrdb_server::{val, Server};
use std::path::PathBuf;
use std::time::Instant;

fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0
}

fn sizes(d: &PathBuf) -> (u64, u64) {
    let (mut db, mut wal) = (0u64, 0u64);
    for e in std::fs::read_dir(d).unwrap().flatten() {
        let m = e.metadata().unwrap();
        if !m.is_file() { continue; }
        let n = e.file_name().to_string_lossy().to_string();
        if n.contains("wal") { wal += m.len() } else { db += m.len() }
    }
    (db / 1_048_576, wal / 1_048_576)
}

fn hwm_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string()))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

#[test]
#[ignore]
fn probe_large_dataset() {
    let n: i64 = std::env::var("MITRDB_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let base = std::env::var("MITRDB_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("mitrdb_large_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();

    let s = open(&d);
    let spec = if std::env::var("MITRDB_INDEX").map(|v| v == "0").unwrap_or(false) { "id int primary key, g int, name text" } else { "id int primary key, g int index, name text" };
    s.table("t").sync(spec).unwrap();
    let t0 = Instant::now();
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
        s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }
    let el = t0.elapsed();
    println!("LARGE insert rows={n} took={el:?} rate={:.0} rows/s", n as f64 / el.as_secs_f64());
    let (db, wal) = sizes(&d);
    println!("LARGE sizes after insert: db={db}MB wal={wal}MB  VmHWM={}MB", hwm_mb());

    let t = s.table("t");
    let t1 = Instant::now();
    assert_eq!(t.find().count().unwrap(), n as usize);
    println!("LARGE full count took={:?}", t1.elapsed());
    let t1 = Instant::now();
    assert_eq!(t.find().filter("g", "=", val(5)).count().unwrap(), (n / 1000) as usize);
    println!("LARGE index lookup (g=5, {} rows) took={:?}", n / 1000, t1.elapsed());
    let t1 = Instant::now();
    for k in 0..1000i64 {
        assert!(t.find().filter("id", "=", val((k * 997) % n)).one().unwrap().is_some());
    }
    println!("LARGE 1000 PK point lookups took={:?}", t1.elapsed());
    let t1 = Instant::now();
    let r = t.find().filter("id", ">", val(n - 1000)).order_by("id").run().unwrap();
    println!("LARGE range tail rows={} took={:?}", r.rows.len(), t1.elapsed());
    drop(t);

    drop(s); // crash (no checkpoint)
    let t1 = Instant::now();
    let s = open(&d);
    println!("LARGE restart after crash (WAL replay) took={:?}", t1.elapsed());
    assert_eq!(s.table("t").find().count().unwrap(), n as usize);

    let t1 = Instant::now();
    s.checkpoint().unwrap();
    println!("LARGE checkpoint took={:?}", t1.elapsed());
    drop(s);
    let t1 = Instant::now();
    let s = open(&d);
    println!("LARGE restart after checkpoint took={:?}", t1.elapsed());
    assert_eq!(s.table("t").find().count().unwrap(), n as usize);

    let t1 = Instant::now();
    let del = s.table("t").delete().filter("id", "<", val(n / 2)).run().unwrap();
    println!("LARGE delete 50% rows={del} took={:?}", t1.elapsed());
    let t1 = Instant::now();
    let rep = s.vacuum();
    println!("LARGE vacuum pruned={} keys_cleared={} txns={} took={:?}",
        rep.versions_pruned, rep.keys_cleared, rep.txns_reclaimed, t1.elapsed());
    s.checkpoint().unwrap();
    let (db, wal) = sizes(&d);
    println!("LARGE sizes after delete+vacuum+ckpt: db={db}MB wal={wal}MB  VmHWM={}MB", hwm_mb());
    assert_eq!(s.table("t").find().count().unwrap(), (n - n / 2) as usize);
    assert_eq!(s.table("t").find().filter("g", "=", val(5)).count().unwrap(), ((n - n / 2) / 1000) as usize);
    let _ = std::fs::remove_dir_all(&d);
}
