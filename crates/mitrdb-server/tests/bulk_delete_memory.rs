// Bulk DELETE memory must not grow with N (WAL arena is flushed mid-statement).
// Keep this file to a single test: VmHWM is process-wide.
use mitrdb_server::{val, Server};

fn hwm_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string()))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

#[test]
fn bulk_delete_memory_is_bounded() {
    let n: i64 = 20000;
    let d = std::env::temp_dir().join(format!("mitrdb_bulkdel_mem_{}", std::process::id()));
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
    let hwm0 = hwm_mb();
    let del = s.table("t").delete().filter("id", ">=", val(0)).run().unwrap();
    let hwm1 = hwm_mb();
    println!("BULKDEL n={n} deleted={del} hwm_before={hwm0}MB hwm_after={hwm1}MB delta={}MB", hwm1.saturating_sub(hwm0));
    assert_eq!(s.table("t").find().count().unwrap(), 0);
    assert!(
        hwm1.saturating_sub(hwm0) < 100,
        "bulk delete grew VmHWM by {}MB (WAL arena not bounded?)",
        hwm1.saturating_sub(hwm0)
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
