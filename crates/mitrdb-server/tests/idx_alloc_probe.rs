// PROBE (ignored): allocations per row, indexed vs unindexed UPDATE SET name (index column unchanged).
// Counting allocator is process-wide; run with --test-threads=1 (andaza: background threads add small noise).
use mitrdb_server::Server;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

struct C;
static N: AtomicU64 = AtomicU64::new(0);
static B: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for C {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        N.fetch_add(1, Relaxed);
        B.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        N.fetch_add(1, Relaxed);
        B.fetch_add(new as u64, Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}
#[global_allocator]
static A: C = C;

#[test]
#[ignore]
fn probe_idx_allocs() {
    let n: i64 = std::env::var("MITRDB_N").ok().and_then(|v| v.parse().ok()).unwrap_or(4000);
    let base = std::env::var("MITRDB_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("idxalloc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096).unwrap().0;
    for (name, idx) in [("ta_i", true), ("ta_u", false)] {
        s.execute_sql(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, u INT, name TEXT)")).unwrap();
        if idx {
            s.execute_sql(&format!("CREATE INDEX {name}_x ON {name}(u)")).unwrap();
        }
        let mut i = 0i64;
        while i < n {
            let end = (i + 500).min(n);
            let vals: Vec<String> = (i..end).map(|k| format!("({k},{k},'user_{k}')")).collect();
            s.execute_sql(&format!("INSERT INTO {name} VALUES {}", vals.join(","))).unwrap();
            i = end;
        }
    }
    for round in 0..3 {
        let order = if round % 2 == 0 { ["ta_i", "ta_u"] } else { ["ta_u", "ta_i"] };
        for tbl in order {
            let sql = format!("UPDATE {tbl} SET name = 'z{round}' WHERE u >= 0");
            let (n0, b0) = (N.load(Relaxed), B.load(Relaxed));
            s.execute_sql(&sql).unwrap();
            let (n1, b1) = (N.load(Relaxed), B.load(Relaxed));
            println!(
                "ALLOC round={round} table={tbl} N={n} allocs/row={:.2} bytes/row={:.0}",
                (n1 - n0) as f64 / n as f64,
                (b1 - b0) as f64 / n as f64
            );
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}
