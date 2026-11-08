//! Probe: heap bytes retained / peak / after-commit for bulk DELETE and UPDATE in an explicit txn.
//! Counting allocator (requested bytes, not RSS). Use a fresh directory.
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use mitrdb_server::Server;

struct Counting;
static CUR: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let c = CUR.fetch_add(l.size(), Relaxed) + l.size();
            PEAK.fetch_max(c, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        CUR.fetch_sub(l.size(), Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            if new >= l.size() {
                let c = CUR.fetch_add(new - l.size(), Relaxed) + (new - l.size());
                PEAK.fetch_max(c, Relaxed);
            } else {
                CUR.fetch_sub(l.size() - new, Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static A: Counting = Counting;

fn mb(b: usize) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 1 {
        return Err("Usage: mem_probe <fresh-dir>".into());
    }
    let base_dir = Path::new(&args[0]);
    if base_dir.exists() {
        return Err("Refusing to use an existing directory".into());
    }
    std::fs::create_dir_all(base_dir).map_err(|e| e.to_string())?;
    let cases: [(&str, &str); 3] = [
        ("delete", "DELETE FROM t WHERE id >= 0"),
        ("update_name", "UPDATE t SET name = 'renamed_row' WHERE id >= 0"),
        ("update_g", "UPDATE t SET g = 7 WHERE id >= 0"),
    ];
    for n in [5000i64, 10000, 20000] {
        for (kind, sql) in cases.iter() {
            let d = base_dir.join(format!("{kind}_{n}"));
            std::fs::create_dir_all(&d).map_err(|e| e.to_string())?;
            let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096)
                .map_err(|e| format!("open: {e:?}"))?
                .0;
            s.table("t")
                .sync("id int primary key, g int, name text")
                .map_err(|e| format!("sync: {e:?}"))?;
            let mut i = 0i64;
            while i < n {
                let end = (i + 500).min(n);
                let vals: Vec<String> =
                    (i..end).map(|k| format!("({k},{},'user_{k}')", k % 1000)).collect();
                s.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(",")))
                    .map_err(|e| format!("insert: {e:?}"))?;
                i = end;
            }
            s.execute_sql("BEGIN;").map_err(|e| format!("begin: {e:?}"))?;
            let base = CUR.load(Relaxed);
            let m0 = s.metrics_snapshot();
            PEAK.store(base, Relaxed);
            s.execute_sql(sql).map_err(|e| format!("stmt {kind}: {e:?}"))?;
            let m1 = s.metrics_snapshot();
            let retained = CUR.load(Relaxed).saturating_sub(base);
            let peak = PEAK.load(Relaxed).saturating_sub(base);
            s.execute_sql("COMMIT;").map_err(|e| format!("commit: {e:?}"))?;
            let after = CUR.load(Relaxed).saturating_sub(base);
            println!(
                "{kind:<12} n={n:<6} retained={:>7.2}MB ({:>5} B/row)  peak={:>7.2}MB ({:>5} B/row)  after_commit={:>7.2}MB ({:>5} B/row)",
                mb(retained), retained / n as usize,
                mb(peak), peak / n as usize,
                mb(after), after / n as usize,
            );
            println!("{kind:<12} n={n:<6} stmt: wal_flushes={} wal_syncs={} sync_ms={:.1}",
                m1.wal_flushes - m0.wal_flushes,
                m1.wal_sync_count - m0.wal_sync_count,
                (m1.wal_sync_total_ns - m0.wal_sync_total_ns) as f64 / 1e6);
            drop(s);
        }
    }
    let _ = std::fs::remove_dir_all(base_dir);
    Ok(())
}
