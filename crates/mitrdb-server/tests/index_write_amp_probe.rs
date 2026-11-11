#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// PROBE (ignored): indexed vs unindexed unique column, UPDATE SET name (name not indexed). Same-run twin, order alternates.
// Counters: GLOBAL_METRICS snapshot delta (page_reads/page_writes/wal_appends/wal_flushes), process-wide, one test only.
// Run: cargo test --release -p mitrdb-server --test index_write_amp_probe -- --ignored --nocapture
use mitrdb_observe::GLOBAL_METRICS;
use mitrdb_server::Server;
use std::path::PathBuf;
use std::time::Instant;

#[test]
#[ignore]
fn probe_index_write_amp() {
    let n: i64 = std::env::var("MITRDB_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16000);
    let ops: i64 = std::env::var("MITRDB_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let base = std::env::var("MITRDB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("idxamp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 4096)
        .unwrap()
        .0;
    for (name, idx) in [("ta_i", true), ("ta_u", false)] {
        if std::env::var("MITRDB_ONLY")
            .map(|o| o != name)
            .unwrap_or(false)
        {
            continue;
        }
        s.execute_sql(&format!(
            "CREATE TABLE {name} (id INT PRIMARY KEY, u INT, name TEXT)"
        ))
        .unwrap();
        if idx {
            s.execute_sql(&format!("CREATE INDEX {name}_x ON {name}(u)"))
                .unwrap();
        }
        let mut i = 0i64;
        while i < n {
            let end = (i + 500).min(n);
            let vals: Vec<String> = (i..end).map(|k| format!("({k},{k},'user_{k}')")).collect();
            s.execute_sql(&format!("INSERT INTO {name} VALUES {}", vals.join(",")))
                .unwrap();
            i = end;
        }
    }
    let rows = (ops * n) as f64;
    let pred = std::env::var("MITRDB_PRED").unwrap_or_else(|_| "u >= 0".to_string());
    println!("PRED {pred}");
    for round in 0..3 {
        let order = if round % 2 == 0 {
            ["ta_i", "ta_u"]
        } else {
            ["ta_u", "ta_i"]
        };
        for tbl in order {
            if std::env::var("MITRDB_ONLY")
                .map(|o| o != tbl)
                .unwrap_or(false)
            {
                continue;
            }
            let b = GLOBAL_METRICS.snapshot();
            let t = Instant::now();
            for _ in 0..ops {
                s.execute_sql(&format!("UPDATE {tbl} SET name = 'z{round}' WHERE {pred}"))
                    .unwrap();
            }
            let el = t.elapsed();
            let a = GLOBAL_METRICS.snapshot();
            println!(
                "AMP round={round} table={tbl} N={n} us_per_row={:.2} reads/row={:.3} writes/row={:.3} wal_appends/row={:.3} wal_flushes={}",
                el.as_micros() as f64 / rows,
                (a.page_reads - b.page_reads) as f64 / rows,
                (a.page_writes - b.page_writes) as f64 / rows,
                (a.wal_appends - b.wal_appends) as f64 / rows,
                a.wal_flushes - b.wal_flushes
            );
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}
