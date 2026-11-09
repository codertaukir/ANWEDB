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
    for pred in ["g > 5000", "g > 990", "g >= 750", "g >= 500", "g >= 0", "g < -3"] {
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
    // same-run twins: indexed vs unindexed, column g (k % 1000) and column u (all distinct)
    let create = |name: &str, col: &str, distinct: bool, idx: bool| {
        s.execute_sql(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, {col} INT, name TEXT)")).unwrap();
        if idx {
            s.execute_sql(&format!("CREATE INDEX {name}_i ON {name}({col})")).unwrap();
        }
        let mut i = 0i64;
        while i < n {
            let end = (i + 500).min(n);
            let vals: Vec<String> = (i..end)
                .map(|k| format!("({k},{},'user_{k}')", if distinct { k } else { k % 1000 }))
                .collect();
            s.execute_sql(&format!("INSERT INTO {name} VALUES {}", vals.join(","))).unwrap();
            i = end;
        }
    };
    create("tg_i", "g", false, true);
    create("tg_u", "g", false, false);
    create("tu_i", "u", true, true);
    create("tu_u", "u", true, false);
    let run = |tbl: &str, pred: &str| {
        let t = Instant::now();
        let mut r = String::new();
        for _ in 0..ops {
            r = format!("{:?}", s.execute_sql(&format!("UPDATE {tbl} SET name = 'z' WHERE {pred}")).unwrap());
        }
        let us = t.elapsed().as_micros() as f64 / ops as f64;
        println!("RANGE2 N={n} table={tbl} pred='{pred}' update={us:.0}us result={r}");
    };
    for pred in ["g >= 750", "g >= 500", "g >= 0", "g < -3"] {
        run("tg_i", pred);
        run("tg_u", pred);
    }
    for pred in [format!("u >= {}", n - 100), format!("u >= {}", n - 1000), format!("u >= {}", n - 2000), format!("u >= {}", n - 3000), format!("u >= {}", n - 5000), "u >= 0".to_string()] {
        run("tu_i", &pred);
        run("tu_u", &pred);
        run("tu_u", &pred);
        run("tu_i", &pred);
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}
