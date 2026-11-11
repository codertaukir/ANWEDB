#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
#![allow(dead_code)]

use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 4096)
        .unwrap()
        .0
}
fn load(s: &Server, t: &str, n: i64, nulls: bool) {
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end)
            .map(|k| {
                let g = if nulls && k % 7 == 0 {
                    "NULL".to_string()
                } else {
                    (k % 1000).to_string()
                };
                format!("({k},{g},'user_{k}')")
            })
            .collect();
        s.execute_sql(&format!("INSERT INTO {t} VALUES {}", vals.join(",")))
            .unwrap();
        i = end;
    }
}
fn dump(s: &Server, t: &str) -> String {
    match s
        .execute_sql(&format!("SELECT * FROM {t} ORDER BY id"))
        .unwrap()
    {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows),
        o => panic!("expected rows, got {o:?}"),
    }
}

use std::time::Instant;

#[test]
fn selective_range_dml_is_not_full_scan() {
    let d = fresh("rngratio");
    let s = open(&d);
    s.execute_sql("CREATE TABLE ti (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    s.execute_sql("CREATE INDEX ti_g ON ti(g)").unwrap();
    s.execute_sql("CREATE TABLE tu (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    load(&s, "ti", 16000, false);
    load(&s, "tu", 16000, false);
    let run = |t: &str| {
        let st = Instant::now();
        let mut r = String::new();
        for _ in 0..10 {
            r = format!(
                "{:?}",
                s.execute_sql(&format!("UPDATE {t} SET name = 'z' WHERE g > 990"))
                    .unwrap()
            );
        }
        (st.elapsed(), r)
    };
    let (ti, ri) = run("ti");
    let (tu, ru) = run("tu");
    println!("RATIO indexed={ti:?} unindexed={tu:?} r_idx={ri} r_unidx={ru}");
    assert_eq!(ri, "Affected(144)");
    assert_eq!(ri, ru);
    let min_multiplier = if cfg!(debug_assertions) { 1 } else { 2 };
    assert!(
        ti * min_multiplier <= tu,
        "indexed range UPDATE not faster: {ti:?} vs {tu:?}"
    );
    let _ = std::fs::remove_dir_all(&d);
}
