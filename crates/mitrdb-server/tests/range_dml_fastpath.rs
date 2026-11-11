#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::IsolationLevel;

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

#[test]
fn range_dml_indexed_equals_unindexed() {
    let d = fresh("rngfast");
    let s = open(&d);
    s.execute_sql("CREATE TABLE ti (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    s.execute_sql("CREATE INDEX ti_g ON ti(g)").unwrap();
    s.execute_sql("CREATE TABLE tu (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    load(&s, "ti", 6000, true);
    load(&s, "tu", 6000, true);
    assert_eq!(dump(&s, "ti"), dump(&s, "tu"), "premise: same data");
    let stmts = [
        "UPDATE {T} SET name = 'a' WHERE g > 990",
        "UPDATE {T} SET name = 'b' WHERE g >= 500 AND g < 510",
        "UPDATE {T} SET name = 'c' WHERE g <= 3",
        "UPDATE {T} SET name = 'd' WHERE g > 995 OR g < 2",
        "UPDATE {T} SET name = 'e' WHERE g >= 500",
        "UPDATE {T} SET name = 'f' WHERE g >= 0",
        "UPDATE {T} SET name = 'h' WHERE g > 5000",
        "UPDATE {T} SET name = 'i' WHERE g > 990 AND id > 3000",
        "UPDATE {T} SET g = g + 1 WHERE g > 990",
        "UPDATE {T} SET g = 7 WHERE g >= 990",
        "DELETE FROM {T} WHERE g > 993",
        "DELETE FROM {T} WHERE g < 2 AND id > 100",
        "DELETE FROM {T} WHERE g >= 0",
    ];
    for st in stmts {
        let a = format!("{:?}", s.execute_sql(&st.replace("{T}", "ti")).unwrap());
        let b = format!("{:?}", s.execute_sql(&st.replace("{T}", "tu")).unwrap());
        assert_eq!(a, b, "result differs for: {st}");
        assert_eq!(dump(&s, "ti"), dump(&s, "tu"), "dump differs after: {st}");
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn range_dml_serializable_phantom_same_with_and_without_index() {
    let d = fresh("rngphantom");
    let s = open(&d);
    s.execute_sql("CREATE TABLE ti (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    s.execute_sql("CREATE INDEX ti_g ON ti(g)").unwrap();
    s.execute_sql("CREATE TABLE tu (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    load(&s, "ti", 2000, false);
    load(&s, "tu", 2000, false);
    let mut outcomes = Vec::new();
    for t in ["ti", "tu"] {
        let s1 = s.create_session_with_isolation(IsolationLevel::Serializable);
        s.execute_in_session(s1, "BEGIN").unwrap();
        let u_ok = s
            .execute_in_session(s1, &format!("UPDATE {t} SET name = 'x' WHERE g > 990"))
            .is_ok();
        let ins_ok = s
            .execute_sql(&format!("INSERT INTO {t} VALUES (100000, 995, 'p')"))
            .is_ok();
        let commit_ok = s.execute_in_session(s1, "COMMIT").is_ok();
        println!("PHANTOM {t}: update_ok={u_ok} insert_ok={ins_ok} commit_ok={commit_ok}");
        outcomes.push((u_ok, ins_ok, commit_ok, dump(&s, t)));
    }
    assert_eq!(
        outcomes[0], outcomes[1],
        "indexed table must behave like the unindexed one"
    );
    let _ = std::fs::remove_dir_all(&d);
}
