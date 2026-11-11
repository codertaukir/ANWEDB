#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
// Server has no Drop: dropping it = crash (no checkpoint / flush).
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn shape(s: &Server, sql: &str) -> (usize, usize) {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { columns, rows } => (columns.len(), rows.len()),
        o => panic!("expected rows, got {o:?}"),
    }
}
fn dump(s: &Server, sql: &str) -> String {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn failed_type_change_then_crash_leaves_schema_intact_and_retry_works() {
    let d = fresh("atcrash_a");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text, c text)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'p', '5')").unwrap();
        s.execute_sql("INSERT INTO t VALUES (2, 'q', 'bad')")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (3, 'r', '7')").unwrap();
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int")
                .is_err()
        );
    } // crash right after the failed change
    {
        let s = open(&d);
        assert_eq!(
            shape(&s, "SELECT * FROM t"),
            (3, 3),
            "hidden shadow must not be visible"
        );
        assert!(dump(&s, "SELECT c FROM t WHERE id = 2").contains("Text(\"bad\")"));
        s.execute_sql("INSERT INTO t VALUES (4, 's', '9')").unwrap(); // positional width unchanged
        s.execute_sql("UPDATE t SET c = '6' WHERE id = 2").unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int")
            .unwrap();
        assert!(dump(&s, "SELECT c FROM t WHERE id = 2").contains("Int64(6)"));
    } // crash right after the swap
    let s = open(&d);
    assert_eq!(shape(&s, "SELECT * FROM t"), (3, 4));
    assert!(dump(&s, "SELECT c FROM t WHERE id = 4").contains("Int64(9)"));
    assert!(dump(&s, "SELECT c FROM t WHERE id = 2").contains("Int64(6)"));
    assert!(
        s.execute_sql("INSERT INTO t (id, c) VALUES (5, 'nine')")
            .is_err()
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn leftover_shadow_of_other_type_is_retired_and_survives_crash() {
    let d = fresh("atcrash_b");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, c text)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x')").unwrap();
        // leaves a hidden INT shadow behind
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int")
                .is_err()
        );
        // different target type: old shadow is retired, a fresh bytes shadow is used
        s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE bytes")
            .unwrap();
    } // crash
    let s = open(&d);
    assert_eq!(shape(&s, "SELECT * FROM t"), (2, 1));
    assert!(
        dump(&s, "SELECT c FROM t").contains("Bytes"),
        "{}",
        dump(&s, "SELECT c FROM t")
    );
    let _ = std::fs::remove_dir_all(&d);
}
