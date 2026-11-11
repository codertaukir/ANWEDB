#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server, Value};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn rows(s: &Server, sql: &str) -> usize {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn table_set_not_null_with_default_backfills_then_enforces() {
    let d = fresh("altnn_api");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text, c int)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 7)").unwrap();
        s.execute_sql("INSERT INTO t (id) VALUES (2)").unwrap();
        s.execute_sql("INSERT INTO t (id, a) VALUES (3, 'y')")
            .unwrap();

        // no default + NULL rows -> refused, nothing changed
        assert!(s.table("t").set_not_null("c", None).is_err());
        assert_eq!(rows(&s, "SELECT * FROM t WHERE c IS NULL"), 2);

        // wrong default type -> refused, NULLs still there, column still nullable
        assert!(
            s.table("t")
                .set_not_null("c", Some(Value::Text("x".into())))
                .is_err()
        );
        assert_eq!(rows(&s, "SELECT * FROM t WHERE c IS NULL"), 2);
        s.execute_sql("INSERT INTO t (id) VALUES (4)").unwrap();
        s.execute_sql("DELETE FROM t WHERE id = 4").unwrap();

        // good default: NULL rows backfilled, existing values untouched, constraint on
        s.table("t")
            .set_not_null("c", Some(Value::Int64(0)))
            .unwrap();
        assert_eq!(rows(&s, "SELECT * FROM t WHERE c IS NULL"), 0);
        assert_eq!(rows(&s, "SELECT * FROM t WHERE c = 0"), 2);
        assert_eq!(rows(&s, "SELECT * FROM t WHERE c = 7"), 1);
        assert!(s.execute_sql("INSERT INTO t (id) VALUES (5)").is_err());

        // text column too, and bad names / unsupported default kinds
        s.table("t")
            .set_not_null("a", Some(Value::Text("n/a".into())))
            .unwrap();
        assert_eq!(rows(&s, "SELECT * FROM t WHERE a = 'n/a'"), 1);
        assert!(s.table("t").set_not_null("bad name", None).is_err());
        assert!(
            s.table("t")
                .set_not_null("a", Some(Value::Bytes(vec![1])))
                .is_err()
        );
    }
    let s = open(&d);
    assert!(s.execute_sql("INSERT INTO t (id) VALUES (6)").is_err());
    assert_eq!(rows(&s, "SELECT * FROM t"), 3);
    let _ = std::fs::remove_dir_all(&d);
}
