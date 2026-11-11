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
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn dump(s: &Server, sql: &str) -> String {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn table_change_type_validates_names_and_converts() {
    let d = fresh("alttype_api");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a int)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 42)").unwrap();

        assert!(s.table("t").change_type("bad name", "text").is_err());
        assert!(s.table("t").change_type("a", "te xt").is_err());
        assert!(s.table("t").change_type("a", "").is_err());
        assert!(s.table("t").change_type("a", "float").is_err());
        assert!(dump(&s, "SELECT a FROM t").contains("Int64(42)"));

        s.table("t").change_type("a", "text").unwrap();
        assert!(dump(&s, "SELECT a FROM t").contains("Text(\"42\")"));
        assert!(s.table("t").change_type("id", "text").is_err());
    }
    let s = open(&d);
    assert!(dump(&s, "SELECT a FROM t").contains("Text(\"42\")"));
    let _ = std::fs::remove_dir_all(&d);
}
