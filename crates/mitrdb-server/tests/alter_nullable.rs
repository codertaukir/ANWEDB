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
fn shape(s: &Server, sql: &str) -> (usize, usize) {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { columns, rows } => (columns.len(), rows.len()),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn drop_not_null_allows_nulls_refuses_pk_and_persists() {
    let d = fresh("altnull");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text not null, b int)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
        assert!(
            s.execute_sql("INSERT INTO t (id, b) VALUES (2, 20)")
                .is_err(),
            "NOT NULL must reject a missing value for a"
        );

        s.execute_sql("ALTER TABLE t ALTER COLUMN a DROP NOT NULL")
            .unwrap();
        s.execute_sql("INSERT INTO t (id, b) VALUES (2, 20)")
            .unwrap();
        assert_eq!(shape(&s, "SELECT * FROM t WHERE a IS NULL"), (3, 1));
        assert_eq!(shape(&s, "SELECT * FROM t"), (3, 2));

        // idempotent
        s.execute_sql("ALTER TABLE t ALTER COLUMN a DROP NOT NULL")
            .unwrap();

        // refusals
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN id DROP NOT NULL")
                .is_err()
        );
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN nope DROP NOT NULL")
                .is_err()
        );
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN a FROBNICATE")
                .is_err()
        );

        // plain DROP COLUMN must still parse as before
        s.execute_sql("ALTER TABLE t DROP COLUMN b").unwrap();
    }
    let s = open(&d);
    assert_eq!(shape(&s, "SELECT * FROM t WHERE a IS NULL"), (2, 1));
    s.execute_sql("INSERT INTO t (id) VALUES (3)").unwrap();
    assert_eq!(shape(&s, "SELECT * FROM t"), (2, 3));
    let _ = std::fs::remove_dir_all(&d);
}
