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
fn set_not_null_validates_rows_then_enforces_and_persists() {
    let d = fresh("altnn");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text, b int)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 1)").unwrap();
        s.execute_sql("INSERT INTO t (id, b) VALUES (2, 2)")
            .unwrap();

        // row id=2 has NULL a -> refused, error names the primary key, schema unchanged
        let e = s
            .execute_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL")
            .unwrap_err();
        let msg = format!("{:?}", e);
        assert!(msg.contains("Int64(2)"), "error should name pk 2: {msg}");
        s.execute_sql("INSERT INTO t (id, b) VALUES (3, 3)")
            .unwrap();
        s.execute_sql("DELETE FROM t WHERE id = 3").unwrap();

        // fix the data, then it succeeds
        s.execute_sql("UPDATE t SET a = 'z' WHERE id = 2").unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL")
            .unwrap();
        assert!(
            s.execute_sql("INSERT INTO t (id, b) VALUES (4, 4)")
                .is_err()
        );

        // idempotent, PK is a no-op, unknown column refused
        s.execute_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL")
            .unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN id SET NOT NULL")
            .unwrap();
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN nope SET NOT NULL")
                .is_err()
        );

        // rows padded by ADD COLUMN count as NULL
        s.execute_sql("ALTER TABLE t ADD COLUMN c int").unwrap();
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN c SET NOT NULL")
                .is_err()
        );

        // DROP NOT NULL then SET NOT NULL roundtrip
        s.execute_sql("ALTER TABLE t ALTER COLUMN a DROP NOT NULL")
            .unwrap();
        s.execute_sql("INSERT INTO t (id, b) VALUES (5, 5)")
            .unwrap();
        assert!(
            s.execute_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL")
                .is_err()
        );
        s.execute_sql("DELETE FROM t WHERE id = 5").unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN a SET NOT NULL")
            .unwrap();
    }
    let s = open(&d);
    assert!(
        s.execute_sql("INSERT INTO t (id, b) VALUES (6, 6)")
            .is_err()
    );
    assert_eq!(shape(&s, "SELECT * FROM t"), (4, 2));
    let _ = std::fs::remove_dir_all(&d);
}
