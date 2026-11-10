use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &PathBuf) -> Server {
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
fn change_type_casts_rows_atomically_and_persists() {
    let d = fresh("alttype");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a int, b text not null, c text)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 10, 'x', '5')").unwrap();
        s.execute_sql("INSERT INTO t VALUES (2, NULL, 'y', '6')").unwrap();
        s.execute_sql("INSERT INTO t VALUES (3, -5, 'z', 'abc')").unwrap();

        // text -> int with a bad row: refused, error names the pk, nothing visible changed
        let e = s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int").unwrap_err();
        let msg = format!("{:?}", e);
        assert!(msg.contains("Int64(3)"), "error should name pk 3: {msg}");
        assert_eq!(shape(&s, "SELECT * FROM t"), (4, 3));
        s.execute_sql("INSERT INTO t VALUES (4, 40, 'w', 'q')").unwrap(); // positional insert still 4 values
        s.execute_sql("DELETE FROM t WHERE id = 4").unwrap();

        // fix the data, retry (leftover hidden shadow is reused), now it works
        s.execute_sql("UPDATE t SET c = '7' WHERE id = 3").unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int").unwrap();
        assert_eq!(shape(&s, "SELECT * FROM t"), (4, 3));
        assert!(dump(&s, "SELECT c FROM t WHERE id = 3").contains("Int64(7)"));
        assert_eq!(shape(&s, "SELECT * FROM t WHERE c = 7"), (4, 1));
        s.execute_sql("INSERT INTO t (id, b, c) VALUES (5, 'v', 9)").unwrap();
        assert!(s.execute_sql("INSERT INTO t (id, b, c) VALUES (6, 'v', 'nine')").is_err());

        // int -> text, NULLs stay NULL; the column moves to the end (id, b, c, a)
        s.execute_sql("ALTER TABLE t ALTER COLUMN a TYPE text").unwrap();
        assert!(dump(&s, "SELECT a FROM t WHERE id = 1").contains("Text(\"10\")"));
        assert!(dump(&s, "SELECT a FROM t WHERE id = 3").contains("Text(\"-5\")"));
        assert_eq!(shape(&s, "SELECT * FROM t WHERE a IS NULL"), (4, 2));
        s.execute_sql("INSERT INTO t VALUES (7, 'v', 11, 'tx')").unwrap();
        assert_eq!(shape(&s, "SELECT * FROM t"), (4, 5));

        // NOT NULL on the changed column is kept
        s.execute_sql("ALTER TABLE t ALTER COLUMN b TYPE bytes").unwrap();
        assert!(s.execute_sql("INSERT INTO t (id, c) VALUES (8, 1)").is_err());

        // same type is a no-op; unknown column / unknown type refused
        s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int").unwrap();
        assert!(s.execute_sql("ALTER TABLE t ALTER COLUMN nope TYPE int").is_err());
        assert!(s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE float").is_err());

        // refusals: PK, unsupported cast (indexed column rebuilds automatically)
        assert!(s.execute_sql("ALTER TABLE t ALTER COLUMN id TYPE text").is_err());
        s.execute_sql("CREATE UNIQUE INDEX t_c_uq ON t (c)").unwrap();
        s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE text").unwrap();
        s.execute_sql("CREATE TABLE u (id int primary key, f bool)").unwrap();
        s.execute_sql("INSERT INTO u VALUES (1, true)").unwrap();
        assert!(s.execute_sql("ALTER TABLE u ALTER COLUMN f TYPE text").is_err());
        s.execute_sql("ALTER TABLE u ALTER COLUMN f TYPE int").unwrap();
        assert!(dump(&s, "SELECT f FROM u").contains("Int64(1)"));
    }
    let s = open(&d);
    assert_eq!(shape(&s, "SELECT * FROM t"), (4, 5));
    assert!(dump(&s, "SELECT a FROM t WHERE id = 1").contains("Text(\"10\")"));
    assert!(dump(&s, "SELECT f FROM u").contains("Int64(1)"));
    assert!(s.execute_sql("INSERT INTO t (id, c) VALUES (8, 1)").is_err());
    let _ = std::fs::remove_dir_all(&d);
}
