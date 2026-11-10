use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};
use std::path::PathBuf;

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_alter_idx_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    (dir, srv)
}

fn done(dir: PathBuf, srv: Server) {
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_alter_column_type_on_indexed_column() {
    let (dir, srv) = open("alter_idx");
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").unwrap();
    srv.execute_sql("INSERT INTO t VALUES (1, '100'), (2, '200'), (3, '300')").unwrap();
    srv.execute_sql("CREATE INDEX idx_val ON t (val)").unwrap();

    let res = srv.execute_sql("ALTER TABLE t ALTER COLUMN val TYPE INT");
    assert!(res.is_ok(), "ALTER TABLE with indexed column should succeed");

    let query_res = srv.execute_sql("SELECT id FROM t WHERE val = 200").unwrap();
    match query_res {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "Expected 1 row for val = 200");
            assert_eq!(rows[0][0], Value::Int64(2));
        }
        other => panic!("Unexpected result: {:?}", other),
    }

    done(dir, srv);
}

#[test]
fn test_alter_indexed_col_subsequent_inserts_and_scan() {
    let (dir, srv) = open("subseq_ins");
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, num TEXT)").unwrap();
    srv.execute_sql("INSERT INTO t VALUES (1, '10'), (2, '20'), (3, '30')").unwrap();
    srv.execute_sql("CREATE INDEX idx_num ON t (num)").unwrap();

    srv.execute_sql("ALTER TABLE t ALTER COLUMN num TYPE INT").unwrap();

    // Insert new row using the new integer type
    srv.execute_sql("INSERT INTO t VALUES (4, 40)").unwrap();

    // Query old row via index
    let r1 = srv.execute_sql("SELECT id FROM t WHERE num = 20").unwrap();
    match r1 {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Int64(2));
        }
        o => panic!("unexpected: {o:?}"),
    }

    // Query newly inserted row via index
    let r2 = srv.execute_sql("SELECT id FROM t WHERE num = 40").unwrap();
    match r2 {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Int64(4));
        }
        o => panic!("unexpected: {o:?}"),
    }

    done(dir, srv);
}

#[test]
fn test_alter_unique_indexed_col_duplicate_fails() {
    let (dir, srv) = open("unique_fail");
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)").unwrap();
    // In TEXT, '01' and '1' are distinct, but when cast to INT both become 1
    srv.execute_sql("INSERT INTO t VALUES (1, '01'), (2, '1')").unwrap();
    srv.execute_sql("CREATE UNIQUE INDEX idx_tag ON t (tag)").unwrap();

    let res = srv.execute_sql("ALTER TABLE t ALTER COLUMN tag TYPE INT");
    assert!(res.is_err(), "Expected unique constraint violation on type cast duplicate keys");

    done(dir, srv);
}
