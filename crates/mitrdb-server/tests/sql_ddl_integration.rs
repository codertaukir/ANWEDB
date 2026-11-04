use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_ddl_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn rows(s: &Server, sql: &str) -> usize {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn drop_table_then_recreate_is_empty_and_survives_restart() {
    let d = fresh("recreate");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, email text)").unwrap();
        s.execute_sql("CREATE UNIQUE INDEX t_email_uq ON t (email)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'a@x.com'), (2, 'b@x.com')").unwrap();
        s.execute_sql("DROP TABLE t").unwrap();

        // same name, different shape; no old rows
        s.execute_sql("CREATE TABLE t (id int primary key, name text, email text)").unwrap();
        assert_eq!(rows(&s, "SELECT * FROM t"), 0);
        // old index is gone with the old table: name is free again
        s.execute_sql("CREATE UNIQUE INDEX t_email_uq ON t (email)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'n', 'a@x.com')").unwrap();
    }
    let s = open(&d);
    assert_eq!(rows(&s, "SELECT * FROM t"), 1);
    assert!(s.execute_sql("INSERT INTO t VALUES (2, 'm', 'a@x.com')").is_err());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn add_column_keeps_unique_enforced_after_restart() {
    let d = fresh("addcol_uq");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, email text)").unwrap();
        s.execute_sql("CREATE UNIQUE INDEX t_email_uq ON t (email)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'a@x.com')").unwrap();
        s.execute_sql("ALTER TABLE t ADD COLUMN age int").unwrap();
        s.execute_sql("INSERT INTO t VALUES (2, 'b@x.com', 30)").unwrap();
        assert!(s.execute_sql("INSERT INTO t VALUES (3, 'a@x.com', 1)").is_err());
    }
    let s = open(&d);
    assert_eq!(rows(&s, "SELECT * FROM t"), 2);
    assert_eq!(rows(&s, "SELECT * FROM t WHERE age IS NULL"), 1);
    assert!(s.execute_sql("INSERT INTO t VALUES (3, 'b@x.com', 1)").is_err());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn bad_ddl_input_is_rejected_and_changes_nothing() {
    let d = fresh("bad_ddl");
    let s = open(&d);
    s.execute_sql("CREATE TABLE t (id int primary key, name text)").unwrap();
    s.execute_sql("INSERT INTO t VALUES (1, 'a')").unwrap();

    assert!(s.execute_sql("ALTER TABLE missing ADD COLUMN x int").is_err());
    assert!(s.execute_sql("ALTER TABLE t ADD COLUMN name text").is_err());
    assert!(s.execute_sql("CREATE TABLE t (id int primary key)").is_err());
    assert!(s.execute_sql("CREATE INDEX i1 ON t (nope)").is_err());
    assert!(s.execute_sql("CREATE INDEX i1 ON missing (id)").is_err());

    assert_eq!(rows(&s, "SELECT * FROM t"), 1);
    assert_eq!(rows(&s, "SELECT id, name FROM t"), 1);
    let _ = std::fs::remove_dir_all(&d);
}

// Separate on purpose: if this fails it is a real bug (index metadata keeps the old column name).
#[test]
fn rename_indexed_column_keeps_unique_working() {
    let d = fresh("rename_idx");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, email text)").unwrap();
        s.execute_sql("CREATE UNIQUE INDEX t_email_uq ON t (email)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'a@x.com')").unwrap();
        s.execute_sql("ALTER TABLE t RENAME COLUMN email TO mail").unwrap();
        assert!(s.execute_sql("INSERT INTO t VALUES (2, 'a@x.com')").is_err());
        s.execute_sql("INSERT INTO t VALUES (2, 'b@x.com')").unwrap();
    }
    let s = open(&d);
    assert_eq!(rows(&s, "SELECT * FROM t WHERE mail = 'b@x.com'"), 1);
    assert!(s.execute_sql("INSERT INTO t VALUES (3, 'a@x.com')").is_err());
    let _ = std::fs::remove_dir_all(&d);
}
