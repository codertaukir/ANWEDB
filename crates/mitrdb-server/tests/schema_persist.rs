#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn open(dir: &PathBuf) -> Server {
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

fn count(server: &Server, sql: &str) -> usize {
    match server.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn schema_data_and_unique_survive_restart() {
    let dir = fresh_dir("persist");
    {
        let server = open(&dir);
        let t = server.table("users");
        t.sync("id int primary key, name text, email text unique")
            .unwrap();
        server
            .execute_sql("INSERT INTO users VALUES (1, 'Taukir', 'a@x.com')")
            .unwrap();
        t.rename("name", "full_name").unwrap();
    }
    let server = open(&dir);
    assert_eq!(count(&server, "SELECT full_name FROM users"), 1);
    assert!(
        server
            .execute_sql("INSERT INTO users VALUES (2, 'X', 'a@x.com')")
            .is_err()
    );
    let plan = server
        .table("users")
        .sync("id int primary key, full_name text, email text unique")
        .unwrap();
    assert!(plan.is_empty(), "unexpected plan: {plan:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_index_removes_constraint_and_persists() {
    let dir = fresh_dir("dropidx");
    {
        let server = open(&dir);
        server
            .execute_sql("CREATE TABLE t (id int primary key, email text)")
            .unwrap();
        server
            .execute_sql("CREATE UNIQUE INDEX t_email_uq ON t (email)")
            .unwrap();
        server
            .execute_sql("INSERT INTO t VALUES (1, 'a@x.com')")
            .unwrap();
        assert!(
            server
                .execute_sql("INSERT INTO t VALUES (2, 'a@x.com')")
                .is_err()
        );
        server.execute_sql("DROP INDEX t_email_uq").unwrap();
        server
            .execute_sql("INSERT INTO t VALUES (2, 'a@x.com')")
            .unwrap();
        assert!(server.execute_sql("DROP INDEX t_email_uq").is_err());
    }
    let server = open(&dir);
    server
        .execute_sql("INSERT INTO t VALUES (3, 'a@x.com')")
        .unwrap();
    assert_eq!(count(&server, "SELECT * FROM t"), 3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_table_persists_and_name_is_reusable() {
    let dir = fresh_dir("droptbl");
    {
        let server = open(&dir);
        server
            .execute_sql("CREATE TABLE t (id int primary key, v text)")
            .unwrap();
        server.execute_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
        server.execute_sql("DROP TABLE t").unwrap();
    }
    let server = open(&dir);
    assert!(server.execute_sql("SELECT * FROM t").is_err());
    server
        .execute_sql("CREATE TABLE t (id int primary key, v text)")
        .unwrap();
    assert_eq!(count(&server, "SELECT * FROM t"), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn alter_add_and_rename_via_sql_persist() {
    let dir = fresh_dir("alter");
    {
        let server = open(&dir);
        server
            .execute_sql("CREATE TABLE t (id int primary key, a text)")
            .unwrap();
        server.execute_sql("INSERT INTO t VALUES (1, 'x')").unwrap();
        server
            .execute_sql("ALTER TABLE t ADD COLUMN age int")
            .unwrap();
        server
            .execute_sql("ALTER TABLE t RENAME COLUMN a TO b")
            .unwrap();
    }
    let server = open(&dir);
    assert_eq!(count(&server, "SELECT id, b, age FROM t"), 1);
    assert!(server.execute_sql("SELECT a FROM t").is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
