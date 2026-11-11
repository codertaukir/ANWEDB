#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};
use std::path::PathBuf;

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_upk_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT)")
        .unwrap();
    let vals: Vec<String> = (0..200).map(|k| format!("({k},{})", k % 10)).collect();
    srv.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .unwrap();
    (dir, srv)
}

fn affected(srv: &Server, sql: &str) -> usize {
    match srv.execute_sql(sql).unwrap() {
        ExecutionResult::Affected(n) => n,
        o => panic!("bad result {o:?}"),
    }
}

fn int(srv: &Server, sql: &str) -> i64 {
    match srv.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            o => panic!("bad value {o:?}"),
        },
        o => panic!("bad result {o:?}"),
    }
}

fn cnt(srv: &Server, where_: &str) -> i64 {
    int(srv, &format!("SELECT COUNT(*) FROM t WHERE {where_}"))
}

fn done(dir: PathBuf, srv: Server) {
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn update_pk_eq() {
    let (dir, srv) = open("eq");
    assert_eq!(affected(&srv, "UPDATE t SET g = 999 WHERE id = 42"), 1);
    assert_eq!(int(&srv, "SELECT g FROM t WHERE id = 42"), 999);
    assert_eq!(cnt(&srv, "g = 999"), 1);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}

#[test]
fn update_pk_missing_key() {
    let (dir, srv) = open("miss");
    assert_eq!(affected(&srv, "UPDATE t SET g = 999 WHERE id = 5000"), 0);
    assert_eq!(cnt(&srv, "g = 999"), 0);
    done(dir, srv);
}

#[test]
fn update_pk_ranges() {
    let (dir, srv) = open("rng");
    assert_eq!(affected(&srv, "UPDATE t SET g = 555 WHERE id >= 190"), 10);
    assert_eq!(cnt(&srv, "g = 555"), 10);
    assert_eq!(affected(&srv, "UPDATE t SET g = 444 WHERE id < 5"), 5);
    assert_eq!(cnt(&srv, "g = 444"), 5);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}

#[test]
fn update_non_pk_filter() {
    let (dir, srv) = open("npk");
    assert_eq!(affected(&srv, "UPDATE t SET g = 777 WHERE g = 3"), 20);
    assert_eq!(cnt(&srv, "g = 777"), 20);
    assert_eq!(cnt(&srv, "g = 3"), 0);
    done(dir, srv);
}

#[test]
fn update_changes_pk() {
    let (dir, srv) = open("chpk");
    assert_eq!(affected(&srv, "UPDATE t SET id = 1000 WHERE id = 7"), 1);
    assert_eq!(cnt(&srv, "id = 7"), 0);
    assert_eq!(cnt(&srv, "id = 1000"), 1);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}

#[test]
fn update_pk_conflict_leaves_row() {
    let (dir, srv) = open("conf");
    assert!(srv.execute_sql("UPDATE t SET id = 8 WHERE id = 7").is_err());
    assert_eq!(cnt(&srv, "id = 7"), 1);
    assert_eq!(cnt(&srv, "id = 8"), 1);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}

#[test]
fn update_sees_own_uncommitted_row_in_explicit_txn() {
    let (dir, srv) = open("own");
    srv.execute_sql("BEGIN").unwrap();
    srv.execute_sql("INSERT INTO t VALUES (5000, 1)").unwrap();
    assert_eq!(affected(&srv, "UPDATE t SET g = 2 WHERE id = 5000"), 1);
    assert_eq!(int(&srv, "SELECT g FROM t WHERE id = 5000"), 2);
    srv.execute_sql("COMMIT").unwrap();
    drop(srv);
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    assert_eq!(int(&srv, "SELECT g FROM t WHERE id = 5000"), 2);
    done(dir, srv);
}

#[test]
fn update_pk_self_assignment() {
    let (dir, srv) = open("self_assign");
    assert_eq!(affected(&srv, "UPDATE t SET id = 7 WHERE id = 7"), 1);
    assert_eq!(cnt(&srv, "id = 7"), 1);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}

#[test]
fn update_pk_with_secondary_index_and_scan() {
    let (dir, srv) = open("sec_idx");
    srv.execute_sql("CREATE INDEX idx_g ON t (g)").unwrap();
    // g is 7 % 10 = 7 for id=7. Changing PK from 7 to 9007:
    assert_eq!(affected(&srv, "UPDATE t SET id = 9007 WHERE id = 7"), 1);
    assert_eq!(cnt(&srv, "id = 7"), 0);
    assert_eq!(cnt(&srv, "id = 9007"), 1);
    // Index scan on g = 7 must return exactly 20 rows, and id 9007 must be reachable via index
    assert_eq!(cnt(&srv, "g = 7"), 20);
    assert_eq!(cnt(&srv, "g = 7 AND id = 9007"), 1);
    assert_eq!(cnt(&srv, "g = 7 AND id = 7"), 0);
    done(dir, srv);
}

#[test]
fn update_pk_rollback_preserves_original_state() {
    let (dir, srv) = open("pk_rollback");
    srv.execute_sql("CREATE INDEX idx_g ON t (g)").unwrap();
    srv.execute_sql("BEGIN").unwrap();
    assert_eq!(affected(&srv, "UPDATE t SET id = 9999 WHERE id = 15"), 1);
    assert_eq!(cnt(&srv, "id = 9999"), 1);
    assert_eq!(cnt(&srv, "id = 15"), 0);
    srv.execute_sql("ROLLBACK").unwrap();

    // After rollback, 15 must be restored, 9999 must not exist
    assert_eq!(cnt(&srv, "id = 15"), 1);
    assert_eq!(cnt(&srv, "id = 9999"), 0);
    assert_eq!(cnt(&srv, "g = 5 AND id = 15"), 1);
    assert_eq!(int(&srv, "SELECT COUNT(*) FROM t"), 200);
    done(dir, srv);
}
