use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};
use std::path::PathBuf;

fn rows(srv: &Server, sql: &str) -> Vec<Vec<Value>> {
    match srv.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows,
        o => panic!("bad result {o:?}"),
    }
}

fn count(srv: &Server) -> i64 {
    match &rows(srv, "SELECT COUNT(*) FROM s")[0][0] {
        Value::Int64(n) => *n,
        o => panic!("bad count {o:?}"),
    }
}

fn v_of(srv: &Server, id: i64) -> i64 {
    match &rows(srv, &format!("SELECT v FROM s WHERE id = {id}"))[0][0] {
        Value::Int64(n) => *n,
        o => panic!("bad v {o:?}"),
    }
}

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_sa_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    srv.execute_sql("CREATE TABLE s (id INT PRIMARY KEY, v INT)").unwrap();
    srv.execute_sql("INSERT INTO s VALUES (100, 0)").unwrap();
    (dir, srv)
}

#[test]
fn autocommit_failed_multi_insert_is_atomic() {
    let (dir, srv) = open("auto");
    assert!(srv.execute_sql("INSERT INTO s VALUES (1,1),(2,2),(100,3)").is_err());
    assert_eq!(count(&srv), 1);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn explicit_txn_failed_statement_rolls_back_only_itself() {
    let (dir, srv) = open("expl");
    srv.execute_sql("BEGIN").unwrap();
    srv.execute_sql("INSERT INTO s VALUES (5,5)").unwrap();
    assert!(srv.execute_sql("INSERT INTO s VALUES (1,1),(2,2),(100,3)").is_err());
    assert_eq!(count(&srv), 2, "earlier stmt must stay, failed stmt must vanish");
    srv.execute_sql("INSERT INTO s VALUES (6,6)").unwrap(); // txn still usable
    srv.execute_sql("COMMIT").unwrap();
    assert_eq!(count(&srv), 3);
    drop(srv);
    // reopen: recovery must agree
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    assert_eq!(count(&srv), 3);
    let ids: Vec<Value> = rows(&srv, "SELECT id FROM s ORDER BY id").into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int64(5), Value::Int64(6), Value::Int64(100)]);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn explicit_txn_failed_statement_restores_earlier_update() {
    let (dir, srv) = open("upd");
    srv.execute_sql("BEGIN").unwrap();
    srv.execute_sql("UPDATE s SET v = 7 WHERE id = 100").unwrap();
    assert!(srv.execute_sql("INSERT INTO s VALUES (1,1),(100,3)").is_err());
    assert_eq!(v_of(&srv, 100), 7);
    assert_eq!(count(&srv), 1);
    srv.execute_sql("ROLLBACK").unwrap();
    assert_eq!(v_of(&srv, 100), 0);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
