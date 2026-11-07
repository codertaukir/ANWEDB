use mitrdb_exec::ExecutionResult;
use mitrdb_server::{IsolationLevel, Server, Value};
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

fn ids(srv: &Server) -> Vec<Value> {
    rows(srv, "SELECT id FROM s ORDER BY id")
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

// earlier.contains branch: failed statement re-writes a key this txn already wrote (DELETE).
#[test]
fn explicit_txn_failed_statement_after_own_delete_restores_chain() {
    let (dir, srv) = open("owndel");
    srv.execute_sql("INSERT INTO s VALUES (200, 0)").unwrap();

    srv.execute_sql("BEGIN").unwrap();
    srv.execute_sql("DELETE FROM s WHERE id = 100").unwrap();
    assert_eq!(count(&srv), 1);
    // 100 re-inserted + 1 inserted, then 200 is a duplicate -> statement fails
    assert!(srv
        .execute_sql("INSERT INTO s VALUES (100,9),(1,1),(200,3)")
        .is_err());
    assert_eq!(rows(&srv, "SELECT v FROM s WHERE id = 100").len(), 0, "100 must stay deleted");
    assert_eq!(rows(&srv, "SELECT v FROM s WHERE id = 1").len(), 0, "1 must vanish");
    assert_eq!(ids(&srv), vec![Value::Int64(200)]);

    // txn still usable, and key 100 can be re-inserted cleanly
    srv.execute_sql("INSERT INTO s VALUES (100, 5)").unwrap();
    assert_eq!(v_of(&srv, 100), 5);
    srv.execute_sql("ROLLBACK").unwrap();

    // after ROLLBACK the original committed state is back
    assert_eq!(ids(&srv), vec![Value::Int64(100), Value::Int64(200)]);
    assert_eq!(v_of(&srv, 100), 0);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn explicit_txn_failed_statement_after_own_delete_commit_and_reopen() {
    let (dir, srv) = open("owndel2");
    srv.execute_sql("INSERT INTO s VALUES (200, 0)").unwrap();

    srv.execute_sql("BEGIN").unwrap();
    srv.execute_sql("DELETE FROM s WHERE id = 100").unwrap();
    assert!(srv
        .execute_sql("INSERT INTO s VALUES (100,9),(1,1),(200,3)")
        .is_err());
    srv.execute_sql("COMMIT").unwrap();
    assert_eq!(ids(&srv), vec![Value::Int64(200)]);
    drop(srv);

    // recovery must agree with the pre-crash state
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    assert_eq!(ids(&srv), vec![Value::Int64(200)]);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

fn ids_in(srv: &Server, sid: u64) -> Vec<Value> {
    match srv.execute_in_session(sid, "SELECT id FROM s ORDER BY id").unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.into_iter().map(|r| r[0].clone()).collect(),
        o => panic!("bad result {o:?}"),
    }
}

// Failed statement inside an explicit txn because of a lock timeout:
// its partial writes vanish, earlier statements stay, txn stays alive.
#[test]
fn lock_timeout_statement_rollback_keeps_txn_alive() {
    let (dir, srv) = open("lt");
    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "DELETE FROM s WHERE id = 100").unwrap();

    srv.execute_in_session(s2, "BEGIN").unwrap();
    srv.execute_in_session(s2, "INSERT INTO s VALUES (5, 5)").unwrap();
    let err = srv
        .execute_in_session(s2, "INSERT INTO s VALUES (1,1),(2,2),(100,3)")
        .unwrap_err();
    eprintln!("ERR={err:?}");

    srv.execute_in_session(s1, "ROLLBACK").unwrap();

    // partial rows 1 and 2 must be gone, earlier row 5 stays, committed 100 untouched
    assert_eq!(ids_in(&srv, s2), vec![Value::Int64(5), Value::Int64(100)]);
    srv.execute_in_session(s2, "INSERT INTO s VALUES (1, 1)").unwrap(); // txn alive
    srv.execute_in_session(s2, "COMMIT").unwrap();
    assert_eq!(ids(&srv), vec![Value::Int64(1), Value::Int64(5), Value::Int64(100)]);
    drop(srv);

    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    assert_eq!(ids(&srv), vec![Value::Int64(1), Value::Int64(5), Value::Int64(100)]);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
