#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{IsolationLevel, Server, Value, val};
use std::path::PathBuf;

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_dpk_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    (dir, srv)
}

fn load(srv: &Server, name: &str, n: i64) {
    srv.execute_sql(&format!(
        "CREATE TABLE {name} (id INT PRIMARY KEY, age INT)"
    ))
    .unwrap();
    let vals: Vec<String> = (0..n).map(|k| format!("({k},{})", k % 3)).collect();
    srv.execute_sql(&format!("INSERT INTO {name} VALUES {}", vals.join(",")))
        .unwrap();
}

fn ids(srv: &Server, name: &str) -> Vec<i64> {
    srv.table(name)
        .find()
        .order_by("id")
        .run()
        .unwrap()
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int64(x) => *x,
            o => panic!("unexpected id value: {o:?}"),
        })
        .collect()
}

#[test]
fn delete_where_pk_shapes() {
    let (dir, srv) = open("shapes");
    let all: Vec<i64> = (0..10).collect();
    let cases: Vec<(&str, Vec<i64>)> = vec![
        ("id = 999", vec![]),
        ("id = 5", vec![5]),
        ("id >= 7", vec![7, 8, 9]),
        ("id < 2 OR id > 7", vec![0, 1, 8, 9]),
        ("id >= 3 AND id <= 5", vec![3, 4, 5]),
        ("id = 2 OR id = 2", vec![2]),
        ("id = 1 OR id = 8", vec![1, 8]),
        ("id != 4", all.iter().copied().filter(|x| *x != 4).collect()),
        ("id >= 2 AND age = 0", vec![3, 6, 9]),
        ("id = 3 AND id = 4", vec![]),
        ("id < 0", vec![]),
        ("id > 100", vec![]),
    ];
    for (i, (wh, deleted)) in cases.iter().enumerate() {
        let name = format!("c{i}");
        load(&srv, &name, 10);
        let res = srv
            .execute_sql(&format!("DELETE FROM {name} WHERE {wh}"))
            .unwrap();
        assert_eq!(
            res,
            ExecutionResult::Affected(deleted.len()),
            "case '{wh}' affected"
        );
        let expect: Vec<i64> = all
            .iter()
            .copied()
            .filter(|x| !deleted.contains(x))
            .collect();
        assert_eq!(ids(&srv, &name), expect, "case '{wh}' remaining");
    }
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_pk_range_with_null_filter_table_api() {
    let (dir, srv) = open("null");
    let t = srv.table("n");
    t.sync("id int primary key, age int").unwrap();
    for i in 0..6i64 {
        let age = if i == 2 || i == 4 {
            Value::Null
        } else {
            val(i)
        };
        t.insert(&[("id", val(i)), ("age", age)]).unwrap();
    }
    let n = t
        .delete()
        .filter("id", ">=", val(0i64))
        .filter("age", "=", Value::Null)
        .run()
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(ids(&srv, "n"), vec![0, 1, 3, 5]);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_pk_range_serializable_phantom() {
    let (dir, srv) = open("phantom");
    load(&srv, "u", 10);
    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let res = srv
        .execute_in_session(s1, "DELETE FROM u WHERE id >= 5")
        .unwrap();
    assert_eq!(res, ExecutionResult::Affected(5));
    // insert inside the deleted range must be blocked
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO u VALUES (20, 0)"),
        Err(Error::LockTimeout)
    ));
    // insert outside the range must work
    srv.execute_in_session(s2, "INSERT INTO u VALUES (-1, 0)")
        .ok();
    srv.execute_in_session(s1, "COMMIT").unwrap();
    assert_eq!(ids(&srv, "u").into_iter().filter(|x| *x >= 5).count(), 0);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
