#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server, Value};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_ic_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    s
}

fn count(s: &Server) -> usize {
    s.table("t").find().run().unwrap().rows.len()
}

#[test]
fn columns_in_any_order_missing_become_null() {
    let s = open("a");
    let r = s
        .execute_sql("INSERT INTO t (name, id) VALUES ('a', 1)")
        .unwrap();
    assert!(matches!(r, ExecutionResult::Affected(1)));
    let rows = s.table("t").find().run().unwrap().rows;
    assert_eq!(
        rows[0],
        vec![Value::Int64(1), Value::Text("a".to_string()), Value::Null]
    );
    s.execute_sql("insert into t (age, id, name) values (30, 2, 'b')")
        .unwrap();
    assert_eq!(count(&s), 2);
}

#[test]
fn many_rows_with_column_list() {
    let s = open("b");
    let r = s
        .execute_sql("INSERT INTO t (id, name) VALUES (1,'a,b'),(2,'it''s'),(3,NULL);")
        .unwrap();
    assert!(matches!(r, ExecutionResult::Affected(3)));
    assert_eq!(count(&s), 3);
}

#[test]
fn bad_forms_are_rejected() {
    let s = open("c");
    let many = (0..1001)
        .map(|i| format!("({i},'x')"))
        .collect::<Vec<_>>()
        .join(",");
    let big = format!("INSERT INTO t (id, name) VALUES {many}");
    for q in [
        "INSERT INTO t (id, nope) VALUES (1,'a')",
        "INSERT INTO t (id, id) VALUES (1,2)",
        "INSERT INTO t (name) VALUES ('x')",
        "INSERT INTO t (id, name) VALUES (1)",
        "INSERT INTO t (id, name) VALUES (1,'a',3)",
        "INSERT INTO t (id,) VALUES (1)",
        "INSERT INTO t () VALUES (1)",
        "INSERT INTO t (id name) VALUES (1)",
        "INSERT INTO t (id, name VALUES (1,'a')",
        "INSERT INTO t (id, name) VALUES (1,'a'),",
        "INSERT INTO t (id, name) VALUES (1,'a'); DROP TABLE t",
        "INSERT INTO t (id, name) VALUES ('x','a')",
        "INSERT INTO t (id, name) VALUES",
        &big,
    ] {
        assert!(
            s.execute_sql(q).is_err(),
            "should fail: {}",
            &q[..q.len().min(60)]
        );
    }
    assert_eq!(count(&s), 0);
    s.execute_sql("INSERT INTO t VALUES (1,'plain',5)").unwrap();
    assert_eq!(count(&s), 1);
}

#[test]
fn one_bad_row_rolls_back_all() {
    let s = open("d");
    s.execute_sql("INSERT INTO t (id) VALUES (1)").unwrap();
    assert!(
        s.execute_sql("INSERT INTO t (id, name) VALUES (2,'a'),(1,'dup')")
            .is_err()
    );
    assert_eq!(count(&s), 1);
}
