#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, Table, Value, val};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_ui_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

fn age(t: &Table, id: i32) -> Value {
    t.find().filter("id", "=", val(id)).run().unwrap().rows[0][1].clone()
}

#[test]
fn plus_minus_and_null_work() {
    let s = open("a");
    let t = s.table("c");
    t.sync("id int primary key, age int").unwrap();
    t.insert(&[("id", val(1)), ("age", val(10))]).unwrap();
    t.insert(&[("id", val(2)), ("age", val(20))]).unwrap();
    t.insert(&[("id", val(3))]).unwrap();
    s.execute_sql("UPDATE c SET age = age + 5 WHERE id = 1")
        .unwrap();
    assert_eq!(age(&t, 1), Value::Int64(15));
    s.execute_sql("UPDATE c SET age = age - 20 WHERE id = 1")
        .unwrap();
    assert_eq!(age(&t, 1), Value::Int64(-5));
    s.execute_sql("UPDATE c SET age = age+1").unwrap();
    assert_eq!(age(&t, 1), Value::Int64(-4));
    assert_eq!(age(&t, 2), Value::Int64(21));
    assert_eq!(age(&t, 3), Value::Null);
}

#[test]
fn overflow_is_an_error_and_changes_nothing() {
    let s = open("b");
    let t = s.table("o");
    t.sync("id int primary key, age int").unwrap();
    t.insert(&[("id", val(1)), ("age", val(i64::MAX))]).unwrap();
    t.insert(&[("id", val(2)), ("age", val(5))]).unwrap();
    assert!(s.execute_sql("UPDATE o SET age = age + 1").is_err());
    assert_eq!(age(&t, 1), Value::Int64(i64::MAX));
    assert_eq!(age(&t, 2), Value::Int64(5));
    assert!(
        s.execute_sql("UPDATE o SET age = age + 99999999999999999999")
            .is_err()
    );
    assert_eq!(age(&t, 2), Value::Int64(5));
}

#[test]
fn bad_forms_and_injection_are_rejected() {
    let s = open("c");
    let t = s.table("x");
    t.sync("id int primary key, age int, name text").unwrap();
    t.insert(&[("id", val(1)), ("age", val(1)), ("name", val("n"))])
        .unwrap();
    assert!(s.execute_sql("UPDATE x SET name = name + 1").is_err());
    assert!(
        s.execute_sql("UPDATE x SET age = age + 1; DROP TABLE x")
            .is_err()
    );
    assert!(s.execute_sql("UPDATE x SET age = age + 1 + 2").is_err());
    assert!(s.execute_sql("UPDATE x SET age = age + -1").is_err());
    assert!(
        s.execute_sql("UPDATE x SET age = 1, age = age + 1")
            .is_err()
    );
    assert_eq!(age(&t, 1), Value::Int64(1));
    assert_eq!(t.find().run().unwrap().rows.len(), 1);
}

#[test]
fn text_with_plus_stays_text_and_mixed_set_works() {
    let s = open("d");
    let t = s.table("m");
    t.sync("id int primary key, age int, name text").unwrap();
    t.insert(&[("id", val(1)), ("age", val(1)), ("name", val("n"))])
        .unwrap();
    s.execute_sql("UPDATE m SET name = 'age + 1', age = age + 2 WHERE id = 1")
        .unwrap();
    let row = t.find().run().unwrap().rows[0].clone();
    assert_eq!(row[1], Value::Int64(3));
    assert_eq!(row[2], Value::Text("age + 1".to_string()));
}
