#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, val};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_un_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

fn rows(t: &mitrdb_server::Table) -> usize {
    t.find().run().unwrap().rows.len()
}

#[test]
fn many_nulls_allowed_but_real_duplicates_rejected() {
    let s = open("a");
    let t = s.table("u");
    t.sync("id int primary key, email text unique").unwrap();
    t.insert(&[("id", val(1))]).unwrap();
    t.insert(&[("id", val(2))]).unwrap();
    t.insert(&[("id", val(3)), ("email", val("a@x"))]).unwrap();
    assert!(t.insert(&[("id", val(4)), ("email", val("a@x"))]).is_err());
    assert_eq!(rows(&t), 3);
    t.delete().filter("id", "=", val(1)).run().unwrap();
    assert_eq!(rows(&t), 2);
    t.insert(&[("id", val(5))]).unwrap();
    assert!(t.insert(&[("id", val(6)), ("email", val("a@x"))]).is_err());
    assert_eq!(rows(&t), 3);
}

#[test]
fn unique_can_be_added_when_table_already_has_many_nulls() {
    let s = open("b");
    let t = s.table("v");
    t.sync("id int primary key, email text").unwrap();
    t.insert(&[("id", val(1))]).unwrap();
    t.insert(&[("id", val(2))]).unwrap();
    t.sync("id int primary key, email text unique").unwrap();
    t.insert(&[("id", val(3))]).unwrap();
    t.insert(&[("id", val(4)), ("email", val("z@x"))]).unwrap();
    assert!(t.insert(&[("id", val(5)), ("email", val("z@x"))]).is_err());
    assert_eq!(rows(&t), 4);
}

#[test]
fn update_to_null_twice_is_allowed() {
    let s = open("c");
    let t = s.table("w");
    t.sync("id int primary key, email text unique").unwrap();
    t.insert(&[("id", val(1)), ("email", val("a"))]).unwrap();
    t.insert(&[("id", val(2)), ("email", val("b"))]).unwrap();
    s.execute_sql("UPDATE w SET email = NULL WHERE id = 1")
        .unwrap();
    s.execute_sql("UPDATE w SET email = NULL WHERE id = 2")
        .unwrap();
    assert!(
        s.execute_sql("UPDATE w SET email = 'c' WHERE id = 1")
            .is_ok()
    );
    assert!(
        s.execute_sql("UPDATE w SET email = 'c' WHERE id = 2")
            .is_err()
    );
}
