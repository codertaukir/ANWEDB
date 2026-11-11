#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, val};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_si_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

#[test]
fn index_token_creates_non_unique_index_and_is_idempotent() {
    let d = dir("a");
    let s = open(&d);
    let t = s.table("users");
    let spec = "id int primary key, name text index";
    let ran = t.sync(spec).unwrap();
    assert!(
        ran.iter()
            .any(|q| q.contains("CREATE INDEX users_name_idx"))
    );
    assert!(!ran.iter().any(|q| q.contains("UNIQUE")));
    assert!(t.sync(spec).unwrap().is_empty());

    // duplicates allowed (non-unique)
    t.insert(&[("id", val(1)), ("name", val("a"))]).unwrap();
    t.insert(&[("id", val(2)), ("name", val("a"))]).unwrap();
    t.insert(&[("id", val(3)), ("name", val("b"))]).unwrap();
    assert_eq!(t.find().filter("name", "=", val("a")).count().unwrap(), 2);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn index_added_over_existing_data() {
    let d = dir("b");
    let s = open(&d);
    let t = s.table("users");
    t.sync("id int primary key, name text").unwrap();
    for i in 1..=20 {
        t.insert(&[("id", val(i as i64)), ("name", val(format!("n{}", i % 4)))])
            .unwrap();
    }
    let ran = t.sync("id int primary key, name text index").unwrap();
    assert!(
        ran.iter()
            .any(|q| q.contains("CREATE INDEX users_name_idx"))
    );
    assert_eq!(t.find().filter("name", "=", val("n1")).count().unwrap(), 5);
    assert_eq!(t.count().unwrap(), 20);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn removing_index_is_refused_and_unique_wins_over_index() {
    let d = dir("c");
    let s = open(&d);
    let t = s.table("users");
    t.sync("id int primary key, name text index").unwrap();
    assert!(t.sync("id int primary key, name text").is_err());

    let t2 = s.table("accounts");
    let ran = t2
        .sync("id int primary key, email text unique index")
        .unwrap();
    assert!(ran.iter().any(|q| q.contains("CREATE UNIQUE INDEX")));
    assert!(!ran.iter().any(|q| q.contains("accounts_email_idx")));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn define_table_index_builder_and_restart_persist() {
    let d = dir("d");
    {
        let s = open(&d);
        let t = s
            .define_table("users", |t| {
                t.id().primary_key();
                t.string("name").index();
                t.integer("age").index();
            })
            .unwrap();
        t.insert(&[("id", val(1)), ("name", val("x")), ("age", val(30))])
            .unwrap();
        t.insert(&[("id", val(2)), ("name", val("x")), ("age", val(30))])
            .unwrap();
    }
    {
        let s = open(&d);
        let t = s.table("users");
        // already in line with the DB after restart: nothing to run
        let ran = t
            .sync("id int primary key, name text index, age int index")
            .unwrap();
        assert!(ran.is_empty(), "unexpected plan after restart: {:?}", ran);
        assert_eq!(t.find().filter("age", "=", val(30)).count().unwrap(), 2);
    }
    let _ = std::fs::remove_dir_all(&d);
}
