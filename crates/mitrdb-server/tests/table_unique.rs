#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, val};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_tu_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

#[test]
fn unique_blocks_duplicates_and_sync_is_idempotent() {
    let s = open("a");
    let t = s.table("users");
    let spec = "id int primary key, email text not null unique";
    let ran = t.sync(spec).unwrap();
    assert!(ran.iter().any(|q| q.contains("CREATE UNIQUE INDEX")));
    assert!(t.sync(spec).unwrap().is_empty()); // second run does nothing

    t.insert(&[("id", val(1)), ("email", val("a@x.com"))])
        .unwrap();
    assert!(
        t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
            .is_err()
    );
    t.insert(&[("id", val(3)), ("email", val("b@x.com"))])
        .unwrap();
    assert!(
        t.update()
            .set("email", val("a@x.com"))
            .filter("id", "=", val(3))
            .run()
            .is_err()
    );
    assert_eq!(t.count().unwrap(), 2);

    // attack-like text is still just data
    t.insert(&[("id", val(4)), ("email", val("x'; DROP TABLE users; --"))])
        .unwrap();
    assert!(
        t.insert(&[("id", val(5)), ("email", val("x'; DROP TABLE users; --"))])
            .is_err()
    );
    assert_eq!(t.count().unwrap(), 3);
}

#[test]
fn removing_unique_drops_constraint_and_keeps_data() {
    let s = open("b");
    let t = s.table("users");
    t.sync("id int primary key, email text not null unique")
        .unwrap();
    t.insert(&[("id", val(1)), ("email", val("a@x.com"))])
        .unwrap();
    assert!(
        t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
            .is_err()
    );

    let ran = t.sync("id int primary key, email text not null").unwrap();
    assert!(ran.iter().any(|q| q.contains("DROP INDEX users_email_uq")));
    assert!(
        t.sync("id int primary key, email text not null")
            .unwrap()
            .is_empty()
    );

    // constraint gone, old row kept
    t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
        .unwrap();
    assert_eq!(t.count().unwrap(), 2);
}

#[test]
fn unique_to_plain_index_swaps_index() {
    let s = open("b2");
    let t = s.table("users");
    t.sync("id int primary key, email text not null unique")
        .unwrap();
    t.insert(&[("id", val(1)), ("email", val("a@x.com"))])
        .unwrap();
    let ran = t
        .sync("id int primary key, email text not null index")
        .unwrap();
    assert!(ran.iter().any(|q| q.contains("DROP INDEX users_email_uq")));
    assert!(
        ran.iter()
            .any(|q| q.contains("CREATE INDEX users_email_idx"))
    );
    t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
        .unwrap();
    assert!(
        t.sync("id int primary key, email text not null index")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn adding_unique_over_existing_duplicates_fails_cleanly() {
    let s = open("c");
    let t = s.table("users");
    t.sync("id int primary key, email text not null").unwrap();
    t.insert(&[("id", val(1)), ("email", val("a@x.com"))])
        .unwrap();
    t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
        .unwrap();
    assert!(
        t.sync("id int primary key, email text not null unique")
            .is_err()
    );
    assert_eq!(t.count().unwrap(), 2); // data untouched
}

#[test]
fn adding_unique_on_clean_data_works() {
    let s = open("d");
    let t = s.table("users");
    t.sync("id int primary key, email text not null").unwrap();
    t.insert(&[("id", val(1)), ("email", val("a@x.com"))])
        .unwrap();
    t.sync("id int primary key, email text not null unique")
        .unwrap();
    assert!(
        t.insert(&[("id", val(2)), ("email", val("a@x.com"))])
            .is_err()
    );
}
