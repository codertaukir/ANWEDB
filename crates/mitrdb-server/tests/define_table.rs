#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, val};

#[test]
fn define_table_builds_schema_and_is_repeatable() {
    let dir = std::env::temp_dir().join(format!("anwedb_define_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let users = server
        .define_table("users", |t| {
            t.id().primary_key();
            t.string("name").not_null();
            t.integer("age");
            t.string("email").unique();
        })
        .unwrap();

    users
        .insert(&[
            ("id", val(1)),
            ("name", val("Taukir")),
            ("age", val(20)),
            ("email", val("a@x.com")),
        ])
        .unwrap();
    assert_eq!(users.find().filter("age", "=", val(20)).count().unwrap(), 1);

    // unique email
    assert!(
        users
            .insert(&[
                ("id", val(2)),
                ("name", val("B")),
                ("email", val("a@x.com"))
            ])
            .is_err()
    );
    // name NOT NULL
    assert!(
        users
            .insert(&[("id", val(3)), ("email", val("c@x.com"))])
            .is_err()
    );

    // same definition again: nothing changes
    server
        .define_table("users", |t| {
            t.id().primary_key();
            t.string("name").not_null();
            t.integer("age");
            t.string("email").unique();
        })
        .unwrap();
    assert_eq!(users.count().unwrap(), 1);

    // extra nullable column added, data kept
    let users = server
        .define_table("users", |t| {
            t.id().primary_key();
            t.string("name").not_null();
            t.integer("age");
            t.string("email").unique();
            t.boolean("active");
        })
        .unwrap();
    assert_eq!(users.count().unwrap(), 1);
    users
        .update()
        .set("active", val(true))
        .filter("id", "=", val(1))
        .run()
        .unwrap();

    // bad definitions refused
    assert!(
        server
            .define_table("users", |t| {
                t.string("bad name");
            })
            .is_err()
    );
    assert!(server.define_table("empty_t", |_t| {}).is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
