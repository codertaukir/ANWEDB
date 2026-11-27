#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server, val};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_sp_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

#[test]
fn injection_strings_are_plain_data() {
    let d = dir("inj");
    let s = open(&d);
    let t = s.table("users");
    t.sync("id int primary key, name text").unwrap();
    t.insert(&[("id", val(1)), ("name", val("x"))]).unwrap();
    t.insert(&[("id", val(2)), ("name", val("y"))]).unwrap();

    for evil in [
        "x' OR '1'='1",
        "a;b",
        "x\" OR \"1\"=\"1",
        "q AND r OR s",
        "$1",
        "'; DROP TABLE users; --",
    ] {
        assert_eq!(
            t.find().filter("name", "=", val(evil)).count().unwrap(),
            0,
            "{evil}"
        );
        assert_eq!(
            t.delete().filter("name", "=", val(evil)).run().unwrap(),
            0,
            "{evil}"
        );
    }
    assert_eq!(t.find().count().unwrap(), 2);

    let weird = "it's $1; DROP TABLE users -- AND x OR y";
    assert_eq!(
        t.update()
            .set("name", val(weird))
            .filter("id", "=", val(1))
            .run()
            .unwrap(),
        1
    );
    assert_eq!(t.find().filter("name", "=", val(weird)).count().unwrap(), 1);
    assert_eq!(t.find().count().unwrap(), 2);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn execute_sql_params_binds_and_validates_counts() {
    let d = dir("bind");
    let s = open(&d);
    let t = s.table("t");
    t.sync("id int primary key, name text").unwrap();

    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($1, $2)", &[val(1), val("a'b")])
            .is_ok()
    );
    assert_eq!(t.find().filter("name", "=", val("a'b")).count().unwrap(), 1);

    // quoted $1 stays literal text, not a placeholder
    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($1, '$1')", &[val(2)])
            .is_ok()
    );
    assert_eq!(t.find().filter("name", "=", val("$1")).count().unwrap(), 1);

    // missing / extra / bad placeholders
    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($1, $2)", &[val(3)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($1, 'z')", &[val(3), val(4)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($0, 'z')", &[val(3)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("INSERT INTO t VALUES ($x, 'z')", &[val(3)])
            .is_err()
    );

    // plain execute_sql must not accept placeholders
    assert!(s.execute_sql("INSERT INTO t VALUES ($1, 'z')").is_err());
    assert_eq!(t.find().count().unwrap(), 2);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn execute_sql_params_supports_limit_and_offset() {
    let d = dir("lim_off");
    let s = open(&d);
    let t = s.table("msgs");
    t.sync("id int primary key, room int, body text").unwrap();
    for id in 1..=6 {
        t.insert(&[
            ("id", val(id)),
            ("room", val(if id <= 4 { 1 } else { 2 })),
            ("body", val(format!("m{id}"))),
        ])
        .unwrap();
    }

    let ids_from = |sql: &str, params: &[mitrdb_server::Value]| -> Vec<i64> {
        match s.execute_sql_params(sql, params).unwrap() {
            ExecutionResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|r| match r[0] {
                    mitrdb_server::Value::Int64(n) => n,
                    ref other => panic!("expected Int64, got {other:?}"),
                })
                .collect(),
            other => panic!("expected Rows, got {other:?}"),
        }
    };

    assert_eq!(
        ids_from("SELECT id FROM msgs ORDER BY id ASC LIMIT $1", &[val(2)]),
        vec![1, 2]
    );
    assert_eq!(
        ids_from("SELECT id FROM msgs ORDER BY id ASC LIMIT $1", &[val(0)]),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids_from("SELECT id FROM msgs ORDER BY id ASC OFFSET $1", &[val(4)]),
        vec![5, 6]
    );
    assert_eq!(
        ids_from(
            "SELECT id FROM msgs WHERE room = $1 ORDER BY id DESC LIMIT $2 OFFSET $3",
            &[val(1), val(2), val(1)],
        ),
        vec![3, 2]
    );
    assert_eq!(
        ids_from(
            "SELECT id FROM msgs WHERE room = $1 UNION ALL SELECT id FROM msgs WHERE room = $2 ORDER BY id ASC LIMIT $3 OFFSET $4",
            &[val(1), val(2), val(2), val(3)],
        ),
        vec![4, 5]
    );

    assert!(s.execute_sql("SELECT id FROM msgs LIMIT $1").is_err());
    assert!(s.execute_sql("SELECT id FROM msgs OFFSET $1").is_err());
    assert!(
        s.execute_sql_params("SELECT id FROM msgs LIMIT $1", &[val(-1)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("SELECT id FROM msgs OFFSET $1", &[val(-5)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("SELECT id FROM msgs LIMIT $1", &[val("2")])
            .is_err()
    );
    assert!(
        s.execute_sql_params("SELECT id FROM msgs LIMIT $1 OFFSET $2", &[val(2)])
            .is_err()
    );
    assert!(
        s.execute_sql_params("SELECT id FROM msgs LIMIT $1", &[val(2), val(1)])
            .is_err()
    );

    let _ = std::fs::remove_dir_all(&d);
}
