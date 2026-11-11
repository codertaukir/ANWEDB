#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server};

fn setup(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_jb_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0;
    s.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    s.execute_sql("CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT)")
        .unwrap();
    s.execute_sql("INSERT INTO users VALUES (1,'a'),(2,'b')")
        .unwrap();
    s.execute_sql("INSERT INTO posts VALUES (10,1,'p1'),(11,1,'p2'),(12,3,'orphan')")
        .unwrap();
    s
}

fn rows(s: &Server, q: &str) -> usize {
    match s.execute_sql(q).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn inner_join_skips_unmatched() {
    let s = setup("a");
    assert_eq!(
        rows(
            &s,
            "SELECT * FROM users JOIN posts ON users.id = posts.user_id"
        ),
        2
    );
    assert_eq!(
        rows(
            &s,
            "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id"
        ),
        2
    );
}

#[test]
fn left_join_keeps_unmatched_left_rows() {
    let s = setup("b");
    assert_eq!(
        rows(
            &s,
            "SELECT * FROM users LEFT JOIN posts ON users.id = posts.user_id"
        ),
        3
    );
}

#[test]
fn bad_join_forms_are_rejected() {
    let s = setup("c");
    for q in [
        "SELECT * FROM users JOIN posts",
        "SELECT * FROM users JOIN nope ON users.id = nope.x",
        "SELECT * FROM users JOIN posts ON users.id = posts.user_id; DROP TABLE users",
        "SELECT * FROM users JOIN po sts ON users.id = posts.user_id",
    ] {
        assert!(s.execute_sql(q).is_err(), "should fail: {q}");
    }
    assert_eq!(rows(&s, "SELECT * FROM users"), 2);
}
