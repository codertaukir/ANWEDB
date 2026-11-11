#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::Server;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn run_with_timeout(name: &str, sql: String) {
    let dir = std::env::temp_dir().join(format!("mitrdb_ps_{}_{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (tx, rx) = mpsc::channel();
    let d = dir.clone();
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let (server, _) = Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap();
            let _ = server.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
            let started = Instant::now();
            let _ = server.execute_sql(&sql);
            let _ = tx.send(started.elapsed());
        })
        .unwrap();
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(t) => println!("{name}: finished in {t:?}"),
        Err(_) => panic!("{name}: hang or crash (no answer in 20s)"),
    }
}

#[test]
fn deep_brackets() {
    let n = 5000;
    let sql = format!(
        "SELECT * FROM t WHERE {}id = 1{}",
        "(".repeat(n),
        ")".repeat(n)
    );
    run_with_timeout("deep_brackets", sql);
}

#[test]
fn unbalanced_brackets() {
    let sql = format!("SELECT * FROM t WHERE {}id = 1", "(".repeat(5000));
    run_with_timeout("unbalanced", sql);
}

#[test]
fn many_and_or() {
    let sql = format!(
        "SELECT * FROM t WHERE id = 1{}",
        " OR id = 2 AND id = 3".repeat(20000)
    );
    run_with_timeout("many_and_or", sql);
}

#[test]
fn long_string_value() {
    let sql = format!("SELECT * FROM t WHERE name = '{}'", "a".repeat(1_000_000));
    run_with_timeout("long_string", sql);
}

#[test]
fn many_quotes() {
    let sql = format!("SELECT * FROM t WHERE name = '{}'", "''".repeat(200_000));
    run_with_timeout("many_quotes", sql);
}

#[test]
fn many_commas() {
    let sql = format!("SELECT {} FROM t", "id,".repeat(100_000) + "id");
    run_with_timeout("many_commas", sql);
}

#[test]
fn weird_bytes() {
    let sql = "SELECT * FROM t WHERE name = '\u{0}\u{202e}\u{1F600}'; \u{0}".to_string();
    run_with_timeout("weird_bytes", sql);
}
