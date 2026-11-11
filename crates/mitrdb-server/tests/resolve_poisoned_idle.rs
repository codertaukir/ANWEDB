#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// An idle server (no next statement) can finish a poisoned commit via Server::resolve_poisoned().
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};
use std::path::{Path, PathBuf};

fn count(srv: &Server) -> i64 {
    match srv.execute_sql("SELECT COUNT(*) FROM s").unwrap() {
        ExecutionResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            o => panic!("bad count {o:?}"),
        },
        o => panic!("bad result {o:?}"),
    }
}

fn next_segment_path(dir: &Path) -> PathBuf {
    let mut max = 0u64;
    for e in std::fs::read_dir(dir).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().to_string();
        if let Some(suffix) = name.strip_prefix("t.wal.") {
            if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                max = max.max(suffix.parse::<u64>().unwrap());
            }
        }
    }
    dir.join(format!("t.wal.{:08}", max + 1))
}

#[test]
fn resolve_poisoned_finishes_commit_without_a_statement() {
    let dir = std::env::temp_dir().join(format!("mitrdb_resolve_idle_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) =
        Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 64, 64).unwrap();
    srv.execute_sql("CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    srv.execute_sql("INSERT INTO s VALUES (100, 0)").unwrap();

    let blocked = next_segment_path(&dir);
    std::fs::create_dir(&blocked).unwrap();
    assert!(
        srv.execute_sql("INSERT INTO s VALUES (1, 1)").is_err(),
        "premise: insert must fail"
    );
    assert!(
        srv.txn_manager().poisoned_commit_count() > 0,
        "premise: commit must be poisoned"
    );

    // still blocked: nothing can be resolved
    assert_eq!(
        srv.resolve_poisoned(),
        0,
        "WAL still blocked, nothing may resolve"
    );
    assert!(srv.txn_manager().poisoned_commit_count() > 0);

    std::fs::remove_dir(&blocked).unwrap();
    // healed but idle: no statement ran, so it is still pending
    assert!(
        srv.txn_manager().poisoned_commit_count() > 0,
        "idle server must not resolve by itself"
    );

    let n = srv.resolve_poisoned();
    println!("RESOLVED {n}");
    assert!(n >= 1, "resolve_poisoned must finish the commit");
    assert_eq!(srv.txn_manager().poisoned_commit_count(), 0);

    srv.execute_sql("UPDATE s SET v = 9 WHERE id = 1").unwrap(); // locks released
    assert_eq!(count(&srv), 2);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
