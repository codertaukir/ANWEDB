#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// A failed auto-checkpoint must not fail user statements, and must show up in metrics.
// Keep this file to a single test: metrics are process-global.
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};

const SEG: u64 = 1 << 20; // 1 MB segments so auto-checkpoint triggers

fn count(srv: &Server) -> i64 {
    match srv.execute_sql("SELECT COUNT(*) FROM t").unwrap() {
        ExecutionResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            o => panic!("bad {o:?}"),
        },
        o => panic!("bad {o:?}"),
    }
}

fn churn(srv: &Server, base: i64, n: i64) {
    let mut i = 0;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end)
            .map(|k| format!("({},'user_{}')", base + k, k))
            .collect();
        srv.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(",")))
            .unwrap();
        i = end;
    }
    for k in 0..n {
        srv.execute_sql(&format!("DELETE FROM t WHERE id = {}", base + k))
            .unwrap();
    }
    for k in 0..n {
        srv.execute_sql(&format!(
            "INSERT INTO t VALUES ({}, 'user_{}')",
            base + k,
            k
        ))
        .unwrap();
    }
}

#[test]
fn failed_auto_checkpoint_is_counted_and_does_not_fail_statements() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ckpterr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) =
        Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();

    // Block the checkpoint metadata tmp file: a directory sits at its path.
    let blocked = dir.join("t.wal.ckpt.tmp");
    std::fs::create_dir(&blocked).unwrap();
    assert!(
        srv.checkpoint().is_err(),
        "premise: checkpoint must fail while the metadata tmp path is blocked"
    );

    srv.set_auto_checkpoint_wal_bytes(4 * SEG);
    let e0 = srv.metrics_snapshot().checkpoint_errors;
    churn(&srv, 0, 2000);
    churn(&srv, 100000, 2000);
    let e1 = srv.metrics_snapshot().checkpoint_errors;
    println!("CKPTERR before={e0} after={e1}");
    assert_eq!(count(&srv), 4000, "user statements must keep working");
    assert!(
        e1 > e0,
        "failed auto-checkpoints must be counted (errors {e0} -> {e1})"
    );

    // Heal: checkpoint works again.
    std::fs::remove_dir(&blocked).unwrap();
    srv.checkpoint().unwrap();
    assert_eq!(count(&srv), 4000);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
