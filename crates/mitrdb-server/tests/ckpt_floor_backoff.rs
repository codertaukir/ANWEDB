#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, Value};

const SEG: u64 = 1 << 20;

fn count(srv: &Server) -> i64 {
    match srv.execute_sql("SELECT COUNT(*) FROM t").unwrap() {
        ExecutionResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            o => panic!("bad {o:?}"),
        },
        o => panic!("bad {o:?}"),
    }
}

fn seg(srv: &Server) -> u64 {
    srv.wal_manager().unwrap().segment_count() as u64
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

// A long txn pins the WAL, so checkpoints cannot shrink it. The floor backoff must then
// space checkpoints at least threshold/2 (= 2 segments) of real growth apart.
#[test]
fn pinned_wal_backs_off_checkpoints() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ckptfloor_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) =
        Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();

    // premise: a manual checkpoint is counted by checkpoints_created
    let c0 = srv.metrics_snapshot().checkpoints_created;
    srv.checkpoint().unwrap();
    let c1 = srv.metrics_snapshot().checkpoints_created;
    assert!(
        c1 > c0,
        "premise: manual checkpoint must raise checkpoints_created ({c0} -> {c1})"
    );

    let s1 = srv.create_session();
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "INSERT INTO t VALUES (900000, 'pinned')")
        .unwrap();

    srv.set_auto_checkpoint_wal_bytes(4 * SEG);
    let before = srv.metrics_snapshot().checkpoints_created;
    churn(&srv, 0, 2000);
    churn(&srv, 100_000, 2000);
    let made = srv.metrics_snapshot().checkpoints_created - before;
    let segs = seg(&srv);
    println!("CKPTFLOOR checkpoints={made} final_segments={segs}");

    assert!(made >= 1, "auto-checkpoint never ran: test proves nothing");
    assert!(
        made <= segs / 2 + 2,
        "floor backoff broken: {made} checkpoints for {segs} segments (max {})",
        segs / 2 + 2
    );

    srv.execute_in_session(s1, "COMMIT").unwrap();
    assert_eq!(count(&srv), 4001);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
