use mitrdb_exec::ExecutionResult;
use mitrdb_server::{IsolationLevel, Server, Value};
use std::path::PathBuf;

const SEG: u64 = 1 << 20; // 1 MB segments so rotation happens

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_ck_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    (dir, srv)
}

fn seg(srv: &Server) -> usize {
    srv.wal_manager().unwrap().segment_count()
}

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
        let vals: Vec<String> = (i..end).map(|k| format!("({},'user_{}')", base + k, k)).collect();
        srv.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        i = end;
    }
    for k in 0..n {
        srv.execute_sql(&format!("DELETE FROM t WHERE id = {}", base + k)).unwrap();
    }
    for k in 0..n {
        srv.execute_sql(&format!("INSERT INTO t VALUES ({}, 'user_{}')", base + k, k)).unwrap();
    }
}

#[test]
fn checkpoint_reclaims_segments_without_active_txn() {
    let (dir, srv) = open("rec");
    churn(&srv, 0, 2000);
    let before = seg(&srv);
    eprintln!("RECLAIM before: segments={} ids={:?}", before, srv.wal_manager().unwrap().segment_ids());
    srv.checkpoint().unwrap();
    let after = seg(&srv);
    eprintln!("RECLAIM after:  segments={} ids={:?}", after, srv.wal_manager().unwrap().segment_ids());
    assert_eq!(count(&srv), 2000);
    assert!(before > 1, "premise: 1MB segments must have rotated");
    assert!(after < before, "checkpoint should reclaim old segments");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn active_txn_pins_wal_then_releases() {
    let (dir, srv) = open("pin");
    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "INSERT INTO t VALUES (900000, 'pinned')").unwrap();
    churn(&srv, 0, 2000);
    srv.checkpoint().unwrap();
    let pinned = seg(&srv);
    eprintln!("PIN with active txn: segments={}", pinned);
    srv.execute_in_session(s1, "COMMIT").unwrap();
    srv.checkpoint().unwrap();
    let released = seg(&srv);
    eprintln!("PIN after commit+checkpoint: segments={}", released);
    assert!(released < pinned, "after the txn ends the WAL must be reclaimable");
    assert_eq!(count(&srv), 2001);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn crash_recovery_with_active_txn_across_checkpoint_and_reclaim() {
    let (dir, srv) = open("crash");
    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "INSERT INTO t VALUES (900000, 'never-committed')").unwrap();
    churn(&srv, 0, 2000);
    srv.checkpoint().unwrap();
    churn(&srv, 5000, 500);
    eprintln!("CRASH segments before drop={}", seg(&srv));
    drop(srv); // s1 never committed

    let (srv, report) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    eprintln!("CRASH recovery report: {:?}", report);
    assert_eq!(count(&srv), 2500, "uncommitted row must be gone, committed rows intact");
    match srv.execute_sql("SELECT COUNT(*) FROM t WHERE id = 900000").unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Int64(0)),
        o => panic!("bad {o:?}"),
    }
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

// Auto-checkpoint: WAL stays bounded without a manual CHECKPOINT.
#[test]
fn auto_checkpoint_bounds_wal_growth() {
    let (dir, srv) = open("auto");
    srv.set_auto_checkpoint_wal_bytes(4 * SEG);
    churn(&srv, 0, 2000);
    churn(&srv, 10_000, 2000);
    let s = seg(&srv);
    eprintln!("AUTO segments after two churns = {s} (without auto-checkpoint this would be ~22)");
    assert!(s <= 8, "auto-checkpoint should keep the WAL bounded, got {s}");
    assert_eq!(count(&srv), 4000);
    drop(srv);

    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    assert_eq!(count(&srv), 4000);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

// Auto-checkpoint with a long-running txn pinning the WAL: stays correct, txn still commits.
#[test]
fn auto_checkpoint_with_active_txn_stays_correct() {
    let (dir, srv) = open("autotxn");
    srv.set_auto_checkpoint_wal_bytes(4 * SEG);
    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "INSERT INTO t VALUES (900000, 'pinned')").unwrap();
    churn(&srv, 0, 2000);
    eprintln!("AUTOTXN segments with txn open = {}", seg(&srv));
    srv.execute_in_session(s1, "COMMIT").unwrap();
    assert_eq!(count(&srv), 2001);
    drop(srv);

    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    assert_eq!(count(&srv), 2001);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

// Auto-checkpoint takes the buffer-pool exclusive op lock: several sessions at once must not deadlock.
#[test]
fn auto_checkpoint_under_concurrent_sessions() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let (dir, srv) = open("conc");
    srv.set_auto_checkpoint_wal_bytes(3 * SEG);
    let srv = Arc::new(srv);
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    let mut handles = Vec::new();
    for t in 0..4i64 {
        let srv = Arc::clone(&srv);
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let sid = srv.create_session_with_isolation(IsolationLevel::Serializable);
            let base = t * 100_000;
            let n = 1000i64;
            let run = || -> Result<(), String> {
                let mut i = 0;
                while i < n {
                    let end = (i + 250).min(n);
                    let vals: Vec<String> = (i..end).map(|k| format!("({},'u_{}')", base + k, k)).collect();
                    srv.execute_in_session(sid, &format!("INSERT INTO t VALUES {}", vals.join(",")))
                        .map_err(|e| format!("t{t} insert: {e:?}"))?;
                    i = end;
                }
                for k in 0..n {
                    srv.execute_in_session(sid, &format!("DELETE FROM t WHERE id = {}", base + k))
                        .map_err(|e| format!("t{t} delete {k}: {e:?}"))?;
                }
                for k in 0..n {
                    srv.execute_in_session(sid, &format!("INSERT INTO t VALUES ({}, 'u_{}')", base + k, k))
                        .map_err(|e| format!("t{t} reinsert {k}: {e:?}"))?;
                }
                Ok(())
            };
            let _ = tx.send(run());
        }));
    }
    drop(tx);
    for _ in 0..4 {
        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(r) => r.unwrap(),
            Err(e) => panic!("no result within 90s (deadlock?): {e:?}"),
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    eprintln!("CONC segments at end = {}", seg(&srv));
    assert_eq!(count(&srv), 4000);
    drop(srv);

    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 256, SEG).unwrap();
    assert_eq!(count(&srv), 4000);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
