#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{Server, TcpServerConfig, Value};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

fn count(srv: &Server) -> i64 {
    match srv.execute_sql("SELECT COUNT(*) FROM s").unwrap() {
        ExecutionResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            o => panic!("bad count {o:?}"),
        },
        o => panic!("bad result {o:?}"),
    }
}

#[test]
fn network_accept_loop_idle_resolves_poisoned() {
    let dir = std::env::temp_dir().join(format!("mitrdb_net_idle_resolve_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) =
        Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 64, 64).unwrap();
    let srv = Arc::new(srv);

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

    // Heal disk while server is idle
    std::fs::remove_dir(&blocked).unwrap();
    assert!(
        srv.txn_manager().poisoned_commit_count() > 0,
        "idle server must not resolve without ticks"
    );

    let cfg = TcpServerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let handle = srv.bind_tcp(cfg).expect("bind_tcp with ephemeral port");

    // Poll until accept loop idle tick resolves the commit automatically
    let mut resolved = false;
    for _ in 0..100 {
        if srv.txn_manager().poisoned_commit_count() == 0 {
            resolved = true;
            break;
        }
        thread::sleep(Duration::from_millis(15));
    }
    assert!(
        resolved,
        "network accept loop must resolve poisoned commits in idle"
    );

    handle.shutdown().expect("clean shutdown");

    // Verify row is committed and lock released
    srv.execute_sql("UPDATE s SET v = 9 WHERE id = 1").unwrap();
    assert_eq!(count(&srv), 2);

    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
