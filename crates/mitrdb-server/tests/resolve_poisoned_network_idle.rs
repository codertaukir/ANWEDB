#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// A network server with no client statements finishes a poisoned commit from its idle accept loop.
use mitrdb_server::{Server, TcpServerConfig};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
fn idle_accept_loop_resolves_poisoned_commit() {
    let dir = std::env::temp_dir().join(format!("mitrdb_net_idle_{}", std::process::id()));
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
    std::fs::remove_dir(&blocked).unwrap();
    assert!(
        srv.txn_manager().poisoned_commit_count() > 0,
        "premise: still pending before the network server starts"
    );

    let config = TcpServerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..TcpServerConfig::default()
    };
    let handle = srv.bind_tcp(config).unwrap();
    let t = Instant::now();
    let mut left = srv.txn_manager().poisoned_commit_count();
    while left > 0 && t.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(10));
        left = srv.txn_manager().poisoned_commit_count();
    }
    println!("IDLE_RESOLVE left={left} after={:?}", t.elapsed());
    assert_eq!(
        left, 0,
        "idle accept loop must resolve the poisoned commit without any statement"
    );
    handle.shutdown().unwrap();
    drop(handle);
    let _ = std::fs::remove_dir_all(&dir);
}
