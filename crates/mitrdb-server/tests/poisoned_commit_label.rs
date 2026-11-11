#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// Server: a WAL flush I/O failure (next segment path blocked) must surface as IO_ERROR,
// not be mislabeled WAL_CORRUPTED, in the poisoned-commit error.
use mitrdb_server::Server;
use std::path::{Path, PathBuf};

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
fn wal_flush_io_failure_is_labeled_io_error_not_wal_corrupted() {
    let dir = std::env::temp_dir().join(format!("mitrdb_srv_poison_label_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) =
        Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 64, 64).unwrap();
    srv.execute_sql("CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    srv.execute_sql("INSERT INTO s VALUES (100, 0)").unwrap();

    let blocked = next_segment_path(&dir);
    std::fs::create_dir(&blocked).unwrap();
    let r = srv.execute_sql("INSERT INTO s VALUES (1, 1)");
    let msg = match r {
        Ok(_) => panic!("insert must fail while the next WAL segment is unwritable"),
        Err(e) => e.to_string(),
    };
    println!("error while WAL blocked = {msg}");
    assert!(msg.contains("IO_ERROR"), "expected IO_ERROR in: {msg}");
    assert!(
        !msg.contains("WAL_CORRUPTED"),
        "I/O failure mislabeled as WAL_CORRUPTED: {msg}"
    );

    std::fs::remove_dir(&blocked).unwrap();
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
