// Server: a commit whose WAL flush fails (next segment path blocked) is poisoned;
// once the WAL heals, the next statement resolves it automatically and releases its locks.
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
fn poisoned_commit_is_resolved_by_next_statement_after_wal_heals() {
    let dir = std::env::temp_dir().join(format!("mitrdb_srv_poison_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 64, 64).unwrap();
    srv.execute_sql("CREATE TABLE s (id INT PRIMARY KEY, v INT)").unwrap();
    srv.execute_sql("INSERT INTO s VALUES (100, 0)").unwrap();

    let blocked = next_segment_path(&dir);
    std::fs::create_dir(&blocked).unwrap();
    let r = srv.execute_sql("INSERT INTO s VALUES (1, 1)");
    println!("INSERT while WAL blocked -> {:?}", r.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    assert!(r.is_err(), "insert must fail while the next WAL segment is unwritable");

    std::fs::remove_dir(&blocked).unwrap();
    // next statement sweeps first: the poisoned commit completes
    let c = count(&srv);
    println!("count after heal = {c}");
    assert_eq!(c, 2, "poisoned commit should have been resolved by the next statement");
    // its locks are gone: same key can be updated
    srv.execute_sql("UPDATE s SET v = 9 WHERE id = 1").unwrap();
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
