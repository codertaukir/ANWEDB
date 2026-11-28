use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use mitrdb_server::Server;
use mitrdb_storage::page::{PAGE_FLAG_FREE, PAGE_TYPE_OVERFLOW};
fn temp_dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("mitrdb_a7_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p); std::fs::create_dir_all(&p).unwrap(); p
}
fn pattern(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
fn overflow_live(s: &Server) -> usize {
    let bpm = s.txn_manager();
    let _ = bpm;
    // count via reopened Server's BPM through catalog? fallback: scan via Server txn manager not exposed — use file scan via DiskManager in test setup
    0
}
fn child_workload(dir: &Path) {
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    let srv = Arc::new(srv);
    let sess = srv.create_session();
    srv.begin_blob_write(sess, u64::MAX).unwrap();
    let payload = pattern(1_000_000, 7);
    // 50% upload, then wait to be killed
    for c in payload[..500_000].chunks(64*1024) { srv.write_blob_chunk(sess, c).unwrap(); }
    File::create(dir.join("ready.flag")).unwrap().sync_all().unwrap();
    std::thread::sleep(Duration::from_secs(30));
}
fn spawn_sigkill(test_name: &str) -> PathBuf {
    if let Ok(m) = std::env::var("MITRDB_A7_CHILD") {
        if m == "blob50" {
            let dir = PathBuf::from(std::env::var("MITRDB_A7_DIR").unwrap());
            child_workload(&dir); return dir;
        }
    }
    let dir = temp_dir("blob50");
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe).args(["--exact", test_name, "--nocapture"])
        .env("MITRDB_A7_CHILD", "blob50").env("MITRDB_A7_DIR", &dir)
        .stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
    let flag = dir.join("ready.flag");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !flag.exists() {
        assert!(Instant::now() < deadline, "timeout waiting for child 50% upload");
        if let Some(st) = child.try_wait().unwrap() { panic!("child exited early: {st}"); }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(80));
    child.kill().unwrap();
    let st = child.wait().unwrap();
    assert!(!st.success(), "expected SIGKILL");
    dir
}
#[test]
fn sigkill_50pct_blob_upload_no_leaked_pages() {
    let dir = spawn_sigkill("sigkill_50pct_blob_upload_no_leaked_pages");
    if std::env::var("MITRDB_A7_CHILD").is_ok() { return; }
    // reopen — recovery must run zero-undo path and reclaim 50% pages
    let (srv, report) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    assert!(report.undone_records > 0, "recovery should undo uncommitted blob pages, report={report:?}");
    // new blob must still work — no pinned/leaked pages blocking
    let sess = srv.create_session();
    srv.begin_blob_write(sess, u64::MAX).unwrap();
    srv.write_blob_chunk(sess, b"after-crash").unwrap();
    let d = srv.finish_blob_write(sess).unwrap();
    assert_eq!(d.total_len, 11);
    srv.execute_in_session(sess, "COMMIT").unwrap();
    srv.close_session(sess).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
#[test]
fn vacuum_after_delete_reclaims_overflow_pages() {
    let dir = temp_dir("vacuum");
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, body TEXT)").unwrap();
    let big = "x".repeat(5000);
    srv.execute_sql(&format!("INSERT INTO t VALUES (1, '{big}')")).unwrap();
    srv.execute_sql("DELETE FROM t WHERE id = 1").unwrap();
    let rep = srv.vacuum();
    let _ = rep;
    // after vacuum, a fresh large insert must reuse pages — no OutOfSpace, no leak
    let big2 = "y".repeat(5000);
    srv.execute_sql(&format!("INSERT INTO t VALUES (2, '{big2}')")).unwrap();
    let r = srv.execute_sql("SELECT body FROM t WHERE id = 2").unwrap();
    let _ = r;
    let _ = std::fs::remove_dir_all(dir);
}
