use std::sync::Arc;
use mitrdb_common::PAGE_SIZE;
use mitrdb_server::Server;
use mitrdb_storage::{OverflowDescriptorV2, PAGE_TYPE_OVERFLOW};

fn setup(tag: &str) -> (std::path::PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_a6_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (s, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    (dir, s)
}
fn pattern(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
#[test]
fn session_blob_write_commit_then_read_range() {
    let (dir, s) = setup("commit");
    let sess = s.create_session();
    let payload = pattern(300_000, 1);
    s.begin_blob_write(sess, u64::MAX).unwrap();
    for c in payload.chunks(64*1024) { s.write_blob_chunk(sess, c).unwrap(); }
    let desc = s.finish_blob_write(sess).unwrap();
    assert_eq!(desc.total_len, 300_000);
    s.execute_in_session(sess, "COMMIT").unwrap();
    s.open_blob_read(sess, desc).unwrap();
    let got = s.read_blob_range(sess, 100_000, 5_000).unwrap();
    assert_eq!(got, &payload[100_000..105_000]);
    s.close_session(sess).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
#[test]
fn session_close_without_finish_frees_pages() {
    let (dir, s) = setup("abort");
    let sess = s.create_session();
    s.begin_blob_write(sess, u64::MAX).unwrap();
    s.write_blob_chunk(sess, &pattern(200_000, 2)).unwrap();
    // no finish, no COMMIT — close must abort + free (A4 undo)
    s.close_session(sess).unwrap();
    // new session can still write — no leaked pinned pages
    let sess2 = s.create_session();
    s.begin_blob_write(sess2, u64::MAX).unwrap();
    s.write_blob_chunk(sess2, b"hi").unwrap();
    let d2 = s.finish_blob_write(sess2).unwrap();
    assert_eq!(d2.total_len, 2);
    s.execute_in_session(sess2, "COMMIT").unwrap();
    s.close_session(sess2).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
#[test]
fn session_blob_read_needs_open_and_range_bounds() {
    let (dir, s) = setup("bounds");
    let sess = s.create_session();
    s.begin_blob_write(sess, u64::MAX).unwrap();
    s.write_blob_chunk(sess, b"hello world").unwrap();
    let desc = s.finish_blob_write(sess).unwrap();
    s.execute_in_session(sess, "COMMIT").unwrap();
    assert!(s.read_blob_range(sess, 0, 5).is_err()); // no reader opened yet
    s.open_blob_read(sess, desc).unwrap();
    assert_eq!(s.read_blob_range(sess, 0, 5).unwrap(), b"hello");
    assert!(s.read_blob_range(sess, 100, 5).is_err());
    assert!(s.read_blob_range(sess, 11, 5).unwrap().is_empty());
    s.close_session(sess).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
