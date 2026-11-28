use mitrdb_server::{Server, TcpServerConfig, TcpServerHandle};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
fn start_tcp(tag: &str, idle: Duration, total: Duration) -> (std::path::PathBuf, Arc<Server>, TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_b3_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap().0);
    let cfg = TcpServerConfig { bind_addr: ([127, 0, 0, 1], 0).into(), blob_idle_timeout: idle, blob_total_timeout: total, ..TcpServerConfig::default() };
    let handle = server.bind_tcp(cfg).unwrap();
    (dir, server, handle)
}
fn send_frame(s: &mut TcpStream, p: &Vec<u8>) { let mut b = Vec::with_capacity(4 + p.len()); b.extend_from_slice(&(p.len() as u32).to_be_bytes()); b.extend_from_slice(p); s.write_all(&b).unwrap(); }
fn send_text(s: &mut TcpStream, t: &str) { send_frame(s, &t.as_bytes().to_vec()); }
fn recv_frame(s: &mut TcpStream) -> Option<Vec<u8>> { let mut h = [0u8; 4]; s.read_exact(&mut h).ok()?; let n = u32::from_be_bytes(h) as usize; let mut body = vec![0u8; n]; s.read_exact(&mut body).ok()?; Some(body) }
fn recv_text(s: &mut TcpStream) -> Option<String> { Some(String::from_utf8_lossy(&recv_frame(s)?).to_string()) }
fn hello(s: &mut TcpStream) { send_text(s, "MITRDB/1"); let h = recv_text(s).unwrap(); assert!(h.contains("MITRDB/1"), "hello: {h}"); }
#[test]
fn b3_idle_timeout_closes_and_frees() {
    let (_dir, _srv, handle) = start_tcp("idle", Duration::from_millis(800), Duration::from_secs(10));
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hello(&mut s);
    send_text(&mut s, "BLOB_UPLOAD_START");
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let chunk = vec![0xAAu8; 1000];
    send_frame(&mut s, &chunk);
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("CHUNK_OK"), "chunk: {r}");
    std::thread::sleep(Duration::from_millis(2000));
    let mut s2 = TcpStream::connect(addr).unwrap();
    s2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hello(&mut s2);
    send_text(&mut s2, "BLOB_UPLOAD_START");
    let r2 = recv_text(&mut s2).unwrap(); assert!(r2.contains("BLOB_UPLOAD_STARTED"), "fresh upload after timeout should work: {r2}");
    let chunk2 = vec![0xBBu8; 10];
    send_frame(&mut s2, &chunk2);
    let r2 = recv_text(&mut s2).unwrap(); assert!(r2.contains("CHUNK_OK"), "fresh chunk: {r2}");
    send_text(&mut s2, "BLOB_UPLOAD_FINISH");
    let r2 = recv_text(&mut s2).unwrap(); assert!(r2.contains("blob_ref"), "fresh finish: {r2}");
    assert!(r2.contains("10"), "fresh total 10: {r2}");
    let after = recv_frame(&mut s);
    assert!(after.is_none(), "idle-timed-out conn should be closed by server, got frame");
}
#[test]
fn b3_total_deadline_closes() {
    let (_dir, _srv, handle) = start_tcp("total", Duration::from_secs(30), Duration::from_millis(1200));
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hello(&mut s);
    send_text(&mut s, "BLOB_UPLOAD_START");
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    for _ in 0..3 {
        let chunk = vec![0x01u8; 100];
        send_frame(&mut s, &chunk);
        let r = recv_text(&mut s).unwrap(); assert!(r.contains("CHUNK_OK"), "chunk: {r}");
        std::thread::sleep(Duration::from_millis(500));
    }
    std::thread::sleep(Duration::from_millis(800));
    let after = recv_frame(&mut s);
    assert!(after.is_none(), "total-deadline conn should be closed, got frame");
}
