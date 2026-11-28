use mitrdb_server::{Server, TcpServerConfig, TcpServerHandle};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
fn start_tcp(tag: &str, max_uploads: usize) -> (std::path::PathBuf, Arc<Server>, TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_b4_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap().0);
    server.set_max_blob_bytes(100);
    let cfg = TcpServerConfig { bind_addr: ([127, 0, 0, 1], 0).into(), max_concurrent_uploads: max_uploads, ..TcpServerConfig::default() };
    let handle = server.bind_tcp(cfg).unwrap();
    (dir, server, handle)
}
fn send_frame(s: &mut TcpStream, p: &[u8]) { let mut b = Vec::with_capacity(4 + p.len()); b.extend_from_slice(&(p.len() as u32).to_be_bytes()); b.extend_from_slice(p); s.write_all(&b).unwrap(); }
fn send_text(s: &mut TcpStream, t: &str) { send_frame(s, t.as_bytes()); }
fn recv_frame(s: &mut TcpStream) -> Option<Vec<u8>> { let mut h = [0u8; 4]; s.read_exact(&mut h).ok()?; let n = u32::from_be_bytes(h) as usize; let mut body = vec![0u8; n]; s.read_exact(&mut body).ok()?; Some(body) }
fn recv_text(s: &mut TcpStream) -> Option<String> { Some(String::from_utf8_lossy(&recv_frame(s)?).to_string()) }
fn hello(s: &mut TcpStream) { send_text(s, "MITRDB/1"); let h = recv_text(s).unwrap(); assert!(h.contains("MITRDB/1"), "hello: {h}"); }
fn upload_start(s: &mut TcpStream) -> String { send_text(s, "BLOB_UPLOAD_START"); recv_text(s).unwrap() }
fn upload_chunk(s: &mut TcpStream, data: &[u8]) -> String { send_frame(s, data); recv_text(s).unwrap() }
fn upload_finish(s: &mut TcpStream) -> String { send_text(s, "BLOB_UPLOAD_FINISH"); recv_text(s).unwrap() }
#[test]
fn b4_max_blob_size_rejects_and_rolls_back() {
    let (_dir, _srv, handle) = start_tcp("max", 8);
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hello(&mut s);
    let r = upload_start(&mut s); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let r = upload_chunk(&mut s, &vec![0xAAu8; 60]); assert!(r.contains("CHUNK_OK"), "chunk1: {r}");
    let r = upload_chunk(&mut s, &vec![0xBBu8; 60]); assert!(r.contains("SQL_ERROR") || r.contains("OutOfSpace") || r.contains("exceeds") || r.contains("error"), "chunk2 over max should error: {r}");
    let r = upload_finish(&mut s); assert!(!r.contains("blob_ref"), "FINISH after max-exceed must NOT return truncated blob_ref: {r}");
    // Session alive, fresh upload under limit works
    let r = upload_start(&mut s); assert!(r.contains("BLOB_UPLOAD_STARTED"), "fresh START after rollback: {r}");
    let r = upload_chunk(&mut s, &vec![0xCCu8; 50]); assert!(r.contains("CHUNK_OK"), "fresh chunk: {r}");
    let r = upload_finish(&mut s); assert!(r.contains("blob_ref") && r.contains("50"), "fresh FINISH total 50: {r}");
}
#[test]
fn b4_per_connection_one_upload() {
    let (_dir, _srv, handle) = start_tcp("perconn", 8);
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hello(&mut s);
    let r = upload_start(&mut s); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let r = upload_chunk(&mut s, &vec![0x01u8; 10]); assert!(r.contains("CHUNK_OK"), "chunk1: {r}");
    // 2nd START bytes while uploading = chunk data, NOT a new writer
    let r = upload_chunk(&mut s, b"BLOB_UPLOAD_START"); assert!(r.contains("CHUNK_OK"), "2nd START as chunk: {r}");
    let r = upload_finish(&mut s); assert!(r.contains("blob_ref") && r.contains("27"), "total 10+17=27, no 2nd writer: {r}");
}
#[test]
fn b4_server_concurrent_upload_limit() {
    let (_dir, _srv, handle) = start_tcp("serverlimit", 1);
    let addr = handle.local_addr();
    let mut s1 = TcpStream::connect(addr).unwrap(); s1.set_read_timeout(Some(Duration::from_secs(5))).unwrap(); hello(&mut s1);
    let mut s2 = TcpStream::connect(addr).unwrap(); s2.set_read_timeout(Some(Duration::from_secs(5))).unwrap(); hello(&mut s2);
    let r1 = upload_start(&mut s1); assert!(r1.contains("BLOB_UPLOAD_STARTED"), "s1 START: {r1}");
    let r2 = upload_start(&mut s2); assert!(r2.contains("TOO_MANY_UPLOADS") || r2.contains("error"), "s2 START over server limit should error: {r2}");
    let r1 = upload_chunk(&mut s1, &vec![0x01u8; 10]); assert!(r1.contains("CHUNK_OK"), "s1 chunk: {r1}");
    let r1 = upload_finish(&mut s1); assert!(r1.contains("blob_ref"), "s1 FINISH: {r1}");
    let r2 = upload_start(&mut s2); assert!(r2.contains("BLOB_UPLOAD_STARTED"), "s2 START after s1 FINISH: {r2}");
    let r2 = upload_chunk(&mut s2, &vec![0x02u8; 5]); assert!(r2.contains("CHUNK_OK"), "s2 chunk: {r2}");
    let r2 = upload_finish(&mut s2); assert!(r2.contains("blob_ref") && r2.contains("5"), "s2 FINISH: {r2}");
}
