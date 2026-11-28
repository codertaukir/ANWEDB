use mitrdb_server::{Server, TcpServerConfig, TcpServerHandle};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
fn start_tcp(tag: &str) -> (std::path::PathBuf, Arc<Server>, TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_b1_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap().0);
    let cfg = TcpServerConfig { bind_addr: ([127, 0, 0, 1], 0).into(), ..TcpServerConfig::default() };
    let handle = server.bind_tcp(cfg).unwrap();
    (dir, server, handle)
}
fn send_frame(s: &mut TcpStream, p: &Vec<u8>) { let mut b = Vec::with_capacity(4 + p.len()); b.extend_from_slice(&(p.len() as u32).to_be_bytes()); b.extend_from_slice(p); s.write_all(&b).unwrap(); }
fn send_text(s: &mut TcpStream, t: &str) { send_frame(s, &t.as_bytes().to_vec()); }
fn recv_frame(s: &mut TcpStream) -> Vec<u8> { let mut h = [0u8; 4]; s.read_exact(&mut h).unwrap(); let n = u32::from_be_bytes(h) as usize; let mut body = vec![0u8; n]; s.read_exact(&mut body).unwrap(); body }
fn recv_text(s: &mut TcpStream) -> String { String::from_utf8_lossy(&recv_frame(s)).to_string() }
fn hello(s: &mut TcpStream) { send_text(s, "MITRDB/1"); let h = recv_text(s); assert!(h.contains("MITRDB/1"), "hello: {h}"); }
#[test]
fn b1_upload_start_binary_chunk_finish() {
    let (_dir, _srv, handle) = start_tcp("ok");
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    hello(&mut s);
    send_text(&mut s, "BLOB_UPLOAD_START");
    let r = recv_text(&mut s);
    assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let chunk1: Vec<u8> = (0..100).map(|i| if i % 2 == 0 { 0xFF } else { 0xFE }).collect();
    send_frame(&mut s, &chunk1);
    let r = recv_text(&mut s);
    assert!(r.contains("CHUNK_OK"), "chunk1: {r}");
    assert!(!r.contains("INVALID_UTF8"), "binary chunk rejected: {r}");
    let chunk2 = vec![0x89u8; 65536];
    send_frame(&mut s, &chunk2);
    let r = recv_text(&mut s);
    assert!(r.contains("CHUNK_OK"), "chunk2 64K: {r}");
    send_text(&mut s, "BLOB_UPLOAD_FINISH");
    let r = recv_text(&mut s);
    assert!(r.contains("BlobRef") || r.contains("blob_ref") || r.contains("head_page"), "FINISH: {r}");
    assert!(r.contains("65636"), "FINISH total_len: {r}");
}
#[test]
fn b1_chunk_over_64k_rejected() {
    let (_dir, _srv, handle) = start_tcp("over");
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    hello(&mut s);
    send_text(&mut s, "BLOB_UPLOAD_START");
    let r = recv_text(&mut s);
    assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let big = vec![0xAAu8; 65537];
    send_frame(&mut s, &big);
    let r = recv_text(&mut s);
    assert!(r.contains("CHUNK_TOO_LARGE") || r.contains("error"), "over-64K: {r}");
    let small = vec![0x01u8; 10];
    send_frame(&mut s, &small);
    let r = recv_text(&mut s);
    assert!(r.contains("CHUNK_OK"), "small after reject: {r}");
    send_text(&mut s, "BLOB_UPLOAD_FINISH");
    let r = recv_text(&mut s);
    assert!(r.contains("10"), "FINISH total_len should be 10 only: {r}");
}
