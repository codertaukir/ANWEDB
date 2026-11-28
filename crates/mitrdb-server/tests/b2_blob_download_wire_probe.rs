use mitrdb_server::{Server, TcpServerConfig, TcpServerHandle};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
fn start_tcp(tag: &str) -> (std::path::PathBuf, Arc<Server>, TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_b2_{}_{}", std::process::id(), tag));
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
fn parse_num(s: &str, key: &str) -> u64 { let pat = format!("\"{}\":", key); let i = s.find(&pat).expect(&format!("key {} in {}", key, s)); let rest = &s[i + pat.len()..]; let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect(); num.parse().unwrap() }
fn upload(s: &mut TcpStream, data: &Vec<u8>) -> (u64, u64, u64) {
    send_text(s, "BLOB_UPLOAD_START");
    let r = recv_text(s); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    for ch in data.chunks(65536) { send_frame(s, &ch.to_vec()); let r = recv_text(s); assert!(r.contains("CHUNK_OK"), "chunk: {r}"); }
    send_text(s, "BLOB_UPLOAD_FINISH");
    let r = recv_text(s); assert!(r.contains("blob_ref"), "FINISH: {r}");
    (parse_num(&r, "head_page_id"), parse_num(&r, "total_len"), parse_num(&r, "payload_crc32c"))
}
fn download_range(s: &mut TcpStream, head: u64, total: u64, crc: u64, off: u64, len: u64) -> Vec<u8> {
    send_text(s, &format!("BLOB_DOWNLOAD_RANGE {} {} {} {} {}", head, total, crc, off, len));
    let hdr = recv_text(s); assert!(hdr.contains("BLOB_DOWNLOAD_START"), "DL header: {hdr}");
    let chunks = parse_num(&hdr, "chunks");
    let mut out = Vec::new();
    for _ in 0..chunks { let f = recv_frame(s); out.extend_from_slice(&f); }
    let foot = recv_text(s); assert!(foot.contains("BLOB_DOWNLOAD_END"), "DL footer: {foot}");
    assert_eq!(out.len() as u64, len, "DL len mismatch");
    out
}
#[test]
fn b2_download_range_and_stream() {
    let (_dir, _srv, handle) = start_tcp("dl");
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    hello(&mut s);
    let data: Vec<u8> = (0..70000).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    let (head, total, crc) = upload(&mut s, &data);
    assert_eq!(total, 70000, "upload total");
    let got = download_range(&mut s, head, total, crc, 0, 100);
    assert_eq!(got, data[0..100].to_vec(), "range 0-100 mismatch");
    let got = download_range(&mut s, head, total, crc, 65000, 5000);
    assert_eq!(got, data[65000..70000].to_vec(), "range 65000-70000 mismatch");
    send_text(&mut s, &format!("BLOB_DOWNLOAD_STREAM {} {} {}", head, total, crc));
    let hdr = recv_text(&mut s); assert!(hdr.contains("BLOB_DOWNLOAD_START"), "STREAM header: {hdr}");
    let chunks = parse_num(&hdr, "chunks");
    assert_eq!(chunks, 2, "STREAM chunks for 70000 should be 2, hdr: {hdr}");
    let mut out = Vec::new();
    for _ in 0..chunks { out.extend_from_slice(&recv_frame(&mut s)); }
    let foot = recv_text(&mut s); assert!(foot.contains("BLOB_DOWNLOAD_END"), "STREAM footer: {foot}");
    assert_eq!(out, data, "STREAM bytes mismatch");
}
#[test]
fn b2_download_out_of_range_rejected() {
    let (_dir, _srv, handle) = start_tcp("oor");
    let addr = handle.local_addr();
    let mut s = TcpStream::connect(addr).unwrap();
    hello(&mut s);
    let data = vec![0x42u8; 1000];
    let (head, total, crc) = upload(&mut s, &data);
    send_text(&mut s, &format!("BLOB_DOWNLOAD_RANGE {} {} {} {} {}", head, total, crc, 900, 200));
    let r = recv_text(&mut s);
    assert!(r.contains("error") || r.contains("OUT_OF_RANGE") || r.contains("BAD"), "OOR should error, got: {r}");
    assert!(!r.contains("BLOB_DOWNLOAD_START"), "OOR must not start download: {r}");
}
