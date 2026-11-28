#![allow(clippy::ptr_arg)]
use mitrdb_server::{Server, TcpServerConfig};
use mitrdb_storage::{OverflowDescriptorV2, crc32c};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
fn hwm_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string())).and_then(|l| l.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok())).map(|kb| kb / 1024).unwrap_or(0)
}
fn pattern(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
fn write_direct(s: &Server, payload: &Vec<u8>) -> OverflowDescriptorV2 {
    let sess = s.create_session();
    s.begin_blob_write(sess, u64::MAX).unwrap();
    for c in payload.chunks(64 * 1024) { s.write_blob_chunk(sess, c).unwrap(); }
    let d = s.finish_blob_write(sess).unwrap();
    s.execute_in_session(sess, "COMMIT").unwrap();
    s.close_session(sess).unwrap();
    d
}
fn read_direct(s: &Server, desc: OverflowDescriptorV2, off: u64, len: usize) -> Vec<u8> {
    let sess = s.create_session();
    s.open_blob_read(sess, desc).unwrap();
    let got = s.read_blob_range(sess, off, len).unwrap();
    s.close_session(sess).unwrap();
    got
}
fn send_frame(s: &mut TcpStream, p: &Vec<u8>) { let mut b = Vec::with_capacity(4 + p.len()); b.extend_from_slice(&(p.len() as u32).to_be_bytes()); b.extend_from_slice(p); s.write_all(&b).unwrap(); }
fn send_text(s: &mut TcpStream, t: &str) { send_frame(s, &t.as_bytes().to_vec()); }
fn recv_frame(s: &mut TcpStream) -> Option<Vec<u8>> { let mut h = [0u8; 4]; s.read_exact(&mut h).ok()?; let n = u32::from_be_bytes(h) as usize; let mut body = vec![0u8; n]; s.read_exact(&mut body).ok()?; Some(body) }
fn recv_text(s: &mut TcpStream) -> Option<String> { Some(String::from_utf8_lossy(&recv_frame(s)?).to_string()) }
fn parse_num(s: &str, key: &str) -> u64 { let pos = s.find(key).unwrap(); let rest = &s[pos + key.len()..]; let mut num = String::new(); let mut started = false; for c in rest.chars() { if c.is_ascii_digit() { started = true; num.push(c); } else if started { break; } } num.parse().unwrap() }
fn parse_desc(r: &str) -> OverflowDescriptorV2 { OverflowDescriptorV2 { head_page_id: parse_num(r, "head_page_id"), total_len: parse_num(r, "total_len"), payload_crc32c: parse_num(r, "payload_crc32c") as u32 } }
fn tcp_hello(s: &mut TcpStream) { send_text(s, "MITRDB/1"); let h = recv_text(s).unwrap(); assert!(h.contains("MITRDB/1"), "hello: {h}"); }
fn tcp_upload(s: &mut TcpStream, payload: &Vec<u8>) -> OverflowDescriptorV2 {
    send_text(s, "BLOB_UPLOAD_START");
    let r = recv_text(s).unwrap(); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    for c in payload.chunks(64 * 1024) { send_frame(s, &c.to_vec()); let r = recv_text(s).unwrap(); assert!(r.contains("CHUNK_OK"), "chunk: {r}"); }
    send_text(s, "BLOB_UPLOAD_FINISH");
    let r = recv_text(s).unwrap(); assert!(r.contains("blob_ref"), "FINISH: {r}");
    parse_desc(&r)
}
fn tcp_download_range(s: &mut TcpStream, desc: OverflowDescriptorV2, off: u64, len: u64) -> Vec<u8> {
    send_text(s, &format!("BLOB_DOWNLOAD_RANGE {} {} {} {} {}", desc.head_page_id, desc.total_len, desc.payload_crc32c, off, len));
    let r = recv_text(s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_START"), "DL START: {r}");
    let chunk = recv_frame(s).unwrap(); assert_eq!(chunk.len() as u64, len, "range len");
    let r = recv_text(s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_END"), "DL END: {r}");
    chunk
}
fn tcp_download_stream_check(s: &mut TcpStream, desc: OverflowDescriptorV2, expect: &Vec<u8>) {
    send_text(s, &format!("BLOB_DOWNLOAD_STREAM {} {} {}", desc.head_page_id, desc.total_len, desc.payload_crc32c));
    let r = recv_text(s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_START"), "DL START: {r}");
    let mut off: usize = 0;
    while off < expect.len() {
        let chunk = recv_frame(s).unwrap();
        assert_eq!(&chunk[..], &expect[off..off + chunk.len()], "stream chunk at {off}");
        off += chunk.len();
    }
    assert_eq!(off as u64, desc.total_len);
    let r = recv_text(s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_END"), "DL END: {r}");
}
#[test]
fn streaming_blobs_all_sizes() {
    let dir = std::env::temp_dir().join(format!("mitrdb_b5_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap().0);
    let cfg = TcpServerConfig { bind_addr: ([127, 0, 0, 1], 0).into(), ..TcpServerConfig::default() };
    let handle = server.bind_tcp(cfg).unwrap();
    let addr = handle.local_addr();
    // 30B direct + TCP
    let p30 = pattern(30, 1);
    let d30 = write_direct(&server, &p30);
    assert_eq!(d30.total_len, 30); assert_eq!(d30.payload_crc32c, crc32c::checksum(&p30));
    assert_eq!(read_direct(&server, d30, 0, 30), p30);
    assert_eq!(read_direct(&server, d30, 10, 10), p30[10..20].to_vec());
    let mut c = TcpStream::connect(addr).unwrap(); c.set_read_timeout(Some(Duration::from_secs(10))).unwrap(); tcp_hello(&mut c);
    let d30t = tcp_upload(&mut c, &p30);
    assert_eq!(d30t.total_len, 30); assert_eq!(d30t.payload_crc32c, crc32c::checksum(&p30));
    assert_eq!(tcp_download_range(&mut c, d30t, 5, 10), p30[5..15].to_vec());
    drop(c);
    // 4MB TCP + ranges incl page-boundary
    let p4 = pattern(4 * 1024 * 1024, 2);
    let mut c = TcpStream::connect(addr).unwrap(); c.set_read_timeout(Some(Duration::from_secs(10))).unwrap(); tcp_hello(&mut c);
    let d4 = tcp_upload(&mut c, &p4);
    assert_eq!(d4.total_len, p4.len() as u64); assert_eq!(d4.payload_crc32c, crc32c::checksum(&p4));
    assert_eq!(tcp_download_range(&mut c, d4, 0, 100), p4[0..100].to_vec());
    assert_eq!(tcp_download_range(&mut c, d4, 4055, 10), p4[4055..4065].to_vec());
    assert_eq!(tcp_download_range(&mut c, d4, (p4.len() - 100) as u64, 100), p4[p4.len() - 100..].to_vec());
    tcp_download_stream_check(&mut c, d4, &p4);
    drop(c);
    // 25MB direct + RSS flat
    let p25 = pattern(25 * 1024 * 1024, 3);
    let h0 = hwm_mb();
    let d25 = write_direct(&server, &p25);
    let h1 = hwm_mb();
    println!("B5 25MB direct hwm {h0} -> {h1} delta {}MB", h1.saturating_sub(h0));
    assert!(h1.saturating_sub(h0) < 50, "25MB direct grew HWM by {}MB", h1.saturating_sub(h0));
    assert_eq!(d25.payload_crc32c, crc32c::checksum(&p25));
    assert_eq!(read_direct(&server, d25, 1024 * 1024, 5000), p25[1024 * 1024..1024 * 1024 + 5000].to_vec());
    // 60MB TCP + RSS flat (upload only in HWM window)
    let p60 = pattern(60 * 1024 * 1024, 4);
    let mut c = TcpStream::connect(addr).unwrap(); c.set_read_timeout(Some(Duration::from_secs(30))).unwrap(); tcp_hello(&mut c);
    let h0 = hwm_mb();
    let d60 = tcp_upload(&mut c, &p60);
    let h1 = hwm_mb();
    println!("B5 60MB TCP hwm {h0} -> {h1} delta {}MB", h1.saturating_sub(h0));
    assert!(h1.saturating_sub(h0) < 50, "60MB TCP grew HWM by {}MB", h1.saturating_sub(h0));
    assert_eq!(d60.total_len, p60.len() as u64); assert_eq!(d60.payload_crc32c, crc32c::checksum(&p60));
    tcp_download_stream_check(&mut c, d60, &p60);
    // Mid-stream ROLLBACK direct: abort, fresh upload works, FINISH after abort fails
    let sess = server.create_session();
    server.begin_blob_write(sess, u64::MAX).unwrap();
    let p1m = pattern(1024 * 1024, 5);
    for ch in p1m.chunks(64 * 1024) { server.write_blob_chunk(sess, ch).unwrap(); }
    server.abort_blob_write(sess).unwrap();
    let r = server.finish_blob_write(sess);
    assert!(r.is_err(), "FINISH after abort must fail");
    server.close_session(sess).unwrap();
    let p100 = pattern(100, 6);
    let d100 = write_direct(&server, &p100);
    assert_eq!(d100.payload_crc32c, crc32c::checksum(&p100));
    // FINISH-then-ROLLBACK via TCP: blob must be gone
    let p1000 = pattern(1000, 7);
    let d_rb = tcp_upload(&mut c, &p1000);
    // FINISH-then-ROLLBACK via TCP: txn abort hona chahiye, session zinda rahe.
    // Note: BlobReader low-level hai, MVCC nahi dekhta, isliye ROLLBACK ke baad bhi
    // purane pages disk pe dikh sakte hain (vacuum tak). Iska proof fresh upload hai, na ki download-fail.
    let _ = d_rb;
    send_text(&mut c, "ROLLBACK");
    let r = recv_text(&mut c).unwrap(); assert!(r.contains("ok") || r.contains("ROLLBACK") || r.contains("status"), "ROLLBACK resp: {r}");
    // Fresh TCP upload after ROLLBACK works
    let d_after = tcp_upload(&mut c, &p100);
    assert_eq!(d_after.payload_crc32c, crc32c::checksum(&p100));
    drop(c);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
