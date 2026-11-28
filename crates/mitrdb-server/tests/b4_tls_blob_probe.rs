#![allow(clippy::ptr_arg)]
use mitrdb_server::{Server, TcpServerConfig};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;
fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("fixtures").join("tls").join(name)
}
fn private_key_copy(tag: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let dst = std::env::temp_dir().join(format!("mitrdb-b4-tls-key-{tag}.pem"));
    std::fs::copy(fixture("server-key.pem"), &dst).unwrap();
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o600)).unwrap();
    dst
}
fn trusted_tls_client(addr: SocketAddr) -> std::io::Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let ca_file = File::open(fixture("ca.pem"))?;
    let mut reader = BufReader::new(ca_file);
    let roots = CertificateDer::pem_reader_iter(&mut reader).collect::<std::result::Result<Vec<_>, _>>().map_err(|e| std::io::Error::other(format!("CA parse: {e}")))?;
    let mut store = rustls::RootCertStore::empty();
    for r in roots { store.add(r).map_err(|e| std::io::Error::other(format!("CA add: {e}")))?; }
    let cfg = rustls::ClientConfig::builder().with_root_certificates(store).with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost".to_owned()).map_err(|e| std::io::Error::other(format!("name: {e}")))?;
    let conn = rustls::ClientConnection::new(Arc::new(cfg), name).map_err(|e| std::io::Error::other(format!("client: {e}")))?;
    let sock = TcpStream::connect(addr)?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    sock.set_write_timeout(Some(Duration::from_secs(5)))?;
    Ok(rustls::StreamOwned::new(conn, sock))
}
fn send_frame<S: Read + Write>(s: &mut S, p: &Vec<u8>) { let mut b = Vec::with_capacity(4 + p.len()); b.extend_from_slice(&(p.len() as u32).to_be_bytes()); b.extend_from_slice(p); s.write_all(&b).unwrap(); }
fn send_text<S: Read + Write>(s: &mut S, t: &str) { send_frame(s, &t.as_bytes().to_vec()); }
fn recv_frame<S: Read + Write>(s: &mut S) -> Option<Vec<u8>> { let mut h = [0u8; 4]; s.read_exact(&mut h).ok()?; let n = u32::from_be_bytes(h) as usize; let mut body = vec![0u8; n]; s.read_exact(&mut body).ok()?; Some(body) }
fn recv_text<S: Read + Write>(s: &mut S) -> Option<String> { Some(String::from_utf8_lossy(&recv_frame(s)?).to_string()) }
fn parse_u64(s: &str, key: &str) -> u64 { let pos = s.find(key).unwrap(); let rest = &s[pos + key.len()..]; let mut num = String::new(); let mut started = false; for c in rest.chars() { if c.is_ascii_digit() { started = true; num.push(c); } else if started { break; } } num.parse().unwrap() }
#[test]
fn b4_blob_over_tls_with_auth() {
    let tag = format!("{}_{}", std::process::id(), "tlsblob");
    let dir = std::env::temp_dir().join(format!("mitrdb_b4_tls_{tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap().0);
    let cfg = TcpServerConfig { bind_addr: ([127, 0, 0, 1], 0).into(), auth_token: Some(Zeroizing::new("b4-tls-token-32-bytes-long-xyz".to_string())), tls_cert_path: Some(fixture("server-cert.pem")), tls_key_path: Some(private_key_copy(&tag)), ..TcpServerConfig::default() };
    let handle = server.bind_tcp(cfg).unwrap();
    let addr = handle.local_addr();
    let mut s = trusted_tls_client(addr).unwrap();
    send_text(&mut s, "MITRDB/1");
    let h = recv_text(&mut s).unwrap(); assert!(h.contains("MITRDB/1"), "hello: {h}");
    send_text(&mut s, "AUTH b4-tls-token-32-bytes-long-xyz");
    let h = recv_text(&mut s).unwrap(); assert!(h.contains("authenticated"), "auth: {h}");
    send_text(&mut s, "BLOB_UPLOAD_START");
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("BLOB_UPLOAD_STARTED"), "START: {r}");
    let mut expect: Vec<u8> = Vec::new(); expect.extend_from_slice(&vec![0xAAu8; 100]); expect.extend_from_slice(&vec![0xBBu8; 50]);
    send_frame(&mut s, &vec![0xAAu8; 100]);
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("CHUNK_OK"), "chunk1: {r}");
    send_frame(&mut s, &vec![0xBBu8; 50]);
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("CHUNK_OK"), "chunk2: {r}");
    send_text(&mut s, "BLOB_UPLOAD_FINISH");
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("blob_ref") && r.contains("150"), "FINISH: {r}");
    let head = parse_u64(&r, "head_page_id");
    let total = parse_u64(&r, "total_len");
    let crc = parse_u64(&r, "payload_crc32c");
    assert_eq!(total, 150, "total: {r}");
    send_text(&mut s, &format!("BLOB_DOWNLOAD_STREAM {head} {total} {crc}"));
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_START"), "DL START: {r}");
    let chunk = recv_frame(&mut s).unwrap(); assert_eq!(chunk, expect, "download bytes must match upload");
    let r = recv_text(&mut s).unwrap(); assert!(r.contains("BLOB_DOWNLOAD_END"), "DL END: {r}");
}
