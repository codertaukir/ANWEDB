#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, TcpServerConfig, TcpServerHandle};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

const TOKEN: &str = "correct-token-with-at-least-32-bytes";

fn start(tag: &str, auth: bool) -> (Arc<Server>, TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_na_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(
        Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
            .unwrap()
            .0,
    );
    let config = TcpServerConfig {
        bind_addr: ([127, 0, 0, 1], 0).into(),
        auth_token: auth.then(|| Zeroizing::new(TOKEN.to_string())),
        ..TcpServerConfig::default()
    };
    let handle = server.bind_tcp(config).unwrap();
    (server, handle)
}

fn connect(h: &TcpServerHandle) -> TcpStream {
    let s = TcpStream::connect(h.local_addr()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s
}

fn send(s: &mut TcpStream, data: &[u8]) {
    s.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
    s.write_all(data).unwrap();
}

fn recv(s: &mut TcpStream) -> Option<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h).ok()?;
    let mut b = vec![0u8; u32::from_be_bytes(h) as usize];
    s.read_exact(&mut b).ok()?;
    Some(b)
}

fn text(s: &mut TcpStream) -> String {
    String::from_utf8_lossy(&recv(s).unwrap_or_default()).to_string()
}

fn assert_alive(h: &TcpServerHandle, auth: bool) {
    let mut s = connect(h);
    send(&mut s, b"MITRDB/1");
    assert!(text(&mut s).contains("MITRDB/1"));
    if auth {
        send(&mut s, format!("AUTH {TOKEN}").as_bytes());
        assert!(text(&mut s).contains("authenticated"));
    }
    send(&mut s, b"PING");
    assert!(text(&mut s).contains("PONG"));
}

#[test]
fn first_frame_not_protocol_is_rejected() {
    let (_s, h) = start("proto", false);
    let mut c = connect(&h);
    send(&mut c, b"SELECT 1");
    assert!(text(&mut c).contains("PROTOCOL_MISMATCH"));
    assert!(recv(&mut c).is_none());
    assert_alive(&h, false);
    h.shutdown().unwrap();
}

#[test]
fn ping_without_token_is_rejected_when_auth_on() {
    let (_s, h) = start("notoken", true);
    let mut c = connect(&h);
    send(&mut c, b"MITRDB/1");
    assert!(text(&mut c).contains("MITRDB/1"));
    send(&mut c, b"PING");
    let reply = text(&mut c);
    assert!(!reply.contains("PONG"));
    assert!(reply.contains("AUTH_FAILED"));
    assert_alive(&h, true);
    h.shutdown().unwrap();
}

#[test]
fn wrong_token_is_rejected_and_server_stays_alive() {
    let (_s, h) = start("wrong", true);
    let mut c = connect(&h);
    send(&mut c, b"MITRDB/1");
    let _ = text(&mut c);
    send(&mut c, b"AUTH wrong-token-with-at-least-32-bytes");
    assert!(text(&mut c).contains("AUTH_FAILED"));
    assert!(recv(&mut c).is_none());
    assert_alive(&h, true);
    h.shutdown().unwrap();
}

fn hello(h: &TcpServerHandle) -> TcpStream {
    let mut c = connect(h);
    send(&mut c, b"MITRDB/1");
    assert!(text(&mut c).contains("MITRDB/1"));
    c
}

#[test]
fn oversize_frame_is_closed_and_server_stays_alive() {
    let (_s, h) = start("big", false);
    for len in [1024u32 * 1024 + 1, u32::MAX] {
        let mut c = hello(&h);
        c.write_all(&len.to_be_bytes()).unwrap();
        assert!(recv(&mut c).is_none());
    }
    assert_alive(&h, false);
    h.shutdown().unwrap();
}

#[test]
fn half_frame_then_silence_is_disconnected() {
    let dir = std::env::temp_dir().join(format!("mitrdb_na_{}_half", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(
        Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
            .unwrap()
            .0,
    );
    let config = TcpServerConfig {
        bind_addr: ([127, 0, 0, 1], 0).into(),
        frame_timeout: Duration::from_secs(1),
        ..TcpServerConfig::default()
    };
    let h = server.bind_tcp(config).unwrap();
    let mut c = hello(&h);
    c.write_all(&100u32.to_be_bytes()).unwrap();
    c.write_all(b"SELECT 1 ").unwrap();
    let started = std::time::Instant::now();
    assert!(recv(&mut c).is_none());
    assert!(started.elapsed() < Duration::from_millis(2500));
    assert_alive(&h, false);
    h.shutdown().unwrap();
}

#[test]
fn ten_bad_tokens_trigger_rate_limit() {
    let (_s, h) = start("rate", true);
    for i in 0..10 {
        let mut c = hello(&h);
        if i < 5 {
            send(&mut c, b"AUTH wrong-token-with-at-least-32-bytes");
            assert!(text(&mut c).contains("AUTH_FAILED"));
        } else {
            assert!(text(&mut c).contains("AUTH_RATE_LIMITED"));
        }
    }
    let mut c = hello(&h);
    let reply = text(&mut c);
    assert!(reply.contains("AUTH_RATE_LIMITED"));
    assert!(!reply.contains("authenticated"));
    h.shutdown().unwrap();
}

#[test]
fn sql_errors_do_not_leak_internal_paths() {
    let (_s, h) = start("leak", false);
    let mut c = hello(&h);
    let big = format!("INSERT INTO lk VALUES (2, '{}')", "x".repeat(5000));
    let queries: Vec<String> = vec![
        "CREATE TABLE lk (id INT PRIMARY KEY, name TEXT)".into(),
        "CREATE TABLE lk (id INT PRIMARY KEY, name TEXT)".into(),
        "SELECT * FROM missing_table".into(),
        "THIS IS NOT SQL ;;; ((".into(),
        "INSERT INTO lk VALUES (1, 'a')".into(),
        "INSERT INTO lk VALUES (1, 'b')".into(),
        big,
    ];
    let mut errors = 0;
    for q in &queries {
        send(&mut c, q.as_bytes());
        let reply = text(&mut c);
        if reply.contains("\"status\":\"error\"") {
            errors += 1;
        }
        for bad in ["/tmp", "/home", "mitrdb_na_", ".db", ".wal"] {
            assert!(!reply.contains(bad), "leak {bad:?} in reply: {reply}");
        }
    }
    assert!(errors >= 4, "expected several SQL errors, got {errors}");
    h.shutdown().unwrap();
}
