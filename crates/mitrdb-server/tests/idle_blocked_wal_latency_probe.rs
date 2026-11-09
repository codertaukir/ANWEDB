// PROBE (ignored): blocked WAL + poisoned commit => new-connection latency through the single accept thread.
// Twin: healthy server, blocked server, healthy server again (same run).
use mitrdb_server::{Server, TcpServerConfig};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

fn send_frame(s: &mut TcpStream, b: &[u8]) -> std::io::Result<()> {
    s.write_all(&(b.len() as u32).to_be_bytes())?;
    s.write_all(b)
}

fn recv_frame(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h)?;
    let n = u32::from_be_bytes(h) as usize;
    let mut b = vec![0u8; n];
    s.read_exact(&mut b)?;
    Ok(b)
}

fn connect_ping(addr: SocketAddr) -> std::io::Result<Duration> {
    let t = Instant::now();
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(10))?;
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    send_frame(&mut s, b"MITRDB/1")?;
    let hello = recv_frame(&mut s)?;
    if !String::from_utf8_lossy(&hello).contains("\"ok\"") {
        return Err(std::io::Error::other(format!("bad hello: {}", String::from_utf8_lossy(&hello))));
    }
    send_frame(&mut s, b"PING")?;
    let pong = recv_frame(&mut s)?;
    if !String::from_utf8_lossy(&pong).contains("PONG") {
        return Err(std::io::Error::other(format!("bad pong: {}", String::from_utf8_lossy(&pong))));
    }
    let d = t.elapsed();
    let _ = send_frame(&mut s, b"QUIT");
    Ok(d)
}

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn run(label: &str, blocked: bool) {
    let dir = std::env::temp_dir().join(format!("mitrdb_idle_probe_{}_{}", label, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open_with_segment_size(dir.join("t.db"), dir.join("t.wal"), 64, 64).unwrap();
    let srv = Arc::new(srv);
    srv.execute_sql("CREATE TABLE s (id INT PRIMARY KEY, v INT)").unwrap();
    srv.execute_sql("INSERT INTO s VALUES (100, 0)").unwrap();

    let mut blocked_path = None;
    if blocked {
        let b = next_segment_path(&dir);
        std::fs::create_dir(&b).unwrap();
        let t = Instant::now();
        let r = srv.execute_sql("INSERT INTO s VALUES (1, 1)");
        println!("SETUP_INSERT {label} err={} after={:?}", r.is_err(), t.elapsed());
        assert!(r.is_err(), "premise: insert must fail");
        assert!(srv.txn_manager().poisoned_commit_count() > 0, "premise: commit must be poisoned");
        blocked_path = Some(b);
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], free_port()));
    let config = TcpServerConfig {
        bind_addr: addr,
        auth_token: None,
        ..TcpServerConfig::default()
    };
    let handle = srv.bind_tcp(config).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    let mut ok: Vec<Duration> = Vec::new();
    let mut errs: Vec<String> = Vec::new();
    for i in 0..15u64 {
        match connect_ping(addr) {
            Ok(d) => ok.push(d),
            Err(e) => errs.push(e.to_string()),
        }
        std::thread::sleep(Duration::from_millis(37 + i * 13));
    }
    ok.sort();
    let med = ok.get(ok.len() / 2).copied();
    println!(
        "PROBE {label} blocked={blocked} n=15 ok={} err={} min={:?} med={:?} max={:?} poisoned_end={}",
        ok.len(),
        errs.len(),
        ok.first(),
        med,
        ok.last(),
        srv.txn_manager().poisoned_commit_count()
    );
    for e in errs.iter().take(3) {
        println!("PROBE_ERR {label} {e}");
    }

    if let Some(b) = blocked_path {
        let _ = std::fs::remove_dir(&b);
    }
    handle.shutdown().unwrap();
    drop(handle);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore]
fn idle_blocked_wal_accept_latency_probe() {
    run("healthy_a", false);
    run("blocked", true);
    run("healthy_b", false);
}
