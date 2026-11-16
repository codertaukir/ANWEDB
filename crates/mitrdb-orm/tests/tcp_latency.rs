use mitrdb_orm::*;
use mitrdb_server::TcpServerConfig;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn send(s: &mut TcpStream, data: &[u8]) {
    s.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
    s.write_all(data).unwrap();
}
fn text(s: &mut TcpStream) -> String {
    let mut h = [0u8; 4];
    s.read_exact(&mut h).unwrap();
    let mut b = vec![0u8; u32::from_be_bytes(h) as usize];
    s.read_exact(&mut b).unwrap();
    String::from_utf8_lossy(&b).to_string()
}
fn tcp_run(addr: std::net::SocketAddr, nodelay: bool, table: &str, n: usize) -> Duration {
    let mut c = TcpStream::connect(addr).unwrap();
    c.set_nodelay(nodelay).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    send(&mut c, b"MITRDB/1");
    text(&mut c);
    let t = Instant::now();
    for i in 0..n {
        send(
            &mut c,
            format!("INSERT INTO {table} VALUES ({i}, {i})").as_bytes(),
        );
        let r = text(&mut c);
        assert!(!r.contains("\"status\":\"error\""), "{r}");
    }
    t.elapsed()
}

#[test]
#[ignore = "probe: run with --ignored --nocapture"]
fn tcp_latency_probe() {
    let dir = std::env::temp_dir().join(format!("mitrdb_lat_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(
        Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
            .unwrap()
            .0,
    );
    for t in ["lat_off", "lat_on", "lat_local"] {
        server
            .execute_sql(&format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let h = server
        .bind_tcp(TcpServerConfig {
            bind_addr: ([127, 0, 0, 1], 0).into(),
            ..TcpServerConfig::default()
        })
        .unwrap();
    let n = 300;
    let off = tcp_run(h.local_addr(), false, "lat_off", n);
    let on = tcp_run(h.local_addr(), true, "lat_on", n);
    let t = Instant::now();
    for i in 0..n {
        server
            .execute_sql(&format!("INSERT INTO lat_local VALUES ({i}, {i})"))
            .unwrap();
    }
    let local = t.elapsed();
    let per = |d: Duration| d.as_micros() as f64 / n as f64;
    eprintln!("LAT nodelay_off us/stmt={:.0}", per(off));
    eprintln!("LAT nodelay_on  us/stmt={:.0}", per(on));
    eprintln!("LAT in_process  us/stmt={:.0}", per(local));
    h.shutdown().unwrap();
}
