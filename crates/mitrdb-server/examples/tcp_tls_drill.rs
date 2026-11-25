use mitrdb_server::Server;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Conn = StreamOwned<ClientConnection, TcpStream>;
const TOKEN: &str = "tcp-tls-drill-token-0123456789-abcdefghij";
const ADDR: &str = "127.0.0.1:17434";

fn send<S: Write>(s: &mut S, d: &[u8]) -> std::io::Result<()> {
    s.write_all(&(d.len() as u32).to_be_bytes())?;
    s.write_all(d)?;
    s.flush()
}
fn recv<S: Read>(s: &mut S) -> std::io::Result<String> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h)?;
    let mut b = vec![0u8; u32::from_be_bytes(h) as usize];
    s.read_exact(&mut b)?;
    Ok(String::from_utf8_lossy(&b).to_string())
}
fn is_err(r: &str) -> bool {
    r.replace(' ', "").contains("\"status\":\"error\"")
}
fn pid(s: &str) -> i64 {
    s[s.find('(').unwrap() + 1..s.rfind(')').unwrap()]
        .trim()
        .parse()
        .unwrap()
}
fn client(cfg: &Arc<ClientConfig>, token: &str) -> std::io::Result<Conn> {
    let tcp = TcpStream::connect(ADDR)?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let conn = ClientConnection::new(cfg.clone(), name).map_err(std::io::Error::other)?;
    let mut c = StreamOwned::new(conn, tcp);
    send(&mut c, b"MITRDB/1")?;
    recv(&mut c)?;
    send(&mut c, format!("AUTH {token}").as_bytes())?;
    let r = recv(&mut c)?;
    if is_err(&r) {
        return Err(std::io::Error::other(format!("auth failed: {r}")));
    }
    Ok(c)
}
fn start(cli: &str, root: &str, certs: &str, maxc: usize) -> Child {
    Command::new(cli)
        .env("MITRDB_AUTH_TOKEN", TOKEN)
        .args([
            "--db",
            &format!("{root}/t.db"),
            "--wal",
            &format!("{root}/t.wal"),
            "--listen",
            ADDR,
            "--tls-cert",
            &format!("{certs}/cert.pem"),
            "--tls-key",
            &format!("{certs}/key.pem"),
            "--max-connections",
            &maxc.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("cannot start cli")
}
fn wait_ready(cfg: &Arc<ClientConfig>) -> Conn {
    let t0 = Instant::now();
    loop {
        let last = match client(cfg, TOKEN) {
            Ok(c) => return c,
            Err(e) => e,
        };
        if t0.elapsed() > Duration::from_secs(20) {
            panic!("server not ready, last error: {last:?}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (cli, root, certs) = (a[1].clone(), a[2].clone(), a[3].clone());
    let n: usize = a[4].parse().unwrap();
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(
            std::fs::read(format!("{certs}/cert.der")).unwrap(),
        ))
        .unwrap();
    let cfg = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let mut child = start(&cli, &root, &certs, 256);
    let mut c0 = wait_ready(&cfg);
    send(&mut c0, b"CREATE TABLE t (id INT PRIMARY KEY, p TEXT)").unwrap();
    println!(
        "TLS create => {}",
        recv(&mut c0).unwrap().chars().take(60).collect::<String>()
    );
    let bad = client(&cfg, "wrong-token-0123456789-abcdefghijklmnop");
    println!("TLS wrong_token_refused={}", bad.is_err());

    let stop = Arc::new(AtomicBool::new(false));
    let intent = Arc::new(Mutex::new(HashSet::<i64>::new()));
    let acked = Arc::new(Mutex::new(HashSet::<i64>::new()));
    let conn_fail = Arc::new(AtomicUsize::new(0));
    let hs: Vec<_> = (0..n as i64)
        .map(|t| {
            let (stop, intent, acked, cf, cfg) = (
                stop.clone(),
                intent.clone(),
                acked.clone(),
                conn_fail.clone(),
                cfg.clone(),
            );
            std::thread::spawn(move || {
                let mut c = match client(&cfg, TOKEN) {
                    Ok(c) => c,
                    Err(_) => {
                        cf.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };
                let mut k = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    let id = t * 1_000_000 + k;
                    intent.lock().unwrap().insert(id);
                    let q = format!("INSERT INTO t VALUES ({id}, 'p-{id}-xxxxxxxxxxxxxxxx')");
                    if send(&mut c, q.as_bytes()).is_err() {
                        break;
                    }
                    match recv(&mut c) {
                        Ok(r) if !is_err(&r) => {
                            acked.lock().unwrap().insert(id);
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                    k += 1;
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(5));
    child.kill().unwrap();
    let _ = child.wait();
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        let _ = h.join();
    }
    let (intent, acked) = (
        intent.lock().unwrap().clone(),
        acked.lock().unwrap().clone(),
    );
    println!(
        "TLS clients={n} conn_fail={} killed: intent={} acked={}",
        conn_fail.load(Ordering::Relaxed),
        intent.len(),
        acked.len()
    );

    let s = Server::open(format!("{root}/t.db"), format!("{root}/t.wal"), 1024)
        .unwrap()
        .0;
    let rows = s
        .table("t")
        .find()
        .columns(&["id", "p"])
        .run()
        .unwrap()
        .rows;
    let mut present = HashSet::new();
    let mut badp = 0;
    for r in &rows {
        let id = pid(&format!("{:?}", r[0]));
        if !format!("{:?}", r[1]).contains(&format!("p-{id}-")) {
            badp += 1;
        }
        present.insert(id);
    }
    let lost = acked.difference(&present).count();
    let phantom = present.iter().filter(|i| !intent.contains(i)).count();
    let dup = rows.len() - present.len();
    let mut fail = 0;
    for i in 0..2000i64 {
        let id = 1_000_000_000_000 + i;
        if s.execute_sql(&format!("INSERT INTO t VALUES ({id}, 'p-{id}-after')"))
            .is_err()
        {
            fail += 1;
        }
    }
    println!(
        "TLS verify rows={} lost={lost} phantom={phantom} dup={dup} bad_payload={badp} after2_fail={fail}",
        rows.len()
    );
    drop(s);

    // connection limit: max 20, open 30 and hold
    let mut child = start(&cli, &root, &certs, 20);
    let _c = wait_ready(&cfg);
    let mut held = vec![];
    let (mut ok, mut refused) = (0, 0);
    for _ in 0..30 {
        match client(&cfg, TOKEN) {
            Ok(c) => {
                ok += 1;
                held.push(c);
            }
            Err(_) => refused += 1,
        }
    }
    let alive = child.try_wait().unwrap().is_none();
    drop(held);
    std::thread::sleep(Duration::from_secs(1));
    let after = client(&cfg, TOKEN).is_ok();
    println!(
        "TLS limit max=20 tried=30 ok={ok} refused={refused} server_alive={alive} new_conn_after_release={after}"
    );
    let _ = child.kill();
    let _ = child.wait();
    if lost + phantom + dup + badp + fail > 0 || !alive || !after {
        std::process::exit(1);
    }
}
