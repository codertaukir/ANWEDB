use mitrdb_server::Server;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn send(s: &mut TcpStream, d: &[u8]) -> std::io::Result<()> {
    s.write_all(&(d.len() as u32).to_be_bytes())?;
    s.write_all(d)
}
fn recv(s: &mut TcpStream) -> std::io::Result<String> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h)?;
    let mut b = vec![0u8; u32::from_be_bytes(h) as usize];
    s.read_exact(&mut b)?;
    Ok(String::from_utf8_lossy(&b).to_string())
}
const TOKEN: &str = "tcp-kill-drill-token-0123456789-abcdefghij";

fn client(addr: &str) -> std::io::Result<TcpStream> {
    let mut c = TcpStream::connect(addr)?;
    c.set_nodelay(true)?;
    c.set_read_timeout(Some(Duration::from_secs(5)))?;
    send(&mut c, b"MITRDB/1")?;
    recv(&mut c)?;
    send(&mut c, format!("AUTH {TOKEN}").as_bytes())?;
    let r = recv(&mut c)?;
    if is_err(&r) {
        return Err(std::io::Error::other(format!("auth failed: {r}")));
    }
    Ok(c)
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

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (cli, root) = (a[1].clone(), a[2].clone());
    let addr = "127.0.0.1:17432";
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (db, wal) = (format!("{root}/t.db"), format!("{root}/t.wal"));

    let mut child = Command::new(&cli)
        .env("MITRDB_AUTH_TOKEN", TOKEN)
        .args(["--db", &db, "--wal", &wal, "--listen", addr])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("cannot start cli");
    let t0 = Instant::now();
    let mut c0 = loop {
        if let Ok(c) = client(addr) {
            break c;
        }
        if t0.elapsed() > Duration::from_secs(15) {
            panic!("server not ready");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    send(&mut c0, b"CREATE TABLE t (id INT PRIMARY KEY, p TEXT)").unwrap();
    println!(
        "TK create => {}",
        recv(&mut c0).unwrap().chars().take(80).collect::<String>()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let intent = Arc::new(Mutex::new(HashSet::<i64>::new()));
    let acked = Arc::new(Mutex::new(HashSet::<i64>::new()));
    let hs: Vec<_> = (0..4i64)
        .map(|t| {
            let (stop, intent, acked) = (stop.clone(), intent.clone(), acked.clone());
            let addr = addr.to_string();
            std::thread::spawn(move || {
                let mut c = client(&addr).unwrap();
                let mut k = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    let id = t * 10_000_000 + k;
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

    std::thread::sleep(Duration::from_secs(3));
    child.kill().unwrap(); // SIGKILL
    let _ = child.wait();
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        let _ = h.join();
    }
    let (intent, acked) = (
        intent.lock().unwrap().clone(),
        acked.lock().unwrap().clone(),
    );
    println!("TK killed: intent={} acked={}", intent.len(), acked.len());

    let t1 = Instant::now();
    let s = Server::open(&db, &wal, 256).unwrap().0;
    println!("TK recovery_open secs={:.2}", t1.elapsed().as_secs_f64());
    let rows = s
        .table("t")
        .find()
        .columns(&["id", "p"])
        .run()
        .unwrap()
        .rows;
    let mut present = HashSet::new();
    let mut bad = 0;
    for r in &rows {
        let id = pid(&format!("{:?}", r[0]));
        if !format!("{:?}", r[1]).contains(&format!("p-{id}-")) {
            bad += 1;
        }
        present.insert(id);
    }
    let lost = acked.difference(&present).count();
    let phantom = present.difference(&intent).count();
    let dup = rows.len() - present.len();
    println!(
        "TK VERIFY rows={} acked={} lost={lost} phantom={phantom} dup={dup} bad_payload={bad}",
        rows.len(),
        acked.len()
    );
}
