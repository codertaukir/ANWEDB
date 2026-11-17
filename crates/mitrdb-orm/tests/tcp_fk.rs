use mitrdb_orm::*;
use mitrdb_server::TcpServerConfig;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

#[derive(Model, Debug)]
#[model(table = "tcp_parent")]
struct TParent {
    #[col(primary_key)]
    id: i64,
}

#[derive(Model, Debug)]
#[model(table = "tcp_child")]
struct TChild {
    #[col(primary_key)]
    id: i64,
    #[col(references = "tcp_parent.id")]
    parent_id: i64,
}

#[derive(Model, Debug)]
#[model(table = "tcp_item")]
struct TItem {
    #[col(primary_key)]
    id: i64,
    owner: i64,
    val: i64,
}

fn start(tag: &str) -> (Arc<Server>, mitrdb_server::TcpServerHandle) {
    let dir = std::env::temp_dir().join(format!("mitrdb_tcpfk_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(
        Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
            .unwrap()
            .0,
    );
    let cfg = TcpServerConfig {
        bind_addr: ([127, 0, 0, 1], 0).into(),
        ..TcpServerConfig::default()
    };
    let h = server.bind_tcp(cfg).unwrap();
    (server, h)
}

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

fn client(addr: SocketAddr) -> TcpStream {
    let mut c = TcpStream::connect(addr).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    send(&mut c, b"MITRDB/1");
    assert!(text(&mut c).contains("MITRDB/1"));
    c
}

fn sql(c: &mut TcpStream, q: &str) -> String {
    send(c, q.as_bytes());
    text(c)
}

#[test]
fn raw_tcp_insert_bypasses_fk() {
    let (db, h) = start("fk");
    db.sync::<TParent>().unwrap();
    db.sync::<TChild>().unwrap();
    db.insert(&TParent { id: 1 }).unwrap();
    // ORM path: FK enforced
    assert!(
        db.insert(&TChild {
            id: 1,
            parent_id: 99
        })
        .is_err()
    );
    assert_eq!(db.all::<TChild>().run().unwrap().len(), 0);
    // TCP raw path: refused (FK table, only Table/ORM API may write)
    let mut c = client(h.local_addr());
    let r = sql(&mut c, "INSERT INTO tcp_child VALUES (2, 99)");
    assert!(
        r.contains("\"status\":\"error\"") && r.contains("refused"),
        "reply: {r}"
    );
    let kids = db.all::<TChild>().run().unwrap();
    assert_eq!(kids.len(), 0, "raw TCP insert must not reach an FK table");
    h.shutdown().unwrap();
}

#[test]
fn multi_client_raw_writes_exact() {
    let (db, h) = start("multi");
    db.sync::<TItem>().unwrap();
    let addr = h.local_addr();
    let (clients, per) = (4i64, 250i64);
    let mut ths = vec![];
    for t in 0..clients {
        ths.push(std::thread::spawn(move || {
            let mut c = client(addr);
            for k in 0..per {
                let id = t * per + k + 1;
                let r = sql(
                    &mut c,
                    &format!("INSERT INTO tcp_item VALUES ({id}, {t}, {id})"),
                );
                assert!(!r.contains("\"status\":\"error\""), "insert {id}: {r}");
            }
            // even ids of this client's range -> val 0
            for k in 0..per {
                let id = t * per + k + 1;
                if id % 2 == 0 {
                    let r = sql(
                        &mut c,
                        &format!("UPDATE tcp_item SET val = 0 WHERE id = {id}"),
                    );
                    assert!(!r.contains("\"status\":\"error\""), "update {id}: {r}");
                }
            }
        }));
    }
    for t in ths {
        t.join().unwrap();
    }
    let rows = db.all::<TItem>().run().unwrap();
    assert_eq!(rows.len() as i64, clients * per);
    let total = clients * per;
    let expect: i64 = (1..=total).filter(|i| i % 2 == 1).sum();
    assert_eq!(rows.iter().map(|r| r.val).sum::<i64>(), expect);
    for r in &rows {
        assert_eq!(r.owner, (r.id - 1) / per, "id {}", r.id);
    }
    h.shutdown().unwrap();
}
