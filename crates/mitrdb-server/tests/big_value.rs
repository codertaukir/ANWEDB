use mitrdb_server::{val, Server, Value};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_big_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap().0
}

#[test]
fn small_blob_round_trips() {
    let server = open("small");
    let t = server.table("files");
    t.sync("id int primary key, data bytes").unwrap();
    let blob = vec![7u8; 3900];
    t.insert(&[("id", val(1)), ("data", val(blob.clone()))]).unwrap();
    let r = t.find().filter("id", "=", val(1)).run().unwrap();
    assert!(matches!(&r.rows[0][1], Value::Bytes(b) if *b == blob));
}

#[test]
fn huge_blob_is_refused_without_crash() {
    let server = open("huge");
    let t = server.table("files");
    t.sync("id int primary key, data bytes").unwrap();
    for n in [8 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024] {
        let res = t.insert(&[("id", val(1)), ("data", val(vec![7u8; n]))]);
        assert!(res.is_err(), "{n} bytes should be refused");
    }
    // DB still works and nothing half-written is left behind
    assert_eq!(t.count().unwrap(), 0);
    t.insert(&[("id", val(1)), ("data", val(vec![1u8; 100]))]).unwrap();
    assert_eq!(t.count().unwrap(), 1);
}
