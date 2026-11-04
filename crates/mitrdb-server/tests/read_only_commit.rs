use mitrdb_server::{val, Server};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_roc_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

fn reads(s: &Server, n: i64) {
    let t = s.table("t");
    for k in 0..n {
        assert!(t.find().filter("id", "=", val(k)).one().unwrap().is_some());
    }
}

#[test]
fn read_only_commits_keep_durability_and_txn_ids_sane() {
    let d = dir("a");
    {
        let s = open(&d);
        s.table("t").sync("id int primary key, g int index").unwrap();
        let t = s.table("t");
        for i in 0..100i64 {
            t.insert(&[("id", val(i)), ("g", val(i % 5))]).unwrap();
        }
        reads(&s, 30);
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("SELECT * FROM t").unwrap();
        s.execute_sql("COMMIT").unwrap();
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("COMMIT").unwrap();
    } // crash
    {
        let s = open(&d);
        let t = s.table("t");
        assert_eq!(t.find().count().unwrap(), 100);
        reads(&s, 30);
        for i in 100..150i64 {
            t.insert(&[("id", val(i)), ("g", val(i % 5))]).unwrap();
        }
        // read + write in one explicit txn
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("SELECT * FROM t").unwrap();
        t.insert(&[("id", val(1000)), ("g", val(1))]).unwrap();
        s.execute_sql("COMMIT").unwrap();
        reads(&s, 10);
    } // crash
    let s = open(&d);
    let t = s.table("t");
    assert_eq!(t.find().count().unwrap(), 151);
    assert_eq!(t.find().filter("g", "=", val(0)).count().unwrap(), 30);
    assert!(t.find().filter("id", "=", val(1000)).one().unwrap().is_some());
}
