use mitrdb_server::{Server, val};
use std::sync::Arc;

fn open(tag: &str) -> Arc<Server> {
    let dir = std::env::temp_dir().join(format!("mitrdb_dsc_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap().0)
}
fn ids(s: &Server, t: &str) -> Vec<String> {
    let mut v: Vec<String> = s.table(t).find().columns(&["id"]).run().unwrap().rows
        .iter().map(|r| format!("{:?}", r[0])).collect();
    v.sort();
    v
}

#[test]
fn raw_autocommit_threads_exact() {
    let s = open("raw");
    s.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, o INT)").unwrap();
    let hs: Vec<_> = (0..4i64).map(|t| {
        let s = s.clone();
        std::thread::spawn(move || {
            for k in 0..300i64 {
                let id = t * 1000 + k;
                s.execute_sql(&format!("INSERT INTO c VALUES ({id}, 0)")).unwrap();
                s.execute_sql(&format!("UPDATE c SET o = 1 WHERE id = {id}")).unwrap();
            }
        })
    }).collect();
    for h in hs { h.join().unwrap(); }
    assert_eq!(ids(&s, "c").len(), 1200);
    let r = s.table("c").find().columns(&["o"]).run().unwrap();
    assert!(r.rows.iter().all(|x| format!("{:?}", x[0]).contains('1')));
}

#[test]
fn table_api_threads_exact() {
    let s = open("tbl");
    s.table("c2").sync("id int primary key, o int").unwrap();
    let hs: Vec<_> = (0..4i64).map(|t| {
        let s = s.clone();
        std::thread::spawn(move || {
            let tb = s.table("c2");
            for k in 0..300i64 {
                tb.insert(&[("id", val(t * 1000 + k)), ("o", val(1i64))]).unwrap();
            }
        })
    }).collect();
    for h in hs { h.join().unwrap(); }
    assert_eq!(ids(&s, "c2").len(), 1200);
}

#[test]
fn explicit_txn_semantics_unchanged() {
    let s = open("txn");
    s.execute_sql("CREATE TABLE x (id INT PRIMARY KEY)").unwrap();
    s.execute_sql("BEGIN").unwrap();
    s.execute_sql("INSERT INTO x VALUES (1)").unwrap();
    s.execute_sql("ROLLBACK").unwrap();
    assert_eq!(ids(&s, "x").len(), 0);
    s.execute_sql("BEGIN").unwrap();
    s.execute_sql("INSERT INTO x VALUES (2)").unwrap();
    s.execute_sql("COMMIT").unwrap();
    assert_eq!(ids(&s, "x").len(), 1);
    s.execute_sql("BEGIN").unwrap();
    s.execute_sql("INSERT INTO x VALUES (3)").unwrap();
    s.execute_sql("SAVEPOINT a").unwrap();
    s.execute_sql("INSERT INTO x VALUES (4)").unwrap();
    s.execute_sql("ROLLBACK TO a").unwrap();
    s.execute_sql("COMMIT").unwrap();
    assert_eq!(ids(&s, "x").len(), 2);
    // DDL inside txn still refused, and autocommit works after
    s.execute_sql("BEGIN").unwrap();
    assert!(s.execute_sql("CREATE TABLE y (id INT PRIMARY KEY)").is_err());
    s.execute_sql("ROLLBACK").unwrap();
    s.execute_sql("INSERT INTO x VALUES (9)").unwrap();
    assert_eq!(ids(&s, "x").len(), 3);
}
