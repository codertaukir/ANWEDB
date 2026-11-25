use mitrdb_server::{Server, val};

#[test]
fn insert_auto_inside_explicit_txn() {
    let d = std::env::temp_dir().join(format!("mitrdb_iatxn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0;
    s.table("t").sync("id int primary key, v int").unwrap();
    let t = s.table("t");
    s.execute_sql("BEGIN").unwrap();
    let (id1, _) = t.insert_auto("id", &[("v", val(1))]).unwrap();
    s.execute_sql("ROLLBACK").unwrap();
    assert_eq!(t.find().count().unwrap(), 0);
    s.execute_sql("BEGIN").unwrap();
    let (id2, _) = t.insert_auto("id", &[("v", val(2))]).unwrap();
    let (id3, _) = t.insert_auto("id", &[("v", val(3))]).unwrap();
    s.execute_sql("COMMIT").unwrap();
    assert_eq!(t.find().count().unwrap(), 2);
    assert!(id1 != id2 && id2 != id3 && id1 != id3, "{id1} {id2} {id3}");
}
