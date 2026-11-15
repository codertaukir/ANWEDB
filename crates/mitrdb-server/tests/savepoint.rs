use mitrdb_server::{Server, Value};

#[test]
fn savepoints() {
    let dir = std::env::temp_dir().join(format!("mitrdb_sp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    let t = db.table("sp");
    t.sync("id int primary key, v text").unwrap();
    let ins = |i: i64| {
        t.insert(&[("id", Value::Int64(i)), ("v", Value::Text("x".into()))])
            .unwrap()
    };
    let cnt = || t.find().run().unwrap().rows.len();

    db.execute_sql("BEGIN").unwrap();
    ins(1);
    db.execute_sql("SAVEPOINT a").unwrap();
    ins(2);
    db.execute_sql("SAVEPOINT b").unwrap();
    ins(3);
    assert_eq!(cnt(), 3);
    db.execute_sql("ROLLBACK TO b").unwrap();
    assert_eq!(cnt(), 2);
    db.execute_sql("ROLLBACK TO SAVEPOINT a").unwrap();
    assert_eq!(cnt(), 1);
    assert!(db.execute_sql("ROLLBACK TO b").is_err()); // b gone, txn still alive
    ins(4);
    db.execute_sql("RELEASE a").unwrap();
    assert!(db.execute_sql("ROLLBACK TO a").is_err());
    db.execute_sql("COMMIT").unwrap();
    assert_eq!(cnt(), 2);
    assert!(db.execute_sql("SAVEPOINT z").is_err()); // no active txn
}
