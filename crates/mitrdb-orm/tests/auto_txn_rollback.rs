use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Job {
    #[col(primary_key, auto)]
    id: i64,
    n: i64,
}

#[test]
fn auto_id_after_rollback_probe() {
    let dir = std::env::temp_dir().join(format!("mitrdb_atp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<Job>().unwrap();

    let a = db.insert_get_id(&Job { id: 0, n: 1 }).unwrap();
    db.execute_sql("BEGIN").unwrap();
    let b = db.insert_get_id(&Job { id: 0, n: 2 });
    let c = db.insert_get_id(&Job { id: 0, n: 3 });
    println!("PROBE in-txn ids: a={a} b={b:?} c={c:?}");
    db.execute_sql("ROLLBACK").unwrap();
    println!(
        "PROBE count after rollback = {}",
        db.count::<Job>().unwrap()
    );
    let d = db.insert_get_id(&Job { id: 0, n: 4 }).unwrap();
    assert!(
        d > *b.as_ref().unwrap(),
        "id reused after rollback: d={d} b={b:?}"
    );
}
