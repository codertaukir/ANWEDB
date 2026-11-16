use mitrdb_orm::*;
use std::time::Instant;

#[derive(Model, Debug)]
struct Plain {
    #[col(primary_key)]
    id: i64,
    v: i64,
}
#[derive(Model, Debug)]
struct Fkd {
    #[col(primary_key)]
    id: i64,
    #[col(references = "plains.id")]
    p: i64,
    #[col(check = ">= 0")]
    n: i64,
}

#[test]
fn insert_cost() {
    let dir = std::env::temp_dir().join(format!("mitrdb_cost_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 1024).unwrap();
    db.sync::<Plain>().unwrap();
    db.sync::<Fkd>().unwrap();
    let t = Instant::now();
    for i in 0..3000 {
        db.insert(&Plain { id: i, v: 1 }).unwrap();
    }
    let a = t.elapsed();
    let t = Instant::now();
    for i in 0..3000 {
        db.insert(&Fkd { id: i, p: i, n: 1 }).unwrap();
    }
    let b = t.elapsed();
    println!("plain={:?} with_fk_check_default={:?}", a, b);
}
