use mitrdb_orm::*;
use std::sync::Barrier;

#[derive(Model, Debug)]
struct Pr {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[derive(Model, Debug)]
struct Ch {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "prs.id")]
    pid: i64,
}

#[test]
fn fk_race_many_rounds() {
    let dir = std::env::temp_dir().join(format!("mitrdb_fkr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<Pr>().unwrap();
    db.sync::<Ch>().unwrap();
    let db = &db;
    for r in 0..1500 {
        let id = db
            .insert_get_id(&Pr {
                id: 0,
                name: format!("p{r}"),
            })
            .unwrap();
        let b = Barrier::new(2);
        std::thread::scope(|s| {
            s.spawn(|| {
                b.wait();
                let _ = db.delete::<Pr>().filter(Pr::id.eq(id)).run();
            });
            s.spawn(|| {
                b.wait();
                let _ = db.insert(&Ch { id: 0, pid: id });
            });
        });
    }
    let mut orphans = 0;
    for c in db.all::<Ch>().run().unwrap() {
        if db.find_by_id::<Pr, i64>(c.pid).unwrap().is_none() {
            orphans += 1;
        }
    }
    println!("rounds=1500 orphans={orphans}");
    assert_eq!(orphans, 0);
}
