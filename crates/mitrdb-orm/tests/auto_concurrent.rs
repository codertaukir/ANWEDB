use mitrdb_orm::*;
use std::collections::HashSet;

#[derive(Model, Debug)]
struct Job {
    #[col(primary_key, auto)]
    id: i64,
    n: i64,
}

#[test]
fn concurrent_auto_ids_unique() {
    let dir = std::env::temp_dir().join(format!("mitrdb_autoc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<Job>().unwrap();
    let db = &db;
    let ids: Vec<i64> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..4)
            .map(|t| {
                s.spawn(move || {
                    let mut got = Vec::new();
                    for i in 0..500 {
                        got.push(
                            db.insert_get_id(&Job {
                                id: 0,
                                n: t * 1000 + i,
                            })
                            .unwrap(),
                        );
                    }
                    got
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let set: HashSet<_> = ids.iter().collect();
    assert_eq!(set.len(), 2000);
    assert_eq!(db.count::<Job>().unwrap(), 2000);
}
