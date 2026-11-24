use mitrdb_orm::*;
use std::time::Instant;

#[derive(Model, Debug)]
struct Job {
    #[col(primary_key, auto)]
    id: i64,
    n: i64,
}

#[test]
#[ignore]
fn auto_id_cost_vs_table_size_probe() {
    let dir = std::env::temp_dir().join(format!("mitrdb_agp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<Job>().unwrap();
    for block in 0..10 {
        let t0 = Instant::now();
        for i in 0..2000 {
            db.insert_get_id(&Job {
                id: 0,
                n: block * 2000 + i,
            })
            .unwrap();
        }
        let us = t0.elapsed().as_secs_f64() * 1e6 / 2000.0;
        println!("AGP rows_before={} us_per_insert={:.0}", block * 2000, us);
    }
}
