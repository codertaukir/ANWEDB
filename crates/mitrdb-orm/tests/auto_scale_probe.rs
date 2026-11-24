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
fn auto_id_thread_scaling_probe() {
    for th in [1i64, 2, 4] {
        let dir = std::env::temp_dir().join(format!("mitrdb_asp_{}_{}", std::process::id(), th));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
        db.sync::<Job>().unwrap();
        let db = &db;
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for t in 0..th {
                s.spawn(move || {
                    for i in 0..500 {
                        db.insert_get_id(&Job {
                            id: 0,
                            n: t * 1000 + i,
                        })
                        .unwrap();
                    }
                });
            }
        });
        let e = t0.elapsed().as_secs_f64();
        let total = th * 500;
        println!(
            "ASP threads={th} total={total} secs={e:.2} us_per_insert={:.0}",
            e * 1e6 / total as f64
        );
    }
}
