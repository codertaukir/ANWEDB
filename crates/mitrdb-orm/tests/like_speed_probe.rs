use mitrdb_orm::*;
use std::time::Instant;

#[test]
fn like_speed_probe() {
    let dir = std::env::temp_dir().join(format!("mitrdb_likespd_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.execute_sql("CREATE TABLE w (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    db.execute_sql("CREATE INDEX idx_w_name ON w(name)").unwrap();
    db.execute_sql("BEGIN").unwrap();
    for i in 0..20000 {
        db.execute_sql(&format!("INSERT INTO w VALUES ({i}, 'user{:05}')", i)).unwrap();
    }
    db.execute_sql("COMMIT").unwrap();
    for (q, tag) in [
        ("SELECT id FROM w WHERE name LIKE 'user0001%'", "prefix_10rows"),
        ("SELECT id FROM w WHERE name LIKE '%0001%'", "contains_fullscan"),
    ] {
        let mut v = vec![];
        let mut n = 0;
        for _ in 0..9 {
            let t = Instant::now();
            let r = db.execute_sql(q).unwrap();
            v.push(t.elapsed().as_micros());
            n = format!("{:?}", r).matches("Int64").count();
        }
        v.sort();
        eprintln!("LS {tag} median_us={} min={} max={} int_cells={n}", v[4], v[0], v[8]);
    }
}
