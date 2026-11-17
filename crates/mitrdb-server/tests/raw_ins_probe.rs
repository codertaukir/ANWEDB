use mitrdb_server::Server;
use std::time::Instant;
#[test]
fn raw_ins_probe() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ri_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap().0;
    s.execute_sql("CREATE TABLE p (id INT PRIMARY KEY, n INT, t TEXT)").unwrap();
    let mut best = 0f64;
    for r in 0..3 {
        let t0 = Instant::now();
        s.execute_sql("BEGIN").unwrap();
        for i in 0..5000 {
            s.execute_sql(&format!("INSERT INTO p VALUES ({}, {}, 'abc')", r * 5000 + i, i)).unwrap();
        }
        s.execute_sql("COMMIT").unwrap();
        best = best.max(5000.0 / t0.elapsed().as_secs_f64());
    }
    let t0 = Instant::now();
    for i in 0..200 {
        s.execute_sql(&format!("INSERT INTO p VALUES ({}, 0, 'x')", 100000 + i)).unwrap();
    }
    let auto = t0.elapsed().as_micros() / 200;
    eprintln!("RI batch_rows_s={:.0} auto_us={}", best, auto);
}
