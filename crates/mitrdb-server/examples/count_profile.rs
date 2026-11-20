use mitrdb_server::Server;
use std::time::Instant;
fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let pool: usize = std::env::args().nth(2).and_then(|x| x.parse().ok()).unwrap_or(4096);
    let s = Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), pool).unwrap().0;
    s.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, n INT, t TEXT)").unwrap();
    let mut id = 0;
    while id < 300_000 {
        let v: Vec<String> = (0..1000).map(|k| format!("({}, 1, 'row-{}-xxxxxxxxxxxxxxxx')", id + k, id + k)).collect();
        s.execute_sql(&format!("INSERT INTO c VALUES {}", v.join(","))).unwrap();
        id += 1000;
    }
    println!("CP loaded");
    let t = Instant::now();
    for _ in 0..4 { let _ = s.execute_sql("SELECT COUNT(*) FROM c").unwrap(); }
    println!("CP pool={pool} count x4 secs={:.2} per_count={:.2}", t.elapsed().as_secs_f64(), t.elapsed().as_secs_f64() / 4.0);
}
