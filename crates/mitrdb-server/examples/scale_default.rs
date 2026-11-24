use mitrdb_server::Server;
use std::sync::Arc;
use std::time::{Duration, Instant};
fn run(root: &str, threads: i64, n: i64, round: i64) -> f64 {
    let dir = format!("{root}/d_{threads}_{n}_{round}");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Arc::new(
        Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 256)
            .unwrap()
            .0,
    );
    s.execute_sql("CREATE TABLE sc (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let s = s.clone();
            std::thread::spawn(move || {
                let (mut id, mut rows) = (t * 100_000_000, 0i64);
                while Instant::now() < end {
                    let v: Vec<String> = (0..n)
                        .map(|k| format!("({}, {}, 'abc')", id + k, k))
                        .collect();
                    s.execute_sql(&format!("INSERT INTO sc VALUES {}", v.join(",")))
                        .unwrap();
                    id += n;
                    rows += n;
                }
                rows
            })
        })
        .collect();
    let total: i64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
    total as f64 / t0.elapsed().as_secs_f64()
}
fn main() {
    let root = std::env::args().nth(1).unwrap();
    for (n, name) in [(1i64, "single"), (100, "multi100")] {
        for th in [1i64, 2, 4, 8] {
            let mut v: Vec<f64> = (0..3).map(|i| run(&root, th, n, i)).collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "SD {name} threads={th} rows_s median={:.0} min={:.0} max={:.0}",
                v[1], v[0], v[2]
            );
        }
    }
}
