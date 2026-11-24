use mitrdb_server::Server;
use std::time::Instant;

fn open(dir: &str) -> Server {
    Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 4096)
        .unwrap()
        .0
}
fn wal_mb(dir: &str) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("wal"))
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum::<u64>()
        / 1_000_000
}

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let total: i64 = 1_000_000;
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = open(&dir);
    s.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let mut id = 0i64;
    while id < total {
        let v: Vec<String> = (0..1000)
            .map(|k| {
                format!(
                    "({}, {}, 'row-{}-xxxxxxxxxxxxxxxx')",
                    id + k,
                    (id + k) % 1000,
                    id + k
                )
            })
            .collect();
        s.execute_sql(&format!("INSERT INTO m VALUES {}", v.join(",")))
            .unwrap();
        id += 1000;
    }
    println!("RD loaded, wal_mb={}", wal_mb(&dir));
    drop(s);
    let t = Instant::now();
    let s = open(&dir);
    println!(
        "RD reopen_no_checkpoint secs={:.2} wal_mb_after={}",
        t.elapsed().as_secs_f64(),
        wal_mb(&dir)
    );
    drop(s);
    let t = Instant::now();
    let s = open(&dir);
    println!("RD reopen_again secs={:.2}", t.elapsed().as_secs_f64());
    let t = Instant::now();
    s.execute_sql("CHECKPOINT").unwrap();
    println!(
        "RD checkpoint secs={:.2} wal_mb_after={}",
        t.elapsed().as_secs_f64(),
        wal_mb(&dir)
    );
    drop(s);
    let t = Instant::now();
    let _s = open(&dir);
    println!(
        "RD reopen_after_checkpoint secs={:.2}",
        t.elapsed().as_secs_f64()
    );
}
