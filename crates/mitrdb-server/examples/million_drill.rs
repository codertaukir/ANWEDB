use mitrdb_server::Server;
use std::time::Instant;

fn open(dir: &str) -> Server {
    Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 4096)
        .unwrap()
        .0
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = a[1].as_str();
    let total: i64 = a.get(2).and_then(|x| x.parse().ok()).unwrap_or(1_000_000);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    let s = open(dir);
    s.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let t0 = Instant::now();
    let mut seg = Instant::now();
    let chunk = 1000i64;
    let mut id = 0i64;
    while id < total {
        let v: Vec<String> = (0..chunk)
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
        id += chunk;
        if id % 100_000 == 0 {
            println!(
                "MD insert rows={id} last100k_rows_s={:.0}",
                100_000.0 / seg.elapsed().as_secs_f64()
            );
            seg = Instant::now();
        }
    }
    println!(
        "MD insert_total secs={:.1} rows_s={:.0}",
        t0.elapsed().as_secs_f64(),
        total as f64 / t0.elapsed().as_secs_f64()
    );
    let wal_sz: u64 = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("wal.0"))
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    println!("MD wal_mb_before_drop={}", wal_sz / 1_000_000);
    drop(s);

    let t1 = Instant::now();
    let s = open(dir);
    println!("MD reopen secs={:.2}", t1.elapsed().as_secs_f64());
    drop(s);
    let t1b = Instant::now();
    let s = open(dir);
    println!("MD reopen_second secs={:.2}", t1b.elapsed().as_secs_f64());

    let t2 = Instant::now();
    let c = s
        .table("m")
        .find()
        .columns(&["id"])
        .run()
        .unwrap()
        .rows
        .len();
    println!(
        "MD full_scan rows={c} expected={total} secs={:.2} ok={}",
        t2.elapsed().as_secs_f64(),
        c as i64 == total
    );

    let t3 = Instant::now();
    let mut ok = 0;
    for k in 0..1000i64 {
        let id = (k * 7919) % total;
        let r = s
            .execute_sql(&format!("SELECT id FROM m WHERE id = {id}"))
            .unwrap();
        if format!("{:?}", r).contains(&id.to_string()) {
            ok += 1;
        }
    }
    println!(
        "MD point_lookup 1000 ok={ok} avg_us={:.0}",
        t3.elapsed().as_secs_f64() * 1e6 / 1000.0
    );

    let t4 = Instant::now();
    s.execute_sql("CREATE INDEX m_n ON m (n)").unwrap();
    println!("MD create_index secs={:.2}", t4.elapsed().as_secs_f64());
    let t5 = Instant::now();
    let _ = s.execute_sql("SELECT id FROM m WHERE n = 7").unwrap();
    println!("MD index_lookup n=7 secs={:.3}", t5.elapsed().as_secs_f64());
}
