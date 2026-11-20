use mitrdb_server::Server;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn open(dir: &str) -> Server {
    Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 4096).unwrap().0
}
fn hwm_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status").unwrap_or_default().lines()
        .find(|l| l.starts_with("VmHWM"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0) / 1024
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = a[1].as_str();
    let total: i64 = a[2].parse().unwrap();
    let threads: i64 = a[3].parse().unwrap();
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    let s = Arc::new(open(dir));
    s.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, n INT, t TEXT)").unwrap();
    let done = Arc::new(AtomicI64::new(0));
    let per = total / threads;
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads).map(|t| {
        let (s, done) = (s.clone(), done.clone());
        std::thread::spawn(move || {
            let base = t * per;
            let mut k = 0;
            while k < per {
                let v: Vec<String> = (0..1000).map(|j| {
                    let id = base + k + j;
                    format!("({id}, {}, 'row-{id}-xxxxxxxxxxxxxxxx')", id % 1000)
                }).collect();
                s.execute_sql(&format!("INSERT INTO m VALUES {}", v.join(","))).unwrap();
                k += 1000;
                done.fetch_add(1000, Ordering::Relaxed);
            }
        })
    }).collect();
    let (mut next, mut seg) = (1_000_000i64, Instant::now());
    while done.load(Ordering::Relaxed) < per * threads {
        std::thread::sleep(Duration::from_millis(200));
        if done.load(Ordering::Relaxed) >= next {
            println!("TM threads={threads} rows={next} last1M_rows_s={:.0} rss_mb={}", 1_000_000.0 / seg.elapsed().as_secs_f64(), hwm_mb());
            seg = Instant::now();
            next += 1_000_000;
        }
    }
    for h in hs { h.join().unwrap(); }
    let secs = t0.elapsed().as_secs_f64();
    println!("TM threads={threads} insert_total secs={secs:.1} rows_s={:.0} peak_rss_mb={}", (per * threads) as f64 / secs, hwm_mb());
    drop(s);

    let t1 = Instant::now();
    let s = open(dir);
    println!("TM reopen secs={:.2}", t1.elapsed().as_secs_f64());
    let t3 = Instant::now();
    let mut ok = 0;
    for k in 0..1000i64 {
        let id = (k * 7919) % (per * threads);
        let r = s.execute_sql(&format!("SELECT id FROM m WHERE id = {id}")).unwrap();
        if format!("{:?}", r).contains(&id.to_string()) { ok += 1; }
    }
    println!("TM point_lookup ok={ok}/1000 avg_us={:.0}", t3.elapsed().as_secs_f64() * 1e6 / 1000.0);
    println!("TM rss_before_count_mb={}", hwm_mb());
    let t2 = Instant::now();
    let r = s.execute_sql("SELECT COUNT(*) FROM m").unwrap();
    println!("TM count secs={:.2} result={:?} peak_rss_mb={}", t2.elapsed().as_secs_f64(), r, hwm_mb());
}
