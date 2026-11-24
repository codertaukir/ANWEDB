use mitrdb_server::Server;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

fn run(root: &str, threads: i64, mode: &str, round: i64) -> (f64, i64) {
    let dir = format!("{root}/{mode}_{threads}_{round}");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Arc::new(
        Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 256)
            .unwrap()
            .0,
    );
    s.execute_sql("CREATE TABLE sc (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let errs = Arc::new(AtomicI64::new(0));
    let end = Instant::now() + Duration::from_secs(5);
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let (s, errs, mode) = (s.clone(), errs.clone(), mode.to_string());
            std::thread::spawn(move || {
                let sid = s.create_session();
                let mut id = t * 100_000_000;
                let mut rows = 0i64;
                let stmt = |id: i64, n: i64| {
                    let v: Vec<String> = (0..n)
                        .map(|k| format!("({}, {}, 'abc')", id + k, k))
                        .collect();
                    format!("INSERT INTO sc VALUES {}", v.join(","))
                };
                while Instant::now() < end {
                    match mode.as_str() {
                        "single" | "multi100" => {
                            let n = if mode == "single" { 1 } else { 100 };
                            match s.execute_in_session(sid, &stmt(id, n)) {
                                Ok(_) => rows += n,
                                Err(e) => {
                                    if errs.fetch_add(1, Ordering::Relaxed) < 3 {
                                        eprintln!("SSERR {:?}", e);
                                    }
                                }
                            }
                            id += n;
                        }
                        _ => {
                            if let Err(e) = s.execute_in_session(sid, "BEGIN") {
                                if errs.fetch_add(1, Ordering::Relaxed) < 3 {
                                    eprintln!("SSERR {:?}", e);
                                }
                                continue;
                            }
                            let mut ok = 0;
                            for _ in 0..100 {
                                match s.execute_in_session(sid, &stmt(id, 100)) {
                                    Ok(_) => ok += 100,
                                    Err(e) => {
                                        if errs.fetch_add(1, Ordering::Relaxed) < 3 {
                                            eprintln!("SSERR {:?}", e);
                                        }
                                    }
                                }
                                id += 100;
                            }
                            match s.execute_in_session(sid, "COMMIT") {
                                Ok(_) => rows += ok,
                                Err(e) => {
                                    if errs.fetch_add(1, Ordering::Relaxed) < 3 {
                                        eprintln!("SSERR {:?}", e);
                                    }
                                    let _ = s.execute_in_session(sid, "ROLLBACK");
                                }
                            }
                        }
                    }
                }
                let _ = s.close_session(sid);
                rows
            })
        })
        .collect();
    let total: i64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
    (
        total as f64 / t0.elapsed().as_secs_f64(),
        errs.load(Ordering::Relaxed),
    )
}

fn main() {
    let root = std::env::args().nth(1).unwrap();
    for mode in ["single", "multi100", "txn"] {
        if let Some(m) = std::env::args().nth(2)
            && m != mode
        {
            continue;
        }
        for th in [1i64, 2, 4, 8] {
            let r: Vec<(f64, i64)> = (0..3).map(|i| run(&root, th, mode, i)).collect();
            let mut v: Vec<f64> = r.iter().map(|x| x.0).collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let errs: i64 = r.iter().map(|x| x.1).sum();
            println!(
                "SS {mode} threads={th} rows_s median={:.0} min={:.0} max={:.0} errors={errs}",
                v[1], v[0], v[2]
            );
        }
    }
}
