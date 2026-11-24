use mitrdb_server::Server;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn main() {
    let root = std::env::args().nth(1).unwrap();
    let _ = std::fs::remove_dir_all(&root);
    let src = format!("{root}/src");
    std::fs::create_dir_all(&src).unwrap();
    let s = Arc::new(
        Server::open(format!("{src}/t.db"), format!("{src}/t.wal"), 4096)
            .unwrap()
            .0,
    );
    s.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let base = 1_000_000i64;
    let mut id = 0;
    while id < base {
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
    println!("BD loaded {base} rows");

    let stop = Arc::new(AtomicBool::new(false));
    let hs: Vec<_> = (0..4i64)
        .map(|t| {
            let (s, stop) = (s.clone(), stop.clone());
            std::thread::spawn(move || {
                let (mut k, mut acked, mut errs) = (0i64, 0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    let id = 10_000_000 + t * 1_000_000 + k;
                    match s.execute_sql(&format!(
                        "INSERT INTO m VALUES ({id}, 1, 'row-{id}-xxxxxxxxxxxxxxxx')"
                    )) {
                        Ok(_) => acked += 1,
                        Err(_) => errs += 1,
                    }
                    k += 1;
                }
                (acked, errs)
            })
        })
        .collect();
    std::thread::sleep(std::time::Duration::from_millis(500));
    let t0 = Instant::now();
    let bdir = format!("{root}/bk");
    let r = s.backup(&bdir);
    println!(
        "BD backup secs={:.2} ok={}",
        t0.elapsed().as_secs_f64(),
        r.is_ok()
    );
    if let Err(e) = &r {
        println!("BD backup_err={e}");
    }
    stop.store(true, Ordering::Relaxed);
    let (mut a, mut e) = (0, 0);
    for h in hs {
        let (x, y) = h.join().unwrap();
        a += x;
        e += y;
    }
    println!("BD writers acked={a} errors={e}");
    drop(s);

    let dst = format!("{root}/dst");
    let t1 = Instant::now();
    let rr = Server::restore(&bdir, &dst);
    println!(
        "BD restore secs={:.2} ok={}",
        t1.elapsed().as_secs_f64(),
        rr.is_ok()
    );
    if let Err(e) = &rr {
        println!("BD restore_err={e}");
        return;
    }
    let mut names: Vec<String> = std::fs::read_dir(&dst)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    println!("BD restored_files={names:?}");
    let db_name = names
        .iter()
        .find(|n| n.ends_with(".db"))
        .expect("no .db in restored dir")
        .clone();
    let wal_name = db_name.trim_end_matches(".db").to_string() + ".wal";
    let s2 = Server::open(
        format!("{dst}/{db_name}"),
        format!("{dst}/{wal_name}"),
        4096,
    )
    .unwrap()
    .0;
    let rows = s2
        .table("m")
        .find()
        .columns(&["id", "n", "t"])
        .run()
        .unwrap()
        .rows;
    let mut seen = std::collections::HashSet::new();
    let (mut base_rows, mut extra, mut bad) = (0, 0, 0);
    for r in &rows {
        let d = format!("{:?}", r[0]);
        let id: i64 = d[d.find('(').unwrap() + 1..d.rfind(')').unwrap()]
            .parse()
            .unwrap();
        if !seen.insert(id) {
            bad += 1;
        }
        if !format!("{:?}", r[2]).contains(&format!("row-{id}-")) {
            bad += 1;
        }
        if id < base {
            base_rows += 1
        } else {
            extra += 1
        }
    }
    println!(
        "BD restored rows={} base={base_rows}/{base} writer_rows={extra} bad={bad}",
        rows.len()
    );
}
