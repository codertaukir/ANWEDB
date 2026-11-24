use mitrdb_server::Server;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn open(dir: &str) -> Server {
    Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 256)
        .unwrap()
        .0
}

fn pid(s: &str) -> i64 {
    let a = s.find('(').unwrap() + 1;
    let b = s.rfind(')').unwrap();
    s[a..b].trim().parse().unwrap()
}

fn load_all(prefix: &str, threads: usize) -> HashSet<i64> {
    let mut set = HashSet::new();
    for t in 0..threads {
        let path = format!("{prefix}.t{t}");
        if let Ok(txt) = std::fs::read_to_string(&path) {
            for line in txt.lines() {
                if let Ok(v) = line.trim().parse::<i64>() {
                    set.insert(v);
                }
            }
        }
    }
    set
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: powerloss_drill <write|verify> <db_dir> <log_prefix> [threads]");
        std::process::exit(2);
    }
    let mode = args[1].as_str();
    let dir = args[2].as_str();
    let log = args[3].as_str();
    let threads: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);

    if mode == "write" {
        std::fs::create_dir_all(dir).unwrap();
        let s = Arc::new(open(dir));
        s.execute_sql("CREATE TABLE IF NOT EXISTS pl (id INT PRIMARY KEY, p TEXT)")
            .unwrap();

        let existing = s.table("pl").find().columns(&["id"]).run().unwrap().rows;
        let max_id = existing
            .iter()
            .map(|r| pid(&format!("{:?}", r[0])))
            .filter(|&id| id >= 0)
            .max()
            .unwrap_or(-1);
        let base_id = (max_id + 1) as u64;

        let acked_total = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let s = Arc::clone(&s);
            let acked_total = Arc::clone(&acked_total);
            let stop = Arc::clone(&stop);
            let intent_path = format!("{log}.intent.t{t}");
            let ack_path = format!("{log}.ack.t{t}");

            handles.push(std::thread::spawn(move || {
                let mut intent: File = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&intent_path)
                    .unwrap();
                let mut ack: File = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&ack_path)
                    .unwrap();

                let mut seq: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    let id = (base_id + seq * (threads as u64) + (t as u64)) as i64;
                    seq += 1;

                    writeln!(intent, "{id}").unwrap();
                    intent.sync_data().unwrap();

                    let sql = format!(
                        "INSERT INTO pl VALUES ({id}, 'payload-{id}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"
                    );
                    if s.execute_sql(&sql).is_ok() {
                        writeln!(ack, "{id}").unwrap();
                        ack.sync_data().unwrap();
                        acked_total.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        loop {
            std::thread::sleep(Duration::from_secs(1));
            let n = acked_total.load(Ordering::Relaxed);
            println!(
                "RUNNING elapsed={:.1}s acked={n} (READY FOR POWER CUT)",
                start.elapsed().as_secs_f64()
            );
        }
    } else if mode == "verify" {
        let s = open(dir);
        let acked = load_all(&format!("{log}.ack"), threads);
        let attempted = load_all(&format!("{log}.intent"), threads);
        let rows = s
            .table("pl")
            .find()
            .columns(&["id", "p"])
            .run()
            .unwrap()
            .rows;

        let mut present = HashSet::new();
        let mut bad_payload = 0usize;
        for r in &rows {
            let id = pid(&format!("{:?}", r[0]));
            if id >= 0 && !format!("{:?}", r[1]).contains(&format!("payload-{id}-")) {
                bad_payload += 1;
            }
            present.insert(id);
        }

        let lost = acked.difference(&present).count();
        let phantom = present
            .iter()
            .filter(|i| **i >= 0 && !attempted.contains(i))
            .count();
        let dup = rows.len() - present.len();

        let uid = -1
            - (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % 1_000_000_000) as i64;
        let res_after = s.execute_sql(&format!(
            "INSERT INTO pl VALUES ({uid}, 'payload-{uid}-after')"
        ));
        if let Err(ref e) = res_after {
            eprintln!("INSERT_AFTER_ERR: {e:?}");
        }
        let ok_after = res_after.is_ok();

        let mut after_fail = 0usize;
        for i in 0..2000i64 {
            let k = uid * 100_000 - i;
            if s
                .execute_sql(&format!("INSERT INTO pl VALUES ({k}, 'payload-{k}-after')"))
                .is_err()
            {
                after_fail += 1;
            }
        }
        let rows2 = s.table("pl").find().columns(&["id"]).run().unwrap().rows;
        let present2: HashSet<i64> = rows2.iter().map(|r| pid(&format!("{:?}", r[0]))).collect();
        let lost2 = acked.difference(&present2).count();
        println!(
            "AFTER2 inserts=2000 fail={after_fail} rows_now={} lost_after_inserts={lost2}",
            rows2.len()
        );

        println!(
            "VERIFY rows={} acked={} attempted={} lost={lost} phantom={phantom} dup={dup} bad_payload={bad_payload} insert_after_ok={ok_after}",
            rows.len(),
            acked.len(),
            attempted.len()
        );
    } else {
        eprintln!("unknown mode: {mode}");
        std::process::exit(2);
    }
}
