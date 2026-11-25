use mitrdb_server::Server;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

const OWN: i64 = 10_000_000;

fn num(s: &str) -> i64 {
    s[s.find('(').unwrap() + 1..s.rfind(')').unwrap()]
        .trim()
        .parse()
        .unwrap()
}
fn ids(root: &str, kind: &str) -> HashSet<i64> {
    let mut set = HashSet::new();
    for t in 0..4 {
        if let Ok(txt) = std::fs::read_to_string(format!("{root}/{kind}{t}.log")) {
            for l in txt.lines() {
                if let Ok(v) = l.trim().parse::<i64>() {
                    set.insert(v);
                }
            }
        }
    }
    set
}
fn col(s: &Server, tbl: &str, c: &str) -> Vec<i64> {
    s.table(tbl)
        .find()
        .columns(&[c])
        .run()
        .unwrap()
        .rows
        .iter()
        .map(|r| num(&format!("{:?}", r[0])))
        .collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, root) = (a[1].as_str(), a[2].clone());
    std::fs::create_dir_all(&root).unwrap();
    let s = Arc::new(
        Server::open(format!("{root}/t.db"), format!("{root}/t.wal"), 256)
            .unwrap()
            .0,
    );
    s.execute_sql("CREATE TABLE IF NOT EXISTS a (id INT PRIMARY KEY, p INT)")
        .unwrap();
    s.execute_sql("CREATE TABLE IF NOT EXISTS b (id INT PRIMARY KEY, p INT)")
        .unwrap();
    s.execute_sql("CREATE TABLE IF NOT EXISTS c (k INT PRIMARY KEY, n INT)")
        .unwrap();
    let have: HashSet<i64> = col(&s, "c", "k").into_iter().collect();
    for k in 0..4i64 {
        if !have.contains(&k) {
            s.execute_sql(&format!("INSERT INTO c VALUES ({k}, 0)"))
                .unwrap();
        }
    }
    if mode == "run" {
        if let Some(cs) = std::env::var("PL_CKPT_SECS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
        {
            let s2 = s.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs_f64(cs));
                    let _ = s2.checkpoint();
                }
            });
        }
        for t in 0..4i64 {
            let (s, root) = (s.clone(), root.clone());
            std::thread::spawn(move || {
                let sid = s.create_session();
                let mut id = ids(&root, "intent")
                    .into_iter()
                    .filter(|i| i / OWN == t)
                    .max()
                    .map(|m| m + 1)
                    .unwrap_or(t * OWN);
                let open = |k: &str| {
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(format!("{root}/{k}{t}.log"))
                        .unwrap()
                };
                let (mut fi, mut fa) = (open("intent"), open("ack"));
                loop {
                    writeln!(fi, "{id}").unwrap();
                    fi.sync_data().unwrap();
                    let q = [
                        "BEGIN".to_string(),
                        format!("INSERT INTO a VALUES ({id}, {t})"),
                        format!("INSERT INTO b VALUES ({id}, {t})"),
                        format!("UPDATE c SET n = n + 1 WHERE k = {t}"),
                        "COMMIT".to_string(),
                    ];
                    let ok = q.iter().all(|x| s.execute_in_session(sid, x).is_ok());
                    if ok {
                        writeln!(fa, "{id}").unwrap();
                        fa.sync_data().unwrap();
                    } else {
                        let _ = s.execute_in_session(sid, "ROLLBACK");
                    }
                    id += 1;
                }
            });
        }
        loop {
            std::thread::sleep(Duration::from_secs(1));
            println!("RUNNING (READY FOR POWER CUT)");
        }
    }
    let (intent, acked) = (ids(&root, "intent"), ids(&root, "ack"));
    let pa: HashSet<i64> = col(&s, "a", "id").into_iter().filter(|i| *i >= 0).collect();
    let pb: HashSet<i64> = col(&s, "b", "id").into_iter().filter(|i| *i >= 0).collect();
    let torn = pa.symmetric_difference(&pb).count();
    let lost = acked
        .iter()
        .filter(|i| !pa.contains(i) || !pb.contains(i))
        .count();
    let phantom = pa.iter().filter(|i| !intent.contains(i)).count();
    let cn: HashMap<i64, i64> = s
        .table("c")
        .find()
        .columns(&["k", "n"])
        .run()
        .unwrap()
        .rows
        .iter()
        .map(|r| (num(&format!("{:?}", r[0])), num(&format!("{:?}", r[1]))))
        .collect();
    let mut cbad = 0;
    for t in 0..4i64 {
        let cnt = pa.iter().filter(|i| **i / OWN == t).count() as i64;
        if cn.get(&t) != Some(&cnt) {
            cbad += 1;
            eprintln!("COUNTER t={t} n={:?} rows={cnt}", cn.get(&t));
        }
    }
    let u = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
        % 1_000_000_000;
    let base = -(u + 1) * 100_000;
    let mut fail = 0;
    for i in 0..1000i64 {
        let k = base - i;
        let ok = [
            "BEGIN".to_string(),
            format!("INSERT INTO a VALUES ({k}, 9)"),
            format!("INSERT INTO b VALUES ({k}, 9)"),
            "COMMIT".to_string(),
        ]
        .iter()
        .all(|x| s.execute_sql(x).is_ok());
        if !ok {
            fail += 1;
            let _ = s.execute_sql("ROLLBACK");
        }
    }
    println!(
        "TXN verify a={} b={} acked={} lost={lost} torn={torn} phantom={phantom} counter_bad={cbad} after2_fail={fail}",
        pa.len(),
        pb.len(),
        acked.len()
    );
    if lost + torn + phantom + cbad + fail > 0 {
        std::process::exit(1);
    }
}
