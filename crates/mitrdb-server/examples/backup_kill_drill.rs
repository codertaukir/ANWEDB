use mitrdb_server::Server;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const BASE: i64 = 200_000;
const WB: i64 = 10_000_000;

fn num(s: &str) -> i64 {
    let a = s.find('(').unwrap() + 1;
    let b = s.rfind(')').unwrap();
    s[a..b].trim().parse().unwrap()
}

fn read_log(root: &str, t: i64) -> Vec<(char, char, i64)> {
    std::fs::read_to_string(format!("{root}/bkack{t}.log"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let mut it = l.split(' ');
            let k = it.next()?.chars().next()?;
            let c = it.next()?.chars().next()?;
            let id = it.next()?.parse().ok()?;
            if (k == 'P' || k == 'A') && "IUD".contains(c) {
                Some((k, c, id))
            } else {
                None
            }
        })
        .collect()
}

fn st(c: char, id: i64) -> Option<i64> {
    match c {
        'I' => Some(id),
        'U' => Some(0),
        _ => None,
    }
}

fn op(s: &Server, f: &mut File, c: char, id: i64, sql: &str) {
    f.write_all(format!("P {c} {id}\n").as_bytes()).unwrap();
    f.sync_data().unwrap();
    s.execute_sql(sql).unwrap();
    f.write_all(format!("A {c} {id}\n").as_bytes()).unwrap();
    f.sync_data().unwrap();
}

fn scan(s: &Server) -> Result<Vec<(i64, i64)>, String> {
    let rows = s
        .table("bk")
        .find()
        .columns(&["id", "val"])
        .run()
        .map_err(|e| format!("scan {e:?}"))?
        .rows;
    Ok(rows
        .iter()
        .map(|r| (num(&format!("{:?}", r[0])), num(&format!("{:?}", r[1]))))
        .collect())
}

fn known_ids(root: &str) -> HashSet<i64> {
    let mut k = HashSet::new();
    for t in 0..4 {
        for (_, _, id) in read_log(root, t) {
            k.insert(id);
        }
    }
    k
}

fn check_restored(dst: &str, known: &HashSet<i64>) -> Result<usize, String> {
    let names: Vec<String> = std::fs::read_dir(dst)
        .map_err(|e| e.to_string())?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    let db = names
        .iter()
        .find(|n| n.ends_with(".db"))
        .ok_or_else(|| "no .db".to_string())?
        .clone();
    let wal = db.trim_end_matches(".db").to_string() + ".wal";
    let s = Server::open(format!("{dst}/{db}"), format!("{dst}/{wal}"), 1024)
        .map_err(|e| format!("open {e:?}"))?
        .0;
    let rows = scan(&s)?;
    let mut seen = HashSet::new();
    let mut base = 0;
    for (id, val) in &rows {
        if !seen.insert(*id) {
            return Err(format!("dup id {id}"));
        }
        if *id < 0 {
            continue;
        }
        if *id < WB {
            base += 1;
            if *val != *id {
                return Err(format!("base id {id} val {val}"));
            }
        } else {
            if !known.contains(id) {
                return Err(format!("phantom id {id}"));
            }
            if *val != *id && *val != 0 {
                return Err(format!("writer id {id} bad val {val}"));
            }
        }
    }
    if base != BASE {
        return Err(format!("base rows {base}/{BASE}"));
    }
    Ok(rows.len())
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, root) = (a[1].as_str(), a[2].clone());
    let src = format!("{root}/src");
    let marker = format!("{root}/loaded");
    if mode == "run" {
        std::fs::create_dir_all(format!("{root}/bk")).unwrap();
        if !std::path::Path::new(&marker).exists() {
            let _ = std::fs::remove_dir_all(&src);
        }
        std::fs::create_dir_all(&src).unwrap();
        let s = Arc::new(
            Server::open(format!("{src}/t.db"), format!("{src}/t.wal"), 4096)
                .unwrap()
                .0,
        );
        s.execute_sql("CREATE TABLE IF NOT EXISTS bk (id INT PRIMARY KEY, owner INT, val INT)")
            .unwrap();
        if !std::path::Path::new(&marker).exists() {
            let mut id = 0;
            while id < BASE {
                let v: Vec<String> = (0..1000)
                    .map(|k| format!("({}, -1, {})", id + k, id + k))
                    .collect();
                s.execute_sql(&format!("INSERT INTO bk VALUES {}", v.join(",")))
                    .unwrap();
                id += 1000;
            }
            std::fs::write(&marker, "1").unwrap();
        }
        let cnt = Arc::new(AtomicU64::new(0));
        for t in 0..4i64 {
            let (s, root) = (s.clone(), root.clone());
            std::thread::spawn(move || {
                let mut id = read_log(&root, t)
                    .iter()
                    .map(|x| x.2)
                    .max()
                    .map(|m| m + 1)
                    .unwrap_or(WB + t * 1_000_000);
                let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(format!("{root}/bkack{t}.log"))
                    .unwrap();
                loop {
                    op(
                        &s,
                        &mut f,
                        'I',
                        id,
                        &format!("INSERT INTO bk VALUES ({id}, {t}, {id})"),
                    );
                    op(
                        &s,
                        &mut f,
                        'U',
                        id,
                        &format!("UPDATE bk SET val = 0 WHERE id = {id}"),
                    );
                    if id % 3 == 0 {
                        op(
                            &s,
                            &mut f,
                            'D',
                            id,
                            &format!("DELETE FROM bk WHERE id = {id}"),
                        );
                    }
                    id += 1;
                }
            });
        }
        {
            let (s, root, cnt) = (s.clone(), root.clone(), cnt.clone());
            std::thread::spawn(move || {
                let mut n = std::fs::read_dir(format!("{root}/bk"))
                    .unwrap()
                    .filter_map(|e| e.ok()?.file_name().to_str()?.parse::<u64>().ok())
                    .max()
                    .unwrap_or(0);
                loop {
                    n += 1;
                    let _ = s.backup(format!("{root}/bk/{n}"));
                    cnt.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        loop {
            std::thread::sleep(Duration::from_secs(1));
            println!("RUNNING backups_started={}", cnt.load(Ordering::Relaxed));
        }
    }
    // verify
    let s = Server::open(format!("{src}/t.db"), format!("{src}/t.wal"), 4096)
        .unwrap()
        .0;
    let rows = scan(&s).unwrap();
    let tbl: HashMap<i64, i64> = rows.iter().copied().collect();
    let dup = rows.len() - tbl.len();
    let mut per: HashMap<i64, Vec<(char, char)>> = HashMap::new();
    for t in 0..4 {
        for (k, c, id) in read_log(&root, t) {
            per.entry(id).or_default().push((k, c));
        }
    }
    let (mut lost, mut phantom) = (0, 0);
    for (id, ev) in &per {
        let acked = ev.iter().rev().find(|e| e.0 == 'A').map(|e| e.1);
        let mut allowed = vec![acked.and_then(|c| st(c, *id))];
        if let Some(&(k, c)) = ev.last()
            && k == 'P'
        {
            allowed.push(st(c, *id));
        }
        if !allowed.contains(&tbl.get(id).copied()) {
            lost += 1;
            eprintln!("LOST id={id} table={:?} allowed={allowed:?}", tbl.get(id));
        }
    }
    for id in tbl.keys() {
        if *id >= WB && !per.contains_key(id) {
            phantom += 1;
        }
    }
    let base = tbl.keys().filter(|i| **i >= 0 && **i < WB).count();
    println!(
        "SRC rows={} base={base}/{BASE} dup={dup} lost={lost} phantom={phantom}",
        rows.len()
    );
    let known = known_ids(&root);
    let mut dirs: Vec<u64> = std::fs::read_dir(format!("{root}/bk"))
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect();
    dirs.sort();
    let dirs: Vec<u64> = dirs.into_iter().rev().take(6).collect();
    let (mut ok, mut refused, mut bad) = (0, 0, 0);
    for n in dirs {
        let bdir = format!("{root}/bk/{n}");
        let dst = format!("{root}/rst/{n}");
        let _ = std::fs::remove_dir_all(&dst);
        std::fs::create_dir_all(format!("{root}/rst")).unwrap();
        let v = Server::validate_backup(&bdir);
        let r = Server::restore(&bdir, &dst);
        match (&v, &r) {
            (Err(_), Err(_)) => {
                refused += 1;
                println!("BK {n} REFUSED (validate+restore err)");
            }
            (Ok(_), Ok(_)) => match check_restored(&dst, &known) {
                Ok(rows) => {
                    ok += 1;
                    println!("BK {n} OK rows={rows}");
                }
                Err(e) => {
                    bad += 1;
                    println!("BK {n} BAD {e}");
                }
            },
            _ => {
                bad += 1;
                println!(
                    "BK {n} BAD validate={:?} restore={:?} err={:?}",
                    v.is_ok(),
                    r.is_ok(),
                    r.as_ref().err()
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dst);
    }
    let u = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
        % 1_000_000_000;
    let b = -(u + 1) * 100_000;
    let mut fail = 0;
    for i in 0..2000i64 {
        if s.execute_sql(&format!("INSERT INTO bk VALUES ({}, 9, 0)", b - i))
            .is_err()
        {
            fail += 1;
        }
    }
    println!("BKSUM ok={ok} refused={refused} bad={bad} after2_fail={fail}");
    if lost + phantom + dup + bad + fail > 0 || base != BASE as usize {
        std::process::exit(1);
    }
}
