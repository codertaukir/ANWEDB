use mitrdb_server::Server;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BASE: i64 = 10_000_000;

fn open(dir: &str) -> Server {
    let d = PathBuf::from(dir);
    std::fs::create_dir_all(&d).unwrap();
    Server::open(d.join("t.db"), d.join("t.wal"), 256).unwrap().0
}
// (kind P=intent A=ack, op I/U/D, id)
fn read_log(dir: &str, t: i64) -> Vec<(char, char, i64)> {
    let p = format!("{dir}/ack{t}.log");
    std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| {
        let mut it = l.split(' ');
        let k = it.next()?.chars().next()?;
        let c = it.next()?.chars().next()?;
        let id = it.next()?.parse().ok()?;
        if (k == 'P' || k == 'A') && "IUD".contains(c) { Some((k, c, id)) } else { None }
    }).collect()
}
fn num<T: std::fmt::Debug>(v: &T) -> i64 {
    let s = format!("{:?}", v);
    let a = s.find('(').unwrap() + 1;
    let b = s.rfind(')').unwrap();
    s[a..b].parse().unwrap()
}
fn op(s: &Server, f: &mut File, c: char, id: i64, sql: &str) {
    f.write_all(format!("P {c} {id}\n").as_bytes()).unwrap();
    s.execute_sql(sql).unwrap();
    f.write_all(format!("A {c} {id}\n").as_bytes()).unwrap();
}
fn state(c: char, id: i64) -> Option<i64> {
    match c { 'I' => Some(id), 'U' => Some(0), _ => None }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, dir) = (a[1].as_str(), a[2].clone());
    let s = Arc::new(open(&dir));
    s.table("soak").sync("id int primary key, owner int, val int").unwrap();
    if mode == "run" {
        let secs: u64 = a[3].parse().unwrap();
        let end = Instant::now() + Duration::from_secs(secs);
        let mut hs = vec![];
        for t in 0..4i64 {
            let (s, dir) = (s.clone(), dir.clone());
            hs.push(std::thread::spawn(move || {
                let log = read_log(&dir, t);
                let mut id = log.iter().map(|x| x.2).max().map(|m| m + 1).unwrap_or(t * BASE + 1);
                let mut f = std::fs::OpenOptions::new().create(true).append(true).open(format!("{dir}/ack{t}.log")).unwrap();
                while Instant::now() < end {
                    op(&s, &mut f, 'I', id, &format!("INSERT INTO soak VALUES ({id}, {t}, {id})"));
                    op(&s, &mut f, 'U', id, &format!("UPDATE soak SET val = 0 WHERE id = {id}"));
                    if id % 3 == 0 {
                        op(&s, &mut f, 'D', id, &format!("DELETE FROM soak WHERE id = {id}"));
                    }
                    id += 1;
                }
            }));
        }
        for h in hs { h.join().unwrap(); }
        return;
    }
    // verify: acked state must hold; the one in-flight (intent without ack) op may be either state
    let rows = s.table("soak").find().columns(&["id", "val"]).run().unwrap().rows;
    let tbl: HashMap<i64, i64> = rows.iter().map(|r| (num(&r[0]), num(&r[1]))).collect();
    let (mut checked, mut lost, mut phantom) = (0, 0, 0);
    let mut known: std::collections::HashSet<i64> = Default::default();
    for t in 0..4i64 {
        let mut per: HashMap<i64, Vec<(char, char)>> = HashMap::new();
        for (k, c, id) in read_log(&dir, t) { per.entry(id).or_default().push((k, c)); }
        for (id, ev) in &per {
            known.insert(*id);
            checked += 1;
            let acked = ev.iter().rev().find(|e| e.0 == 'A').map(|e| e.1);
            let mut allowed = vec![acked.map(|c| state(c, *id)).unwrap_or(None)];
            if let Some(&(k, c)) = ev.last() { if k == 'P' { allowed.push(state(c, *id)); } }
            let got = tbl.get(id).copied();
            if !allowed.contains(&got) {
                lost += 1;
                eprintln!("SOAK LOST id={id} acked={acked:?} table={got:?} allowed={allowed:?}");
            }
        }
    }
    for id in tbl.keys() {
        if !known.contains(id) { phantom += 1; eprintln!("SOAK PHANTOM id={id}"); }
    }
    println!("SOAK verify rows={} checked={} lost={} phantom={}", tbl.len(), checked, lost, phantom);
    if lost + phantom > 0 { std::process::exit(1); }
}
