use mitrdb_orm::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BASE: i64 = 10_000_000;

#[derive(Model, Debug)]
#[model(table = "sk_parent")]
struct SParent {
    #[col(primary_key)]
    id: i64,
}
#[derive(Model, Debug)]
#[model(table = "sk_child")]
struct SChild {
    #[col(primary_key)]
    id: i64,
    #[col(references = "sk_parent.id")]
    parent_id: i64,
}

fn read_log(dir: &str, t: i64) -> Vec<(char, char, i64)> {
    std::fs::read_to_string(format!("{dir}/fk{t}.log")).unwrap_or_default().lines().filter_map(|l| {
        let mut it = l.split(' ');
        let k = it.next()?.chars().next()?;
        let c = it.next()?.chars().next()?;
        let id = it.next()?.parse().ok()?;
        if (k == 'P' || k == 'A') && "IC".contains(c) || (k == 'P' || k == 'A') && c == 'D' { Some((k, c, id)) } else { None }
    }).collect()
}
fn logged<F: FnOnce()>(f: &mut File, c: char, id: i64, run: F) {
    f.write_all(format!("P {c} {id}\n").as_bytes()).unwrap();
    run();
    f.write_all(format!("A {c} {id}\n").as_bytes()).unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, dir) = (a[1].clone(), a[2].clone());
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 256).unwrap();
    let db = Arc::new(db);
    db.sync::<SParent>().unwrap();
    db.sync::<SChild>().unwrap();
    if mode == "run" {
        let end = Instant::now() + Duration::from_secs(a[3].parse().unwrap());
        let mut hs = vec![];
        for t in 0..4i64 {
            let (db, dir) = (db.clone(), dir.clone());
            hs.push(std::thread::spawn(move || {
                let log = read_log(&dir, t);
                let mut id = log.iter().map(|x| x.2).max().map(|m| m + 1).unwrap_or(t * BASE + 1);
                let mut f = std::fs::OpenOptions::new().create(true).append(true).open(format!("{dir}/fk{t}.log")).unwrap();
                while Instant::now() < end {
                    // 'I' = parent insert, 'C' = child insert (same id), 'D' = child delete
                    logged(&mut f, 'I', id, || { db.insert(&SParent { id }).unwrap(); });
                    logged(&mut f, 'C', id, || { db.insert(&SChild { id, parent_id: id }).unwrap(); });
                    if id % 3 == 0 {
                        logged(&mut f, 'D', id, || { db.delete::<SChild>().filter(SChild::id.eq(id)).run().unwrap(); });
                    }
                    id += 1;
                }
            }));
        }
        for h in hs { h.join().unwrap(); }
        return;
    }
    let parents: HashSet<i64> = db.all::<SParent>().run().unwrap().iter().map(|p| p.id).collect();
    let children: HashMap<i64, i64> = db.all::<SChild>().run().unwrap().iter().map(|c| (c.id, c.parent_id)).collect();
    let (mut lost, mut phantom, mut orphan, mut checked) = (0, 0, 0, 0);
    let mut known: HashSet<i64> = Default::default();
    for t in 0..4i64 {
        let mut per: HashMap<i64, Vec<(char, char)>> = HashMap::new();
        for (k, c, id) in read_log(&dir, t) { per.entry(id).or_default().push((k, c)); }
        for (id, ev) in &per {
            known.insert(*id);
            checked += 1;
            let pending = ev.last().filter(|e| e.0 == 'P').map(|e| e.1);
            let acked = |c: char| ev.iter().any(|e| e.0 == 'A' && e.1 == c);
            // parent: acked I => must exist (parents are never deleted)
            if acked('I') && !parents.contains(id) { lost += 1; eprintln!("FKSOAK LOST parent {id}"); }
            if !acked('I') && pending != Some('I') && parents.contains(id) { phantom += 1; eprintln!("FKSOAK PHANTOM parent {id}"); }
            // child
            let want_child_acked = acked('C') && !acked('D');
            let has = children.contains_key(id);
            if want_child_acked && !has && pending != Some('D') { lost += 1; eprintln!("FKSOAK LOST child {id}"); }
            if acked('D') && has { lost += 1; eprintln!("FKSOAK DELETE LOST child {id}"); }
            if !acked('C') && pending != Some('C') && has { phantom += 1; eprintln!("FKSOAK PHANTOM child {id}"); }
        }
    }
    for (c, p) in &children {
        if !parents.contains(p) { orphan += 1; eprintln!("FKSOAK ORPHAN child {c} parent {p}"); }
        if !known.contains(c) { phantom += 1; }
    }
    println!("FKSOAK verify parents={} children={} checked={} lost={} phantom={} orphan={}", parents.len(), children.len(), checked, lost, phantom, orphan);
    if lost + phantom + orphan > 0 { std::process::exit(1); }
}
