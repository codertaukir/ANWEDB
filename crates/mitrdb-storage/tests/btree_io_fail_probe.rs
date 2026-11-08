// Probe v4 (btree level): ONE injected page-op failure per fresh tree; failures grouped by the
// call signature (bpm fn @ btree call chain) captured from a backtrace at the injection point.
// Run: cargo test -p mitrdb-storage --features fault-injection --test btree_io_fail_probe -- --ignored --nocapture
use std::collections::{BTreeMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use mitrdb_storage::btree::{BPlusTree, KeyRange};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn sig(bt: &str) -> String {
    let mut bpm = String::new();
    let mut fns: Vec<String> = Vec::new();
    for line in bt.lines() {
        let l = line.trim();
        let Some((num, rest)) = l.split_once(": ") else { continue };
        if num.parse::<usize>().is_err() {
            continue;
        }
        let mut name = rest.to_string();
        if let Some(i) = name.rfind("::h") {
            let h = &name[i + 3..];
            if h.len() == 16 && h.chars().all(|c| c.is_ascii_hexdigit()) {
                name.truncate(i);
            }
        }
        if let Some(p) = name.find("BufferPoolManager>::") {
            let f = &name[p + 20..];
            if bpm.is_empty() && !f.contains("inject_page_op_fail") && !f.contains("{{closure}}") {
                bpm = f.to_string();
            }
            continue;
        }
        if let Some(p) = name.find("BPlusTree>::") {
            let f = &name[p + 12..];
            if f.contains("{{closure}}") {
                continue;
            }
            if fns.len() < 4 && fns.last().map(|x| x.as_str()) != Some(f) {
                fns.push(f.to_string());
            }
        }
    }
    format!("{}@{}", bpm, fns.join("<"))
}

#[test]
#[ignore]
fn btree_io_fail_probe() {
    let maxk: u16 = std::env::var("MAXK").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let pres: Vec<u64> = std::env::var("PRES").ok()
        .map(|v| v.split(',').filter_map(|x| x.parse().ok()).collect())
        .unwrap_or_else(|| vec![16, 64]);
    println!("CFG maxk={maxk} pres={pres:?}");
    for op in ["insert", "delete"] {
        let mut table: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        let (mut total_err, mut total_bad, mut total_ok) = (0usize, 0usize, 0usize);
        for n in 0..=24i64 {
            for pre in pres.iter().copied() {
                for trial in 0..16u64 {
                    let dir = std::env::temp_dir().join(format!(
                        "mitrdb_iofp4_{}_{}_{}_{}_{}",
                        std::process::id(), op, n, pre, trial
                    ));
                    let _ = std::fs::remove_dir_all(&dir);
                    std::fs::create_dir_all(&dir).unwrap();
                    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
                    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
                    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
                    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
                    let tree = BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), maxk, 1, &mut prev).unwrap();
                    let mut expected: HashSet<u64> = HashSet::new();
                    for i in 0..pre {
                        tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8]).unwrap();
                        expected.insert(2 * i);
                    }
                    if op == "delete" {
                        for i in 0..pre {
                            tree.insert_logged(1, &mut prev, &key(2 * i + 1), &[7u8; 8]).unwrap();
                            expected.insert(2 * i + 1);
                        }
                    }
                    let kk = 2 * ((trial * 7919 + 3) % pre) + 1;
                    let prev_before = prev;
                    bpm.set_fail_miss_after(n);
                    let res = catch_unwind(AssertUnwindSafe(|| {
                        if op == "insert" {
                            tree.insert_logged(1, &mut prev, &key(kk), &[7u8; 8]).map(|_| true)
                        } else {
                            tree.delete_logged(1, &mut prev, &key(kk))
                        }
                    }));
                    bpm.set_fail_miss_after(-1);
                    let trace = bpm.take_injection_trace();
                    match res {
                        Ok(Ok(_)) => total_ok += 1,
                        Err(_) => {
                            total_err += 1;
                            total_bad += 1;
                            let e = table.entry("PANIC".to_string()).or_insert((0, 0));
                            e.0 += 1;
                            e.1 += 1;
                        }
                        Ok(Err(_)) => {
                            total_err += 1;
                            let (mut coll, mut geterr) = (0i64, 0i64);
                            for &c in &expected {
                                if c == kk {
                                    continue;
                                }
                                match tree.get(&key(c)) {
                                    Ok(Some(_)) => {}
                                    Ok(None) => coll += 1,
                                    Err(_) => geterr += 1,
                                }
                            }
                            let scan_err = tree.range_scan_bounds(&KeyRange::full()).is_err();
                            let t = tree.get(&key(kk));
                            let tgt = (op == "insert" && matches!(t, Ok(Some(_))))
                                || (op == "delete" && matches!(t, Ok(None)));
                            let bad = coll > 0 || geterr > 0 || scan_err || tgt || prev != prev_before;
                            if bad {
                                total_bad += 1;
                            }
                            let s = trace.as_deref().map(sig).unwrap_or_else(|| "NO_TRACE".to_string());
                            let e = table.entry(s).or_insert((0, 0));
                            e.0 += 1;
                            if bad {
                                e.1 += 1;
                            }
                        }
                    }
                    drop(tree);
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }
        println!("SUMMARY4 op={op} ok={total_ok} err={total_err} bad={total_bad}");
        let mut rows: Vec<_> = table.into_iter().collect();
        rows.sort_by(|a, b| b.1.1.cmp(&a.1.1).then(b.1.0.cmp(&a.1.0)));
        for (s, (t, b)) in rows {
            println!("SIG4 op={op} total={t} bad={b} {s}");
        }
    }
}
