#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
#![cfg(feature = "fault-injection")]
// Probe v7 (ignored): ONE injected cache-miss failure on a DEEP tree (depth 3 at max_keys=64).
// Scenarios: root split (2->3) insert, and deletes after d leftmost deletes (merge/borrow/rebalance_internal).
// Run: MITRDB_DIR=$HOME/x cargo test --release -p mitrdb-storage --features fault-injection --test btree_deep_fail_probe -- --ignored --nocapture
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

fn depth(tree: &BPlusTree) -> usize {
    let mut node = tree.read_node(tree.root_page_id()).unwrap();
    let mut d = 1usize;
    while !node.is_leaf {
        let c = node.children[0];
        node = tree.read_node(c).unwrap();
        d += 1;
    }
    d
}

fn sig(bt: &str) -> String {
    let mut bpm = String::new();
    let mut fns: Vec<String> = Vec::new();
    for line in bt.lines() {
        let l = line.trim();
        let Some((num, rest)) = l.split_once(": ") else {
            continue;
        };
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

macro_rules! mk {
    ($dir:expr, $maxk:expr, $pool:expr) => {{
        let _ = std::fs::remove_dir_all(&$dir);
        std::fs::create_dir_all(&$dir).unwrap();
        let disk = Arc::new(DiskManager::open_or_create($dir.join("t.db").as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), $pool));
        let wal = Arc::new(WalManager::open_or_create($dir.join("t.wal").as_path()).unwrap());
        let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        let tree =
            BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), $maxk, 1, &mut prev).unwrap();
        (disk, bpm, wal, tree, prev)
    }};
}

#[test]
#[ignore]
fn btree_deep_fail_probe() {
    let maxk: u16 = std::env::var("MAXK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let pool: usize = std::env::var("POOL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let nmax: i64 = std::env::var("NMAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let total: u64 = 8000;
    let base = std::env::var("MITRDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    println!("CFG7 maxk={maxk} pool={pool} nmax={nmax} total={total}");

    // calibrate: first seq key whose insert changes the root a SECOND time (depth 2 -> 3)
    let c: u64 = {
        let dir = base.join(format!("deep7_cal_{}", std::process::id()));
        let (_disk, _bpm, _wal, tree, mut prev) = mk!(dir, maxk, 4096);
        let mut changes = 0;
        let mut found = 0u64;
        for i in 0..total {
            let before = tree.root_page_id();
            tree.insert_logged(1, &mut prev, &key(i), &[7u8; 8])
                .unwrap();
            if tree.root_page_id() != before {
                changes += 1;
                if changes == 2 {
                    found = i;
                    break;
                }
            }
        }
        let d = depth(&tree);
        drop(tree);
        let _ = std::fs::remove_dir_all(&dir);
        println!("CAL7 second root change at key index {found}, depth_after={d}");
        assert!(found > 0, "calibration: no second root change");
        found
    };

    // (label, op, keys inserted 0..pre, leftmost deletes d)
    let mut scens: Vec<(String, &str, u64, u64)> =
        vec![(format!("root_split_c{c}"), "insert", c, 0)];
    for d in [0u64, 3000, 6000, 7500] {
        scens.push((format!("delete_after_{d}"), "delete", total, d));
    }

    for (label, op, pre, dl) in scens {
        let mut table: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        let (mut total_err, mut total_bad, mut total_ok) = (0usize, 0usize, 0usize);
        for n in 0..=nmax {
            let dir = base.join(format!("deep7_{}_{}_{}", std::process::id(), label, n));
            let (_disk, bpm, _wal, tree, mut prev) = mk!(dir, maxk, pool);
            let mut expected: HashSet<u64> = HashSet::new();
            for i in 0..pre {
                tree.insert_logged(1, &mut prev, &key(i), &[7u8; 8])
                    .unwrap();
                expected.insert(i);
            }
            for i in 0..dl {
                assert!(tree.delete_logged(1, &mut prev, &key(i)).unwrap());
                expected.remove(&i);
            }
            let kk = if op == "insert" { pre } else { dl };
            if n == 0 {
                println!(
                    "PRE7 scen={label} depth={} pages={}",
                    depth(&tree),
                    tree.collect_all_pages().unwrap().len()
                );
            }
            let prev_before = prev;
            bpm.set_fail_miss_after(n);
            let res = catch_unwind(AssertUnwindSafe(|| {
                if op == "insert" {
                    tree.insert_logged(1, &mut prev, &key(kk), &[7u8; 8])
                        .map(|_| true)
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
                    for &k in &expected {
                        if k == kk {
                            continue;
                        }
                        match tree.get(&key(k)) {
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
                        println!(
                            "BAD7 scen={label} n={n} lost={coll} get_err={geterr} scan_err={scan_err} target_changed={tgt} prev_moved={}",
                            prev != prev_before
                        );
                    }
                    let s = trace
                        .as_deref()
                        .map(sig)
                        .unwrap_or_else(|| "NO_TRACE".to_string());
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
        println!("SUMMARY7 scen={label} op={op} ok={total_ok} err={total_err} bad={total_bad}");
        let mut rows: Vec<_> = table.into_iter().collect();
        rows.sort_by(|a, b| b.1.1.cmp(&a.1.1).then(b.1.0.cmp(&a.1.0)));
        for (s, (t, b)) in rows {
            println!("SIG7 scen={label} total={t} bad={b} {s}");
        }
    }
}
