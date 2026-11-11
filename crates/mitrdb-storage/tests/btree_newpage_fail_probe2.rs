#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// Probe v2: permuted (mid-tree) inserts so splits move committed keys to the right leaf.
// Run: cargo test --release -p mitrdb-storage --test btree_newpage_fail_probe2 -- --ignored --nocapture
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

#[test]
#[ignore]
fn probe2_new_page_fail_midtree() {
    for k in [1i64, 2, 3] {
        for pre in [8u64, 16, 32, 64, 128, 200, 400] {
            let dir = std::path::PathBuf::from("/dev/shm").join(format!(
                "mitrdb_np2_{}_{}_{}",
                std::process::id(),
                k,
                pre
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
            let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
            let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
            let tree =
                BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, 1, &mut prev).unwrap();
            let mut committed: Vec<u64> = Vec::new();
            for i in 0..pre {
                tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8])
                    .unwrap();
                committed.push(2 * i);
            }
            let odd = |i: u64| 2 * ((i * 7919) % pre) + 1;
            let mut fail_at: Option<u64> = None;
            let mut info = String::new();
            for i in 0..pre {
                let kk = odd(i);
                disk.set_fail_alloc_after(k);
                let prev_before = prev;
                let next_before = wal.next_lsn();
                let root_before = tree.root_page_id();
                let res = tree.insert_logged(1, &mut prev, &key(kk), &[7u8; 8]);
                disk.set_fail_alloc_after(-1);
                match res {
                    Ok(()) => committed.push(kk),
                    Err(e) => {
                        let lost: Vec<u64> = committed
                            .iter()
                            .copied()
                            .filter(|&c| !matches!(tree.get(&key(c)), Ok(Some(_))))
                            .collect();
                        let fk_visible = matches!(tree.get(&key(kk)), Ok(Some(_)));
                        info = format!(
                            "ins#{i} err={e} wal_recs={} prev_changed={} root {}->{} committed={} LOST={} {:?} failed_key_get_visible={}",
                            wal.next_lsn() - next_before,
                            prev != prev_before,
                            root_before,
                            tree.root_page_id(),
                            committed.len(),
                            lost.len(),
                            &lost[..lost.len().min(6)],
                            fk_visible
                        );
                        fail_at = Some(i);
                        break;
                    }
                }
            }
            match fail_at {
                None => println!("PROBE k={k} pre={pre} no failure"),
                Some(fi) => {
                    let mut errs = 0;
                    for i in fi + 1..(fi + 51).min(pre) {
                        let kk = odd(i);
                        match tree.insert_logged(1, &mut prev, &key(kk), &[7u8; 8]) {
                            Ok(()) => committed.push(kk),
                            Err(_) => errs += 1,
                        }
                    }
                    let lost2 = committed
                        .iter()
                        .filter(|&&c| !matches!(tree.get(&key(c)), Ok(Some(_))))
                        .count();
                    println!("PROBE k={k} pre={pre} {info} | after50: errs={errs} LOST={lost2}");
                }
            }
            drop(tree);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
