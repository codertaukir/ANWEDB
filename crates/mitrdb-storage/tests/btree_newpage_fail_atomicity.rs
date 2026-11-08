// Regression: new_page failing anywhere in a split chain must leave tree + WAL untouched.
// Run: cargo test -p mitrdb-storage --features fault-injection --test btree_newpage_fail_atomicity
use std::sync::Arc;

use mitrdb_storage::btree::{BPlusTree, KeyRange};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn verify(tree: &BPlusTree, committed: &[u64], ctx: &str) {
    for &c in committed {
        assert!(
            matches!(tree.get(&key(c)), Ok(Some(_))),
            "{ctx}: committed key {c} lost"
        );
    }
    let all = tree.range_scan_bounds(&KeyRange::full()).unwrap();
    assert_eq!(all.len(), committed.len(), "{ctx}: scan count");
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0), "{ctx}: scan order");
}

fn setup(tag: &str, maxk: u16) -> (std::path::PathBuf, Arc<DiskManager>, Arc<WalManager>, BPlusTree, u64) {
    let dir = std::env::temp_dir().join(format!("mitrdb_np_atomic_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = BPlusTree::create_logged(bpm, Some(wal.clone()), maxk, 1, &mut prev).unwrap();
    (dir, disk, wal, tree, prev)
}

#[test]
fn new_page_failure_in_split_leaves_tree_and_wal_untouched() {
    let mut failures = 0usize;
    for k in 0..=4i64 {
        for pre in [8u64, 16, 32, 64, 128, 200, 400] {
            let (dir, disk, wal, tree, mut prev) = setup(&format!("a{k}_{pre}"), 4);
            let mut committed: Vec<u64> = Vec::new();
            for i in 0..pre {
                tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8]).unwrap();
                committed.push(2 * i);
            }
            let odd = |i: u64| 2 * ((i * 7919) % pre) + 1;
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
                    Err(_) => {
                        failures += 1;
                        let ctx = format!("k={k} pre={pre} ins#{i}");
                        assert_eq!(prev, prev_before, "{ctx}: prev_lsn moved");
                        assert_eq!(wal.next_lsn(), next_before, "{ctx}: WAL records appended");
                        assert_eq!(tree.root_page_id(), root_before, "{ctx}: root moved");
                        assert!(
                            matches!(tree.get(&key(kk)), Ok(None)),
                            "{ctx}: failed insert is visible"
                        );
                        verify(&tree, &committed, &ctx);
                        tree.insert_logged(1, &mut prev, &key(kk), &[7u8; 8]).unwrap();
                        committed.push(kk);
                        verify(&tree, &committed, &format!("{ctx} after retry"));
                    }
                }
            }
            verify(&tree, &committed, &format!("k={k} pre={pre} final"));
            drop(tree);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    assert!(failures > 20, "injection barely fired: {failures}");
}

// Plan/real-split exactness: debug_assert in insert_or_replace_logged fires if the planned
// page count differs from what the split used. Mixed value sizes force byte-based splits.
#[test]
fn split_plan_is_exact_without_injection() {
    for maxk in [4u16, 16] {
        let (dir, _disk, _wal, tree, mut prev) = setup(&format!("b{maxk}"), maxk);
        let n = 3000u64;
        let mut committed = Vec::new();
        for i in 0..n {
            let kk = (i * 7919) % n;
            let vlen = 8 + ((i as usize * 37) % 1400);
            tree.insert_logged(1, &mut prev, &key(kk), &vec![7u8; vlen]).unwrap();
            committed.push(kk);
        }
        committed.sort_unstable();
        verify(&tree, &committed, &format!("maxk={maxk}"));
        drop(tree);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
