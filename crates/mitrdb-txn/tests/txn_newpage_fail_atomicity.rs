#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
// Regression (txn level): new_page failing mid-split inside TxnManager::put must not corrupt
// the tree, so rollback_to_mark / abort / commit all leave exactly the expected keys.
// Engine undo is key-level only; without btree-level atomicity keys vanish silently.
// Run: cargo test -p mitrdb-txn --features fault-injection --test txn_newpage_fail_atomicity
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use mitrdb_storage::btree::{BPlusTree, KeyRange};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

struct Env {
    dir: PathBuf,
    disk: Arc<DiskManager>,
    tree: Arc<BPlusTree>,
    mgr: TxnManager,
}

fn setup(tag: &str) -> Env {
    let dir = std::env::temp_dir().join(format!("mitrdb_tna_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    bpm.flush_all().unwrap();
    let mgr = TxnManager::with_storage(wal, bpm);
    Env {
        dir,
        disk,
        tree,
        mgr,
    }
}

// Raw tree view (not MVCC) so physical loss is visible.
fn check(tree: &BPlusTree, expected: &HashSet<u64>, ctx: &str) {
    for &c in expected {
        assert!(
            matches!(tree.get(&key(c)), Ok(Some(_))),
            "{ctx}: expected key {c} lost"
        );
    }
    let all = tree.range_scan_bounds(&KeyRange::full()).unwrap();
    assert_eq!(
        all.len(),
        expected.len(),
        "{ctx}: scan count (phantom or lost)"
    );
    for (k, _) in &all {
        let mut a = [0u8; 8];
        assert_eq!(k.len(), 8, "{ctx}: bad key len");
        a.copy_from_slice(k);
        assert!(
            expected.contains(&u64::from_be_bytes(a)),
            "{ctx}: phantom key"
        );
    }
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0), "{ctx}: scan order");
}

#[test]
fn newpage_failure_in_put_then_rollback_or_abort_keeps_tree_consistent() {
    let mut fired = 0usize;
    for variant in ["stmt_rollback", "abort"] {
        for k in 0..=4i64 {
            for pre in [16u64, 64, 200] {
                let env = setup(&format!("{variant}_{k}_{pre}"));
                let p = env.mgr.begin().unwrap();
                let mut base: HashSet<u64> = HashSet::new();
                for i in 0..pre {
                    env.mgr.put(p, &env.tree, &key(2 * i), &[7u8; 8]).unwrap();
                    base.insert(2 * i);
                }
                env.mgr.commit(p).unwrap();

                let t = env.mgr.begin().unwrap();
                let mut expected = base.clone();
                for i in 0..pre {
                    let kk = 2 * ((i * 7919) % pre) + 1;
                    let mark = env.mgr.write_mark(t).unwrap();
                    env.disk.set_fail_alloc_after(k);
                    let res = env.mgr.put(t, &env.tree, &key(kk), &[7u8; 8]);
                    env.disk.set_fail_alloc_after(-1);
                    match res {
                        Ok(()) => {
                            expected.insert(kk);
                        }
                        Err(_) => {
                            fired += 1;
                            let ctx = format!("{variant} k={k} pre={pre} put#{i}");
                            if variant == "stmt_rollback" {
                                env.mgr
                                    .rollback_to_mark(t, mark)
                                    .unwrap_or_else(|e| panic!("{ctx}: rollback_to_mark: {e}"));
                                check(&env.tree, &expected, &ctx);
                            } else {
                                break;
                            }
                        }
                    }
                }
                let ctx = format!("{variant} k={k} pre={pre} end");
                if variant == "abort" {
                    env.mgr
                        .abort(t)
                        .unwrap_or_else(|e| panic!("{ctx}: abort: {e}"));
                    check(&env.tree, &base, &ctx);
                } else {
                    env.mgr
                        .commit(t)
                        .unwrap_or_else(|e| panic!("{ctx}: commit: {e}"));
                    check(&env.tree, &expected, &ctx);
                }
                let dir = env.dir.clone();
                drop(env);
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }
    assert!(fired > 20, "injection barely fired: {fired}");
}
