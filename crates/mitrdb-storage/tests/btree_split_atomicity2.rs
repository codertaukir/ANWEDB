// Regression: a failing read in the split chain (split_internal child) must not touch WAL or tree.
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

const MAXK: u16 = 4;

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

struct Target {
    root: u32,
    corrupt_page: u32,
    insert_key: u64,
    corrupt_keys: Vec<Vec<u8>>,
}

fn build(dir: &Path, n: u64) -> Option<Target> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), MAXK, 1, &mut prev).unwrap();
    for i in 0..n {
        tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8]).unwrap();
    }
    let c = wal.append_record(LogRecord::new_commit(0, 1, prev)).unwrap();
    wal.flush_up_to(c).unwrap();
    let root = tree.root_page_id();
    let mut stack = vec![root];
    let mut found = None;
    while let Some(id) = stack.pop() {
        let node = tree.read_node(id).unwrap();
        if node.is_leaf {
            continue;
        }
        let first_child = tree.read_node(node.children[0]).unwrap();
        if first_child.is_leaf && node.keys.len() == MAXK as usize && node.children.len() >= 5 {
            for ci in 0..=2usize {
                let leaf = tree.read_node(node.children[ci]).unwrap();
                if leaf.keys.len() == MAXK as usize {
                    let last = *node.children.last().unwrap();
                    let lastn = tree.read_node(last).unwrap();
                    let k = u64::from_be_bytes(leaf.keys[0].as_slice().try_into().unwrap());
                    found = Some(Target {
                        root,
                        corrupt_page: last,
                        insert_key: k + 1,
                        corrupt_keys: lastn.keys.clone(),
                    });
                    break;
                }
            }
        }
        if found.is_some() {
            break;
        }
        for &ch in &node.children {
            stack.push(ch);
        }
    }
    bpm.flush_all().unwrap();
    found
}

#[test]
fn split_chain_read_failure_leaves_tree_and_wal_untouched() {
    let dir = std::env::temp_dir().join(format!("mitrdb_split_atomic2_{}", std::process::id()));
    let mut chosen = None;
    for n in (40..400u64).step_by(7) {
        if let Some(t) = build(&dir, n) {
            chosen = Some((t, n));
            break;
        }
    }
    let (t, n) = chosen.expect("no suitable full internal node found");
    {
        let mut f = std::fs::OpenOptions::new().write(true).open(dir.join("t.db")).unwrap();
        f.seek(SeekFrom::Start(t.corrupt_page as u64 * 4096)).unwrap();
        f.write_all(&[0xFF; 4]).unwrap();
        f.sync_all().unwrap();
    }
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), t.root, MAXK).unwrap();
    let mut prev = wal.append_record(LogRecord::new_begin(0, 2)).unwrap();
    let prev_before = prev;
    let next_before = wal.next_lsn();

    let res = tree.insert_logged(2, &mut prev, &key(t.insert_key), &[9u8; 8]);
    assert!(res.is_err(), "insert should fail: corrupt child is in the split chain");
    assert_eq!(prev, prev_before, "failed split must not advance prev_lsn");
    assert_eq!(wal.next_lsn(), next_before, "failed split must not append WAL records");
    let corrupt: std::collections::HashSet<Vec<u8>> = t.corrupt_keys.iter().cloned().collect();
    for i in 0..n {
        let kb = key(2 * i).to_vec();
        if corrupt.contains(&kb) {
            continue;
        }
        assert!(
            matches!(tree.get(&kb), Ok(Some(_))),
            "committed key {} lost after failed split",
            2 * i
        );
    }
    assert!(
        matches!(tree.get(&key(t.insert_key)), Ok(None)),
        "failed insert must not be visible"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
