// Probe: split_internal ke child loop mein read_node(child) fail ho to tree/WAL ki halat kya hoti hai.
// Run: cargo test --release -p mitrdb-storage --test btree_split_internal_probe -- --ignored --nocapture
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
    internal_id: u32,
    corrupt_keys: Vec<Vec<u8>>,
}

fn build(dir: &Path, n: u64) -> Option<(Target, u64)> {
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
                        internal_id: id,
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
    found.map(|t| (t, n))
}

#[test]
#[ignore]
fn probe_split_internal_child_read_fails() {
    let base = std::env::var("MITRDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = base.join(format!("mitrdb_split_int_{}", std::process::id()));
    let mut chosen = None;
    for n in (40..400u64).step_by(7) {
        if let Some(t) = build(&dir, n) {
            chosen = Some(t);
            break;
        }
    }
    let Some((t, n)) = chosen else {
        println!("PROBE no suitable full internal node found for n in 40..400, nothing to test");
        return;
    };
    println!(
        "PROBE n={} root={} internal={} corrupt_page={} insert_key={}",
        n, t.root, t.internal_id, t.corrupt_page, t.insert_key
    );
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
    println!("PROBE insert -> {:?}", res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    if res.is_ok() {
        println!("PROBE no failure (corrupt page not read on this path)");
        return;
    }
    println!(
        "PROBE prev_lsn before={} after={} changed={} ; wal.next_lsn before={} after={}",
        prev_before, prev, prev != prev_before, next_before, wal.next_lsn()
    );
    let corrupt: std::collections::HashSet<Vec<u8>> = t.corrupt_keys.iter().cloned().collect();
    let mut lost = Vec::new();
    for i in 0..n {
        let kb = key(2 * i).to_vec();
        if corrupt.contains(&kb) {
            continue;
        }
        match tree.get(&kb) {
            Ok(Some(_)) => {}
            other => lost.push((2 * i, format!("{:?}", other.map(|v| v.is_some())))),
        }
    }
    println!("PROBE committed keys unreachable (excluding corrupt leaf): {} {:?}", lost.len(), &lost[..lost.len().min(10)]);
    println!("PROBE failed key visible = {:?}", tree.get(&key(t.insert_key)).map(|v| v.is_some()));
    let _ = std::fs::remove_dir_all(&dir);
}
