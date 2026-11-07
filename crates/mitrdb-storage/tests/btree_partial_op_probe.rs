// Probe: split_leaf ke beech read_node(old_next) fail ho to tree/WAL ki halat kya hoti hai.
// Run: cargo test --release -p mitrdb-storage --test btree_partial_op_probe -- --ignored --nocapture
use std::io::{Seek, SeekFrom, Write};
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
fn probe_split_leaf_old_next_read_fails() {
    let base = std::env::var("MITRDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = base.join(format!("mitrdb_partial_op_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let max_keys = 4u16;

    let root_id;
    let l2_id;
    let psz;
    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        let tree =
            BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), max_keys, 1, &mut prev)
                .unwrap();
        for i in 0..20u64 {
            tree.insert_logged(1, &mut prev, &key(2 * i), &[7u8; 8]).unwrap();
        }
        let c = wal.append_record(LogRecord::new_commit(0, 1, prev)).unwrap();
        wal.flush_up_to(c).unwrap();
        root_id = tree.root_page_id();
        let root = tree.read_node(root_id).unwrap();
        println!("PROBE setup root={} children={:?} sep_keys={}", root_id, root.children, root.keys.len());
        assert!(root.children.len() >= 3, "expected >=3 leaves under root");
        l2_id = root.children[2];
        let pg = bpm.fetch_page(l2_id).unwrap();
        psz = pg.data.len();
        bpm.unpin_page(l2_id, false).unwrap();
        bpm.flush_all().unwrap();
    }

    {
        let mut f = std::fs::OpenOptions::new().write(true).open(&db_path).unwrap();
        f.seek(SeekFrom::Start(l2_id as u64 * psz as u64)).unwrap();
        f.write_all(&[0xFF; 4]).unwrap();
        f.sync_all().unwrap();
        println!("PROBE corrupted page {} (magic bytes), page size {}", l2_id, psz);
    }

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, max_keys).unwrap();
    let mut prev = wal.append_record(LogRecord::new_begin(0, 2)).unwrap();
    let prev_before = prev;
    let next_before = wal.next_lsn();

    let res = tree.insert_logged(2, &mut prev, &key(9), &[9u8; 8]);
    println!("PROBE insert key 9 -> {:?}", res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    if res.is_ok() {
        println!("PROBE no failure injected (page read ok), corruption didn't take");
        return;
    }
    println!(
        "PROBE prev_lsn before={} after={} changed={} ; wal.next_lsn before={} after={}",
        prev_before, prev, prev != prev_before, next_before, wal.next_lsn()
    );
    for k in [8u64, 10, 12, 14] {
        println!("PROBE get({}) -> {:?}", k, tree.get(&key(k)).map(|v| v.is_some()));
    }
    println!("PROBE get(9) -> {:?}", tree.get(&key(9)).map(|v| v.is_some()));
    let root = tree.read_node(tree.root_page_id()).unwrap();
    println!("PROBE root children after = {:?}", root.children);
    let l1 = tree.read_node(root.children[1]).unwrap();
    println!("PROBE L1 keys={} next_leaf={:?}", l1.keys.len(), l1.next_leaf_page_id);
}
