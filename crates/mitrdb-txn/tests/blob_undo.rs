#![allow(clippy::collapsible_if)]
// A4: streaming BLOB pages are undone by rollback, abort and statement rollback, even when
// the writer is dropped mid-stream, and a transaction that logs other records while a blob
// write is open is rejected.
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use mitrdb_common::{PAGE_HEADER_SIZE, PageId};
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_storage::{BlobReader, OVERFLOW_CHUNK_CAPACITY, OverflowDescriptorV2, PAGE_FLAG_FREE};
use mitrdb_txn::TxnManager;

struct Env {
    dir: PathBuf,
    bpm: Arc<BufferPoolManager>,
    tree: Arc<BPlusTree>,
    mgr: TxnManager,
}

fn setup(tag: &str) -> Env {
    let dir = std::env::temp_dir().join(format!("mitrdb_blobundo_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk, 256));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    // creating a tree also reserves page 0 (0 = end of chain in overflow pages)
    let tree = Arc::new(BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap());
    bpm.flush_all().unwrap();
    let mgr = TxnManager::with_storage(wal, bpm.clone());
    Env { dir, bpm, tree, mgr }
}

fn pattern(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn is_free(bpm: &BufferPoolManager, pid: PageId) -> bool {
    // freed pages go to the reuse list: fetch_page fails with InvalidPageId.
    // That IS free — not a leak. Live pages must fetch + have FREE flag.
    match bpm.fetch_page(pid) {
        Err(_) => true,
        Ok(p) => {
            let free = p.flags() & PAGE_FLAG_FREE != 0;
            let _ = bpm.unpin_page(pid, false);
            free
        }
    }
}

/// Follow `next` pointers from the head (page body: kind, 0, len u16, next u32, data).
fn chain_pages(bpm: &BufferPoolManager, head: u64) -> Vec<PageId> {
    let mut out = Vec::new();
    let mut pid = head as PageId;
    while pid != 0 {
        out.push(pid);
        let p = bpm.fetch_page(pid).unwrap();
        bpm.unpin_page(pid, false).unwrap();
        let o = PAGE_HEADER_SIZE + 4;
        pid = u32::from_le_bytes(p.data[o..o + 4].try_into().unwrap());
        assert!(out.len() < 1_000_000, "chain loop");
    }
    out
}

fn write_blob(env: &Env, txn: u64, payload: &[u8]) -> OverflowDescriptorV2 {
    let mut w = env.mgr.begin_blob_write(txn, u64::MAX).unwrap();
    for c in payload.chunks(64 * 1024) {
        w.write_all(c).unwrap();
    }
    env.mgr.finish_blob_write(txn, w).unwrap()
}

#[test]
fn commit_keeps_blob_and_it_reads_back() {
    let env = setup("commit");
    let payload = pattern(300_000, 1);
    let txn = env.mgr.begin().unwrap();
    let desc = write_blob(&env, txn, &payload);
    env.mgr.commit(txn).unwrap();
    let pages = chain_pages(&env.bpm, desc.head_page_id);
    assert_eq!(pages.len(), payload.len().div_ceil(OVERFLOW_CHUNK_CAPACITY));
    assert!(pages.iter().all(|&p| !is_free(&env.bpm, p)));
    let mut r = BlobReader::open(env.bpm.clone(), desc).unwrap();
    r.verify().unwrap();
    assert!(r.read_range(100_000, 5_000).unwrap() == payload[100_000..105_000]);
    let _ = std::fs::remove_dir_all(&env.dir);
}

#[test]
fn abort_after_finish_frees_the_whole_chain() {
    let env = setup("abort");
    let payload = pattern(250_000, 2);
    let txn = env.mgr.begin().unwrap();
    let desc = write_blob(&env, txn, &payload);
    let pages = chain_pages(&env.bpm, desc.head_page_id);
    assert!(pages.len() > 10);
    assert!(pages.iter().all(|&p| !is_free(&env.bpm, p)));
    env.mgr.abort(txn).unwrap();
    assert!(pages.iter().all(|&p| is_free(&env.bpm, p)), "leaked pages");
    let _ = std::fs::remove_dir_all(&env.dir);
}

#[test]
fn abort_mid_stream_frees_partial_chain_even_if_writer_is_dropped() {
    let env = setup("mid");
    let txn = env.mgr.begin().unwrap();
    let mut w = env.mgr.begin_blob_write(txn, u64::MAX).unwrap();
    for c in pattern(200_000, 3).chunks(64 * 1024) {
        w.write_all(c).unwrap();
    }
    let pages = w.pages().to_vec();
    assert!(pages.len() > 10);
    drop(w); // no finish
    env.mgr.abort(txn).unwrap();
    assert!(pages.iter().all(|&p| is_free(&env.bpm, p)), "leaked pages");
    let _ = std::fs::remove_dir_all(&env.dir);
}

#[test]
fn statement_rollback_frees_only_blob_written_after_the_mark() {
    let env = setup("stmt");
    let a = pattern(100_000, 4);
    let b = pattern(120_000, 5);
    let txn = env.mgr.begin().unwrap();
    let da = write_blob(&env, txn, &a);
    let mark = env.mgr.write_mark(txn).unwrap();
    let db = write_blob(&env, txn, &b);
    let pa = chain_pages(&env.bpm, da.head_page_id);
    let pb = chain_pages(&env.bpm, db.head_page_id);
    env.mgr.rollback_to_mark(txn, mark).unwrap();
    assert!(pb.iter().all(|&p| is_free(&env.bpm, p)), "B not freed");
    assert!(pa.iter().all(|&p| !is_free(&env.bpm, p)), "A freed too");
    env.mgr.commit(txn).unwrap();
    let mut r = BlobReader::open(env.bpm.clone(), da).unwrap();
    r.verify().unwrap();
    let _ = std::fs::remove_dir_all(&env.dir);
}

#[test]
fn interleaved_txn_activity_is_rejected_and_pages_are_still_freed() {
    let env = setup("interleave");
    let txn = env.mgr.begin().unwrap();
    let mut w = env.mgr.begin_blob_write(txn, u64::MAX).unwrap();
    // exact multiple of the page capacity: every page is flushed before finish
    w.write_all(&pattern(8 * OVERFLOW_CHUNK_CAPACITY, 6)).unwrap();
    let pages = w.pages().to_vec();
    assert_eq!(pages.len(), 8);
    env.mgr.put(txn, &env.tree, b"k1", b"v1").unwrap(); // logs another record
    assert!(env.mgr.finish_blob_write(txn, w).is_err());
    env.mgr.abort(txn).unwrap();
    assert!(pages.iter().all(|&p| is_free(&env.bpm, p)), "leaked pages");
    let _ = std::fs::remove_dir_all(&env.dir);
}

#[test]
fn begin_blob_write_needs_an_active_transaction() {
    let env = setup("inactive");
    let txn = env.mgr.begin().unwrap();
    env.mgr.commit(txn).unwrap();
    assert!(env.mgr.begin_blob_write(txn, u64::MAX).is_err());
    assert!(env.mgr.begin_blob_write(999_999, u64::MAX).is_err());
    let _ = std::fs::remove_dir_all(&env.dir);
}
