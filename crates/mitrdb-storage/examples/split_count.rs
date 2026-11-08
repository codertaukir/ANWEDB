// Deterministic alloc counter for the split path. args: dir seq|rand n
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(n as u64, Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static G: Counting = Counting;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let mode = a[2].as_str();
    let n: u64 = a[3].parse().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let disk = Arc::new(DiskManager::open_or_create(dir.join("t.db").as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8192));
    let wal = Arc::new(WalManager::open_or_create(dir.join("t.wal").as_path()).unwrap());
    let mut prev = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = BPlusTree::create_logged(bpm, Some(wal), 64, 1, &mut prev).unwrap();
    let keys: Vec<u64> = (0..n).map(|i| if mode == "rand" { (i * 7919) % n } else { i }).collect();
    let (a0, b0) = (ALLOCS.load(Relaxed), BYTES.load(Relaxed));
    for k in &keys {
        tree.insert_logged(1, &mut prev, &k.to_be_bytes(), &[7u8; 8]).unwrap();
    }
    let (a1, b1) = (ALLOCS.load(Relaxed), BYTES.load(Relaxed));
    println!(
        "ALLOCCOUNT mode={mode} allocs={} bytes={} pages={} root={}",
        a1 - a0, b1 - b0, disk.num_pages(), tree.root_page_id()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
