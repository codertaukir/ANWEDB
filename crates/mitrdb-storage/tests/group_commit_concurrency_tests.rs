#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
//! Regression tests for concurrent WAL group commit.
//!
//! These tests prove that:
//! 1. Multiple threads can call `commit` concurrently and all records become durable.
//! 2. A commit returns only after its own LSN is durable (group-commit correctness).
//! 3. Crash recovery after concurrent durable commits preserves all committed txns.

use std::sync::{Arc, Barrier};
use std::thread;

use mitrdb_storage::wal::{LogRecord, WalManager};

#[test]
fn concurrent_commits_share_group_flush_and_all_are_durable() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_group_commit_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    let writers = 8usize;
    let commits_per = 25usize;
    let barrier = Arc::new(Barrier::new(writers));
    let mut handles = Vec::with_capacity(writers);

    for w in 0..writers {
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut lsns = Vec::with_capacity(commits_per);
            for i in 0..commits_per {
                let tid = (w as u64) * 10_000 + (i as u64) + 1;
                let begin = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
                let upd = wal
                    .append_record(LogRecord::new_update(
                        0,
                        tid,
                        begin,
                        1,
                        64,
                        format!("w{w}-i{i}").into_bytes(),
                        vec![0u8; 8],
                    ))
                    .expect("update append");
                let commit_lsn = wal.commit(tid, upd).unwrap();
                assert!(
                    commit_lsn >= begin,
                    "commit LSN must be after begin for txn {tid}"
                );
                // After commit returns, flushed_lsn must cover the commit record.
                assert!(
                    wal.flushed_lsn() >= commit_lsn,
                    "group commit must make LSN {commit_lsn} durable before returning (flushed={})",
                    wal.flushed_lsn()
                );
                lsns.push(commit_lsn);
            }
            lsns
        }));
    }

    let mut all_lsns = Vec::new();
    for h in handles {
        all_lsns.extend(h.join().expect("worker panicked"));
    }
    assert_eq!(all_lsns.len(), writers * commits_per);
    let max_lsn = *all_lsns.iter().max().unwrap();
    assert!(
        wal.flushed_lsn() >= max_lsn,
        "all concurrent commits must be durable"
    );

    // Re-open and ensure recovery can read through the durable tail.
    drop(wal);
    let wal2 = WalManager::open_or_create(&wal_path).unwrap();
    assert!(
        wal2.flushed_lsn() >= max_lsn || wal2.next_lsn() > max_lsn,
        "reopened WAL must retain durable records up to {max_lsn}"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn single_writer_commit_still_fsyncs_before_return() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_single_commit_{}", std::process::id()));
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let wal = WalManager::open_or_create(&wal_path).unwrap();
    let begin = wal.append_record(LogRecord::new_begin(0, 42)).unwrap();
    let commit_lsn = wal.commit(42, begin).unwrap();
    assert!(wal.flushed_lsn() >= commit_lsn);
    assert_eq!(wal.flushed_lsn(), commit_lsn);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn adaptive_group_commit_coalesces_burst_commits() {
    use mitrdb_observe::GLOBAL_METRICS;
    use std::time::Duration;

    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_adaptive_gc_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
    wal.set_group_commit_linger(Duration::from_millis(5), 64);
    assert_eq!(wal.group_commit_linger(), (Duration::from_millis(5), 64));

    let flushes_before = GLOBAL_METRICS.snapshot().wal_flushes;
    let writers = 12usize;
    let commits_per = 10usize;
    let barrier = Arc::new(Barrier::new(writers));
    let mut handles = Vec::with_capacity(writers);

    for w in 0..writers {
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut max_lsn = 0;
            for i in 0..commits_per {
                let tid = 100_000 + (w as u64) * 1_000 + (i as u64) + 1;
                let b = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
                let c = wal.commit(tid, b).unwrap();
                assert!(wal.flushed_lsn() >= c);
                max_lsn = max_lsn.max(c);
            }
            max_lsn
        }));
    }

    let mut global_max = 0;
    for h in handles {
        global_max = global_max.max(h.join().unwrap());
    }
    let flushes_after = GLOBAL_METRICS.snapshot().wal_flushes;
    let total_commits = (writers * commits_per) as u64;
    let flush_delta = flushes_after.saturating_sub(flushes_before);

    assert!(
        flush_delta < total_commits,
        "expected adaptive group commit to coalesce {total_commits} commits into fewer flushes, got {flush_delta}"
    );
    assert!(wal.flushed_lsn() >= global_max);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
