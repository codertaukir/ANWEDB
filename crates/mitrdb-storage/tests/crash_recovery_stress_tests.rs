#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

use mitrdb_common::PAGE_HEADER_SIZE;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_TYPE_RAW;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

#[test]
fn test_crash_recovery_uncommitted_undo_and_committed_redo() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_crash_stress_1_{}", std::process::id()));
    let db_path = temp_dir.join("crash_test.db");
    let wal_path = temp_dir.join("crash_test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let offset = PAGE_HEADER_SIZE as u16;
    let initial_val = b"initial_payload_";
    let committed_val = b"committed_patch_";
    let uncommitted_val = b"dirty_crash_pat_";

    // --- PHASE 1: Active engine with committed & in-flight transactions ---
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 10));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let (p0, mut page0) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (p1, mut page1) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();

        page0.data[offset as usize..offset as usize + initial_val.len()]
            .copy_from_slice(initial_val);
        page1.data[offset as usize..offset as usize + initial_val.len()]
            .copy_from_slice(initial_val);

        disk.write_page(p0, &page0).unwrap();
        disk.write_page(p1, &page1).unwrap();
        disk.sync().unwrap();

        recovery.create_checkpoint().unwrap();

        // Txn 101: Committed update on Page 0
        let lsn_begin_1 = wal.append_record(LogRecord::new_begin(0, 101)).unwrap();
        let lsn_upd_1 = wal
            .append_record(LogRecord::new_update(
                0,
                101,
                lsn_begin_1,
                p0,
                offset,
                committed_val.to_vec(),
                initial_val.to_vec(),
            ))
            .unwrap();
        wal.commit(101, lsn_upd_1).unwrap();

        // Txn 102: Uncommitted update on Page 1 (Crash victim)
        let lsn_begin_2 = wal.append_record(LogRecord::new_begin(0, 102)).unwrap();
        let lsn_upd_2 = wal
            .append_record(LogRecord::new_update(
                0,
                102,
                lsn_begin_2,
                p1,
                offset,
                uncommitted_val.to_vec(),
                initial_val.to_vec(),
            ))
            .unwrap();

        // Simulate Steal: Dirty page 1 flushed to disk before crash
        page1.data[offset as usize..offset as usize + uncommitted_val.len()]
            .copy_from_slice(uncommitted_val);
        page1.set_page_lsn(lsn_upd_2);
        disk.write_page(p1, &page1).unwrap();
        disk.sync().unwrap();

        wal.flush_up_to(lsn_upd_2).unwrap();
    }

    // --- PHASE 2: Restart & Recovery Execution ---
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 10));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();

        assert!(report.redone_records >= 1);
        assert!(report.undone_records >= 1);
        assert!(report.committed_txns.contains(&101));

        // Page 0 must have committed value
        let page0 = bpm.fetch_page(0).unwrap();
        assert_eq!(
            &page0.data[offset as usize..offset as usize + committed_val.len()],
            committed_val
        );

        // Page 1 must have been rolled back from uncommitted to initial value
        let page1 = bpm.fetch_page(1).unwrap();
        assert_eq!(
            &page1.data[offset as usize..offset as usize + initial_val.len()],
            initial_val
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_crash_recovery_with_torn_tail_corruption() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_crash_stress_2_{}", std::process::id()));
    let db_path = temp_dir.join("torn_test.db");
    let wal_path = temp_dir.join("torn_test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let offset = PAGE_HEADER_SIZE as u16;
    let stable_val = b"stable_database_";

    // --- PHASE 1: Write committed transaction ---
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 10));
        let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let (p0, mut page0) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        page0.data[offset as usize..offset as usize + stable_val.len()]
            .copy_from_slice(b"init_before_upd_");
        disk.write_page(p0, &page0).unwrap();
        disk.sync().unwrap();

        let lsn_begin = wal.append_record(LogRecord::new_begin(0, 201)).unwrap();
        let lsn_upd = wal
            .append_record(LogRecord::new_update(
                0,
                201,
                lsn_begin,
                p0,
                offset,
                stable_val.to_vec(),
                b"init_before_upd_".to_vec(),
            ))
            .unwrap();
        wal.commit(201, lsn_upd).unwrap();
    }

    // --- PHASE 2: Inject Torn-Tail (Garbage bytes appended to WAL) ---
    {
        let mut wal_file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        wal_file
            .write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02])
            .unwrap();
        wal_file.sync_all().unwrap();
    }

    // --- PHASE 3: Restart & Recovery must truncate torn tail cleanly ---
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 10));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert!(report.redone_records >= 1);
        assert!(report.committed_txns.contains(&201));

        let page0 = bpm.fetch_page(0).unwrap();
        assert_eq!(
            &page0.data[offset as usize..offset as usize + stable_val.len()],
            stable_val
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
