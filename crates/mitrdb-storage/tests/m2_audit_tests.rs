#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mitrdb_common::{Error, TxnId};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::{PAGE_TYPE_RAW, PAGE_TYPE_SLOTTED};
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::{LogRecord, LogRecordType, WalManager};

#[test]
fn test_crash_before_commit_durability() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_before_commit_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

        let lsn1 = wal.append_record(LogRecord::new_begin(0, 501)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                501,
                lsn1,
                pid,
                64,
                b"uncommitted payload".to_vec(),
                b"clean base state---".to_vec(),
            ))
            .unwrap();
        wal.flush_up_to(lsn2).unwrap();

        bpm.update_page_data(pid, |p| {
            p.data[64..64 + 19].copy_from_slice(b"uncommitted payload");
            p.set_page_lsn(lsn2);
            Ok(())
        })
        .unwrap();
        bpm.flush_all().unwrap();

        let _unflushed_commit_lsn = wal
            .append_record(LogRecord::new_commit(0, 501, lsn2))
            .unwrap();
        assert_eq!(wal.flushed_lsn(), lsn2);
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.aborted_txns, vec![501]);
        assert_eq!(report.committed_txns, Vec::<TxnId>::new());
        assert_eq!(report.undone_records, 1);
    }

    {
        let fresh_disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let on_disk_page = fresh_disk.read_page(0).unwrap();
        assert_eq!(&on_disk_page.data[64..64 + 19], b"clean base state---");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_crash_after_commit_durability() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_after_commit_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

        let lsn1 = wal.append_record(LogRecord::new_begin(0, 502)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                502,
                lsn1,
                pid,
                32,
                b"durable committed value".to_vec(),
                b"old value--------------".to_vec(),
            ))
            .unwrap();
        let lsn3 = wal.commit(502, lsn2).unwrap();
        assert_eq!(wal.flushed_lsn(), lsn3);
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.committed_txns, vec![502]);
        assert_eq!(report.aborted_txns, Vec::<TxnId>::new());
        assert_eq!(report.redone_records, 1);
        assert_eq!(report.undone_records, 0);
    }

    {
        let fresh_disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let on_disk_page = fresh_disk.read_page(0).unwrap();
        assert_eq!(&on_disk_page.data[32..32 + 23], b"durable committed value");
        assert_eq!(on_disk_page.page_lsn(), 2);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_wal_flush_failure_prevents_dirty_page_write() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_wal_fail_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let (pid, mut p) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
    p.data[64..74].copy_from_slice(b"clean data");
    disk.write_page(pid, &p).unwrap();
    disk.sync().unwrap();

    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 2));
    let wal_for_hook = wal.clone();
    bpm.set_wal_check_hook(move |page_lsn| wal_for_hook.flush_up_to(page_lsn));

    bpm.update_page_data(pid, |page| {
        page.data[64..74].copy_from_slice(b"dirty data");
        page.set_page_lsn(99);
        Ok(())
    })
    .unwrap();

    let unlogged_err = bpm.flush_page(pid).unwrap_err();
    match unlogged_err {
        Error::WalCorrupted(msg) => assert!(msg.contains("unwritten LSN")),
        _ => panic!("Expected WalCorrupted for unwritten page_lsn"),
    }

    let disk_reader = DiskManager::open_or_create(db_path.as_path()).unwrap();
    let on_disk_page = disk_reader.read_page(pid).unwrap();
    assert_eq!(&on_disk_page.data[64..74], b"clean data");

    let fail_hook = Arc::new(AtomicBool::new(true));
    let fail_clone = fail_hook.clone();
    bpm.set_wal_check_hook(move |_page_lsn| {
        if fail_clone.load(Ordering::SeqCst) {
            Err(Error::Io("Simulated WAL flush I/O failure".to_string()))
        } else {
            Ok(())
        }
    });

    let err = bpm.flush_page(pid).unwrap_err();
    match err {
        Error::Io(msg) => assert!(msg.contains("Simulated WAL flush")),
        _ => panic!("Expected simulated I/O error"),
    }

    let on_disk_page_still_clean = disk_reader.read_page(pid).unwrap();
    assert_eq!(&on_disk_page_still_clean.data[64..74], b"clean data");

    fail_hook.store(false, Ordering::SeqCst);
    assert!(bpm.flush_page(pid).is_ok());

    let on_disk_page_after = disk_reader.read_page(pid).unwrap();
    assert_eq!(&on_disk_page_after.data[64..74], b"dirty data");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_corrupt_and_truncated_wal_records() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_corrupt_wal_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let bad_short_buf = vec![0u8; 45];
    match LogRecord::deserialize(&bad_short_buf) {
        Err(Error::WalCorrupted(msg)) => assert!(msg.contains("too short")),
        _ => panic!("Expected WalCorrupted for 45-byte buffer"),
    }

    {
        let wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let lsn1 = wal.append_record(LogRecord::new_begin(0, 601)).unwrap();
        wal.commit(601, lsn1).unwrap();
    }

    {
        let mut bytes = std::fs::read(wal_path.as_path()).unwrap();
        bytes[10] ^= 0xEE;
        std::fs::write(wal_path.as_path(), &bytes).unwrap();
    }

    {
        let res = WalManager::open_or_create(wal_path.as_path());
        match res {
            Err(Error::WalCorrupted(msg)) => assert!(msg.contains("CRC32C")),
            _ => panic!("Expected WalCorrupted CRC error on mid-log corruption"),
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
    let valid_wal_len: u64;
    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let lsn1 = wal.append_record(LogRecord::new_begin(0, 602)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                602,
                lsn1,
                pid,
                64,
                b"survives torn tail".to_vec(),
                b"old baseline------".to_vec(),
            ))
            .unwrap();
        wal.commit(602, lsn2).unwrap();
        valid_wal_len = std::fs::metadata(wal_path.as_path()).unwrap().len();
    }

    {
        let mut file = OpenOptions::new()
            .append(true)
            .open(wal_path.as_path())
            .unwrap();
        file.write_all(&[0x30, 0x00, 0x00]).unwrap();
        file.sync_all().unwrap();
    }

    {
        let wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        assert_eq!(
            std::fs::metadata(wal_path.as_path()).unwrap().len(),
            valid_wal_len
        );
        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 3);
    }

    {
        let mut file = OpenOptions::new()
            .append(true)
            .open(wal_path.as_path())
            .unwrap();
        let partial_rec = LogRecord::new_begin(4, 603).serialize();
        file.write_all(&partial_rec[..25]).unwrap();
        file.sync_all().unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.committed_txns, vec![602]);
        assert_eq!(report.redone_records, 1);
        assert_eq!(
            std::fs::metadata(wal_path.as_path()).unwrap().len(),
            valid_wal_len
        );

        let page = disk.read_page(0).unwrap();
        assert_eq!(&page.data[64..64 + 18], b"survives torn tail");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_checkpoint_recovery_skips_flushed_pages_and_undoes_active_txns() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_ckpt_rec_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (pid1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let lsn1 = wal.append_record(LogRecord::new_begin(0, 901)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                901,
                lsn1,
                pid0,
                64,
                b"checkpointed data".to_vec(),
                b"origin data------".to_vec(),
            ))
            .unwrap();
        bpm.update_page_data(pid0, |p| {
            p.data[64..64 + 17].copy_from_slice(b"checkpointed data");
            p.set_page_lsn(lsn2);
            Ok(())
        })
        .unwrap();
        wal.commit(901, lsn2).unwrap();

        let ckpt_lsn = recovery.create_checkpoint().unwrap();
        assert_eq!(ckpt_lsn, 4);

        let lsn5 = wal.append_record(LogRecord::new_begin(0, 902)).unwrap();
        let lsn6 = wal
            .append_record(LogRecord::new_update(
                0,
                902,
                lsn5,
                pid1,
                64,
                b"post-ckpt winner".to_vec(),
                b"post-ckpt base--".to_vec(),
            ))
            .unwrap();
        wal.commit(902, lsn6).unwrap();

        let lsn8 = wal.append_record(LogRecord::new_begin(0, 903)).unwrap();
        let lsn9 = wal
            .append_record(LogRecord::new_update(
                0,
                903,
                lsn8,
                pid1,
                128,
                b"uncommitted post".to_vec(),
                b"clean slot 128--".to_vec(),
            ))
            .unwrap();
        wal.flush_up_to(lsn9).unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.committed_txns, vec![901, 902]);
        assert_eq!(report.aborted_txns, vec![903]);
        assert_eq!(report.redone_records, 2);
        assert_eq!(report.undone_records, 1);

        let p0 = disk.read_page(0).unwrap();
        let p1 = disk.read_page(1).unwrap();
        assert_eq!(&p0.data[64..64 + 17], b"checkpointed data");
        assert_eq!(&p1.data[64..64 + 16], b"post-ckpt winner");
        assert_eq!(&p1.data[128..128 + 16], b"clean slot 128--");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_torn_or_corrupted_page_detected_fail_closed() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_torn_page_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let lsn1 = wal.append_record(LogRecord::new_begin(0, 950)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                950,
                lsn1,
                pid,
                64,
                b"redo payload".to_vec(),
                b"undo payload".to_vec(),
            ))
            .unwrap();
        wal.commit(950, lsn2).unwrap();
    }

    {
        let mut db_bytes = std::fs::read(db_path.as_path()).unwrap();
        db_bytes[2048] ^= 0xFF;
        std::fs::write(db_path.as_path(), &db_bytes).unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let err = recovery.recover().unwrap_err();
        match err {
            Error::PageCorrupted(msg) => assert!(msg.contains("CRC32C mismatch")),
            _ => panic!("Expected PageCorrupted on torn/corrupted page"),
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_crash_during_recovery_and_restart_recovery() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_crash_in_rec_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

        let lsn1 = wal.append_record(LogRecord::new_begin(0, 701)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                701,
                lsn1,
                pid,
                64,
                b"update 1".to_vec(),
                b"origin 1".to_vec(),
            ))
            .unwrap();
        let lsn3 = wal
            .append_record(LogRecord::new_update(
                0,
                701,
                lsn2,
                pid,
                96,
                b"update 2".to_vec(),
                b"origin 2".to_vec(),
            ))
            .unwrap();
        wal.flush_up_to(lsn3).unwrap();

        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        bpm.update_page_data(pid, |p| {
            p.data[64..72].copy_from_slice(b"update 1");
            p.data[96..104].copy_from_slice(b"update 2");
            p.set_page_lsn(lsn3);
            Ok(())
        })
        .unwrap();
        bpm.flush_all().unwrap();

        let clr1 = LogRecord::new_clr(0, 701, lsn3, pid, 96, lsn2, b"origin 2".to_vec());
        let clr1_lsn = wal.append_record(clr1).unwrap();
        wal.flush_up_to(clr1_lsn).unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.aborted_txns, vec![701]);
        assert_eq!(report.redone_records, 1);
        assert_eq!(report.undone_records, 1);
    }

    {
        let fresh_disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let page = fresh_disk.read_page(0).unwrap();
        assert_eq!(&page.data[64..72], b"origin 1");
        assert_eq!(&page.data[96..104], b"origin 2");

        let fresh_wal = WalManager::open_or_create(wal_path.as_path()).unwrap();
        let records = fresh_wal.read_all_records().unwrap();
        assert_eq!(records.len(), 6);
        assert_eq!(records[3].record_type, LogRecordType::Clr);
        assert_eq!(records[3].lsn, 4);
        assert_eq!(records[3].prev_lsn, 3);
        assert_eq!(records[3].undo_next_lsn, 2);

        assert_eq!(records[4].record_type, LogRecordType::Clr);
        assert_eq!(records[4].lsn, 5);
        assert_eq!(records[4].prev_lsn, 4);
        assert_eq!(records[4].undo_next_lsn, 1);

        assert_eq!(records[5].record_type, LogRecordType::Abort);
        assert_eq!(records[5].lsn, 6);
        assert_eq!(records[5].prev_lsn, 5);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_repeated_recovery_idempotence() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_idempotence_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let (pid, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());

        let lsn1 = wal.append_record(LogRecord::new_begin(0, 801)).unwrap();
        let lsn2 = wal
            .append_record(LogRecord::new_update(
                0,
                801,
                lsn1,
                pid,
                50,
                b"permanent winner".to_vec(),
                b"zeroed base data".to_vec(),
            ))
            .unwrap();
        wal.commit(801, lsn2).unwrap();

        let lsn4 = wal.append_record(LogRecord::new_begin(0, 802)).unwrap();
        let lsn5 = wal
            .append_record(LogRecord::new_update(
                0,
                802,
                lsn4,
                pid,
                80,
                b"uncommitted junk".to_vec(),
                b"pristine surface".to_vec(),
            ))
            .unwrap();
        wal.flush_up_to(lsn5).unwrap();
    }

    for run in 1..=5 {
        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 5));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let report = recovery.recover().unwrap();
            assert_eq!(report.committed_txns, vec![801]);
            assert_eq!(report.aborted_txns, vec![802]);

            if run == 1 {
                assert_eq!(report.undone_records, 1);
            } else {
                assert_eq!(report.redone_records, 0);
                assert_eq!(report.undone_records, 0);
            }
        }

        let fresh_disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let page = fresh_disk.read_page(0).unwrap();
        assert_eq!(&page.data[50..50 + 16], b"permanent winner");
        assert_eq!(&page.data[80..80 + 16], b"pristine surface");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_pinned_frame_protection_and_buffer_pool_eviction_failures() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_audit_pin_evict_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    for _ in 0..4 {
        disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
    }

    let bpm = BufferPoolManager::new(disk.clone(), 2);

    let _p0 = bpm.fetch_page(0).unwrap();
    let _p1 = bpm.fetch_page(1).unwrap();

    let err1 = bpm.fetch_page(2).unwrap_err();
    assert_eq!(err1, Error::BufferPoolExhausted);

    let err2 = bpm.new_page(PAGE_TYPE_SLOTTED).unwrap_err();
    assert_eq!(err2, Error::BufferPoolExhausted);

    bpm.unpin_page(0, false).unwrap();
    bpm.unpin_page(1, false).unwrap();

    let bad_fetch = bpm.fetch_page(999).unwrap_err();
    assert_eq!(bad_fetch, Error::InvalidPageId(999));

    let frames_after_bad_fetch = bpm.inspect_frames();
    assert_eq!(frames_after_bad_fetch[0].1, Some(0));
    assert_eq!(frames_after_bad_fetch[1].1, Some(1));

    let failed_update: mitrdb_common::Result<()> = bpm.update_page_data(0, |p| {
        p.insert_record(b"partial mutation that must roll back")?;
        Err(Error::Io("abort mutation".to_string()))
    });
    assert!(failed_update.is_err());

    let p0_check = bpm.fetch_page(0).unwrap();
    assert_eq!(p0_check.slot_count(), 0);
    let frames_after_failed_update = bpm.inspect_frames();
    assert!(!frames_after_failed_update[0].3);
    bpm.unpin_page(0, false).unwrap();

    let p2 = bpm.fetch_page(2);
    assert!(p2.is_ok());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_btree_uncommitted_root_and_internal_split_crash_recovery() {
    use mitrdb_common::INVALID_PAGE_ID;
    use mitrdb_storage::btree::BPlusTree;

    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_audit_btree_split_undo_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let initial_root_id: u32;
    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));

        let mut lsn_t1 = wal.append_record(LogRecord::new_begin(0, 2001)).unwrap();
        let tree =
            BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 3, 2001, &mut lsn_t1).unwrap();
        for i in 1..=3 {
            let k = format!("k{:02}", i * 10);
            let v = format!("committed_val_{}", i);
            tree.insert_logged(2001, &mut lsn_t1, k.as_bytes(), v.as_bytes())
                .unwrap();
        }
        wal.commit(2001, lsn_t1).unwrap();
        initial_root_id = tree.root_page_id();
        assert_eq!(initial_root_id, 0);

        let mut lsn_t2 = wal.append_record(LogRecord::new_begin(0, 2002)).unwrap();
        for i in 4..=18 {
            let k = format!("k{:02}", i * 10);
            let v = format!("uncommitted_split_val_{}", i);
            tree.insert_logged(2002, &mut lsn_t2, k.as_bytes(), v.as_bytes())
                .unwrap();
        }
        assert_ne!(tree.root_page_id(), initial_root_id);
        wal.flush_up_to(lsn_t2).unwrap();
        bpm.flush_all().unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.committed_txns, vec![2001]);
        assert_eq!(report.aborted_txns, vec![2002]);
        assert!(report.undone_records >= 5);

        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), initial_root_id, 3).unwrap();
        let root_node = tree.read_node(initial_root_id).unwrap();
        assert!(root_node.is_leaf);
        assert_eq!(root_node.parent_page_id, INVALID_PAGE_ID);
        assert_eq!(root_node.next_leaf_page_id, INVALID_PAGE_ID);

        let scanned = tree.range_scan(None, None).unwrap();
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0], (b"k10".to_vec(), b"committed_val_1".to_vec()));
        assert_eq!(scanned[1], (b"k20".to_vec(), b"committed_val_2".to_vec()));
        assert_eq!(scanned[2], (b"k30".to_vec(), b"committed_val_3".to_vec()));

        let mut lsn_t3 = wal.append_record(LogRecord::new_begin(0, 2003)).unwrap();
        for i in 4..=10 {
            let k = format!("k{:02}", i * 10);
            let v = format!("post_recovery_val_{}", i);
            tree.insert_logged(2003, &mut lsn_t3, k.as_bytes(), v.as_bytes())
                .unwrap();
        }
        wal.commit(2003, lsn_t3).unwrap();
        assert_eq!(tree.range_scan(None, None).unwrap().len(), 10);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
