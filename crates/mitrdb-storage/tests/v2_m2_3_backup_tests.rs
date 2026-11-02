use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

use mitrdb_common::Error;
use mitrdb_storage::backup::{
    BACKUP_MANIFEST_VERSION, BackupFileMeta, BackupManager, BackupManifest,
};
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_TYPE_RAW;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::{LogRecord, WalManager};

#[test]
fn test_quiescent_backup_validate_and_restore() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_bkp_quiescent_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("backup_1");
    let dst_dir = temp_dir.join("restored_db");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (p1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 256).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let l1 = wal.append_record(LogRecord::new_begin(0, 10)).unwrap();
        let l2 = wal
            .append_record(LogRecord::new_update(
                0,
                10,
                l1,
                p0,
                64,
                b"quiescent_page_0_payload".to_vec(),
                vec![0u8; 24],
            ))
            .unwrap();
        let l3 = wal
            .append_record(LogRecord::new_update(
                0,
                10,
                l2,
                p1,
                128,
                b"quiescent_page_1_payload".to_vec(),
                vec![0u8; 24],
            ))
            .unwrap();
        wal.commit(10, l3).unwrap();

        bpm.update_page_data(p0, |p| {
            p.data[64..88].copy_from_slice(b"quiescent_page_0_payload");
            p.set_page_lsn(l2);
            Ok(())
        })
        .unwrap();
        bpm.update_page_data(p1, |p| {
            p.data[128..152].copy_from_slice(b"quiescent_page_1_payload");
            p.set_page_lsn(l3);
            Ok(())
        })
        .unwrap();
        let _ = recovery.create_checkpoint_with_meta().unwrap();
    }

    let manifest = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_dir).unwrap();
    assert_eq!(manifest.manifest_version, BACKUP_MANIFEST_VERSION);
    assert_eq!(manifest.max_user_txn_id, 10);
    assert!(bkp_dir.join("backup.manifest").exists());
    assert!(bkp_dir.join("backup_manifest.json").exists());
    assert!(bkp_dir.join("test.db").exists());

    let validated = BackupManager::validate_backup(&bkp_dir).unwrap();
    assert_eq!(validated, manifest);

    let report = BackupManager::restore_backup(&bkp_dir, &dst_dir).unwrap();
    assert_eq!(report.backup_id, manifest.backup_id);
    assert_eq!(report.max_user_txn_id, 10);

    let restored_disk = DiskManager::open_or_create(dst_dir.join("test.db")).unwrap();
    let rp0 = restored_disk.read_page(0).unwrap();
    let rp1 = restored_disk.read_page(1).unwrap();
    assert_eq!(&rp0.data[64..88], b"quiescent_page_0_payload");
    assert_eq!(&rp1.data[128..152], b"quiescent_page_1_payload");

    let validated_after = BackupManager::validate_backup(&bkp_dir).unwrap();
    assert_eq!(validated_after, manifest);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_online_backup_with_active_uncommitted_and_committed_txns() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_bkp_online_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("backup_online");
    let dst_dir = temp_dir.join("restored_online");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (p1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 200).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
        let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));

        let l1 = wal.append_record(LogRecord::new_begin(0, 20)).unwrap();
        let l2 = wal
            .append_record(LogRecord::new_update(
                0,
                20,
                l1,
                p0,
                64,
                b"committed_data_v1".to_vec(),
                vec![0u8; 17],
            ))
            .unwrap();
        wal.commit(20, l2).unwrap();
        bpm.update_page_data(p0, |p| {
            p.data[64..81].copy_from_slice(b"committed_data_v1");
            p.set_page_lsn(l2);
            Ok(())
        })
        .unwrap();

        bpm.update_page_data(p1, |p| {
            p.data[64..78].copy_from_slice(b"clean_baseline");
            Ok(())
        })
        .unwrap();
        bpm.flush_all().unwrap();

        let l3 = wal.append_record(LogRecord::new_begin(0, 25)).unwrap();
        let l4 = wal
            .append_record(LogRecord::new_update(
                0,
                25,
                l3,
                p1,
                64,
                b"dirty_uncommit".to_vec(),
                b"clean_baseline".to_vec(),
            ))
            .unwrap();
        bpm.update_page_data(p1, |p| {
            p.data[64..78].copy_from_slice(b"dirty_uncommit");
            p.set_page_lsn(l4);
            Ok(())
        })
        .unwrap();

        let manifest =
            BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_dir).unwrap();
        assert!(manifest.min_recovery_lsn <= l3);
        assert!(manifest.checkpoint_lsn > l4);

        let report = BackupManager::restore_backup(&bkp_dir, &dst_dir).unwrap();
        assert_eq!(report.aborted_txns, vec![25]);
        assert_eq!(report.undone_records, 1);

        let restored_disk = DiskManager::open_or_create(dst_dir.join("test.db")).unwrap();
        let rp0 = restored_disk.read_page(0).unwrap();
        let rp1 = restored_disk.read_page(1).unwrap();
        assert_eq!(&rp0.data[64..81], b"committed_data_v1");
        assert_eq!(&rp1.data[64..78], b"clean_baseline");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_backup_pin_protects_wal_segments_across_concurrent_checkpoints() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_v2_bkp_pin_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
    let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 160).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
    let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

    let l1 = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let l2 = wal
        .append_record(LogRecord::new_update(
            0,
            1,
            l1,
            p0,
            64,
            b"seg0_data".to_vec(),
            vec![0u8; 9],
        ))
        .unwrap();
    wal.commit(1, l2).unwrap();

    let pin_id = wal.pin_backup_horizon(l1);

    for tid in 2..=8u64 {
        let lb = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
        let lu = wal
            .append_record(LogRecord::new_update(
                0,
                tid,
                lb,
                p0,
                64,
                format!("payload_{:04}", tid).into_bytes(),
                vec![0u8; 12],
            ))
            .unwrap();
        wal.commit(tid, lu).unwrap();
    }

    assert!(wal.segment_count() >= 4);

    let ckpt1 = recovery.create_checkpoint_with_meta().unwrap();
    assert_eq!(ckpt1.min_retained_segment_id, 0);
    assert_eq!(ckpt1.segments_reclaimed, 0);
    assert!(wal_path.exists());

    wal.unpin_backup_horizon(pin_id);

    let ckpt2 = recovery.create_checkpoint_with_meta().unwrap();
    assert!(ckpt2.min_retained_segment_id > 0);
    assert!(ckpt2.segments_reclaimed > 0);
    assert!(!wal_path.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_corrupt_manifest_and_missing_or_tampered_files_rejected() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_bkp_corrupt_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("backup_good");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let _ = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let l1 = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        wal.commit(1, l1).unwrap();
    }

    let manifest = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_dir).unwrap();
    let manifest_bytes = manifest.serialize();

    assert!(BackupManifest::deserialize(&manifest_bytes[..10]).is_err());

    let mut bad_magic = manifest_bytes.clone();
    bad_magic[0] = b'X';
    let crc = mitrdb_storage::crc32c::checksum(&bad_magic[..bad_magic.len() - 4]);
    let len = bad_magic.len();
    bad_magic[len - 4..].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        BackupManifest::deserialize(&bad_magic),
        Err(Error::BackupError(_))
    ));

    let mut bad_ver = manifest_bytes.clone();
    bad_ver[8..12].copy_from_slice(&99u32.to_le_bytes());
    let crc2 = mitrdb_storage::crc32c::checksum(&bad_ver[..bad_ver.len() - 4]);
    bad_ver[len - 4..].copy_from_slice(&crc2.to_le_bytes());
    assert!(matches!(
        BackupManifest::deserialize(&bad_ver),
        Err(Error::BackupError(_))
    ));

    let mut bad_crc = manifest_bytes.clone();
    bad_crc[15] ^= 0x55;
    assert!(matches!(
        BackupManifest::deserialize(&bad_crc),
        Err(Error::BackupError(_))
    ));

    let bkp_corrupt_db = temp_dir.join("bkp_corrupt_db");
    let _ = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_corrupt_db).unwrap();
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(bkp_corrupt_db.join("test.db"))
            .unwrap();
        f.write_all(b"corrupted").unwrap();
    }
    let dst_fail1 = temp_dir.join("dst_fail1");
    assert!(BackupManager::restore_backup(&bkp_corrupt_db, &dst_fail1).is_err());
    assert!(!dst_fail1.exists());

    let bkp_missing_wal = temp_dir.join("bkp_missing_wal");
    let m2 = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_missing_wal).unwrap();
    std::fs::remove_file(bkp_missing_wal.join(&m2.wal_segments[0].file_name)).unwrap();
    let dst_fail2 = temp_dir.join("dst_fail2");
    assert!(BackupManager::restore_backup(&bkp_missing_wal, &dst_fail2).is_err());
    assert!(!dst_fail2.exists());

    let mut empty_segs_manifest = manifest.clone();
    empty_segs_manifest.wal_segments.clear();
    let bkp_empty_segs = temp_dir.join("bkp_empty_segs");
    let _ = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_empty_segs).unwrap();
    std::fs::write(
        bkp_empty_segs.join("backup.manifest"),
        empty_segs_manifest.serialize(),
    )
    .unwrap();
    assert!(BackupManager::validate_backup(&bkp_empty_segs).is_err());

    let _ = BackupFileMeta {
        file_name: "dummy".to_string(),
        size_bytes: 0,
        crc32c: 0,
    };

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_existing_destination_protection_and_staging_cleanup() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_v2_bkp_dest_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("bkp_1");
    let dst_dir = temp_dir.join("dst_1");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let _ = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_or_create(&wal_path).unwrap());
        let l1 = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
        wal.commit(1, l1).unwrap();
    }

    let _ = BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_dir).unwrap();

    assert!(matches!(
        BackupManager::create_quiescent_backup(&db_path, &wal_path, &bkp_dir),
        Err(Error::BackupError(_))
    ));

    let _ = BackupManager::restore_backup(&bkp_dir, &dst_dir).unwrap();

    assert!(matches!(
        BackupManager::restore_backup(&bkp_dir, &dst_dir),
        Err(Error::BackupError(_))
    ));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_repeated_backup_restore_cycles_and_monotonic_continuity() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_v2_bkp_chain_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);

    let mut cur_db_dir = temp_dir.join("db_gen_0");
    let mut root_page_id;

    {
        let db_path = cur_db_dir.join("test.db");
        let wal_path = cur_db_dir.join("test.wal");
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 384).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
        let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let tid = 10;
        let mut lsn = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
        let tree =
            BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, tid, &mut lsn).unwrap();
        tree.insert_logged(tid, &mut lsn, b"k_gen_0", b"val_gen_0")
            .unwrap();
        wal.commit(tid, lsn).unwrap();
        root_page_id = tree.root_page_id();
        bpm.flush_all().unwrap();
    }

    for generation in 1..=3u64 {
        let bkp_dir = temp_dir.join(format!("bkp_gen_{}", generation));
        let next_db_dir = temp_dir.join(format!("db_gen_{}", generation));

        let manifest = BackupManager::create_quiescent_backup(
            &cur_db_dir.join("test.db"),
            &cur_db_dir.join("test.wal"),
            &bkp_dir,
        )
        .unwrap();
        assert!(manifest.max_user_txn_id >= generation * 10);

        let report = BackupManager::restore_backup(&bkp_dir, &next_db_dir).unwrap();
        assert_eq!(report.max_user_txn_id, manifest.max_user_txn_id);

        {
            let db_path = next_db_dir.join("test.db");
            let wal_path = next_db_dir.join("test.wal");
            let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
            let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 384).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let _ = recovery.recover().unwrap();

            let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_page_id, 4).unwrap();
            for prev_gen in 0..generation {
                let k = format!("k_gen_{}", prev_gen);
                let v = format!("val_gen_{}", prev_gen);
                assert_eq!(tree.get(k.as_bytes()).unwrap(), Some(v.into_bytes()));
            }

            let new_tid = (generation + 1) * 10;
            assert!(new_tid > wal.max_user_txn_id());
            let mut lsn = wal.append_record(LogRecord::new_begin(0, new_tid)).unwrap();
            let k_new = format!("k_gen_{}", generation);
            let v_new = format!("val_gen_{}", generation);
            tree.insert_logged(new_tid, &mut lsn, k_new.as_bytes(), v_new.as_bytes())
                .unwrap();
            wal.commit(new_tid, lsn).unwrap();
            root_page_id = tree.root_page_id();
            bpm.flush_all().unwrap();
        }

        cur_db_dir = next_db_dir;
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_online_backup_pin_cleanup_and_concurrent_checkpoint_redo_safety() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_v2_bkp_audit_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("bkp_audit");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
    let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 160).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
    let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));

    let l1 = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let l2 = wal
        .append_record(LogRecord::new_update(
            0,
            1,
            l1,
            p0,
            64,
            b"pre_ckpt_committed".to_vec(),
            vec![0u8; 18],
        ))
        .unwrap();
    wal.commit(1, l2).unwrap();

    let _ = wal
        .append_record(LogRecord::new_checkpoint(0, &[]))
        .unwrap();
    wal.flush_all().unwrap();

    let rec_report = recovery.recover().unwrap();
    assert_eq!(rec_report.redone_records, 1);
    let page0_after = disk.read_page(p0).unwrap();
    assert_eq!(&page0_after.data[64..82], b"pre_ckpt_committed");

    let manifest =
        BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_dir).unwrap();

    let mut bad_path_manifest = manifest.clone();
    bad_path_manifest.db_file.file_name = "../escape.db".to_string();
    let bkp_bad_path = temp_dir.join("bkp_bad_path");
    let _ =
        BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_bad_path).unwrap();
    std::fs::write(
        bkp_bad_path.join("backup.manifest"),
        bad_path_manifest.serialize(),
    )
    .unwrap();
    assert!(matches!(
        BackupManager::validate_backup(&bkp_bad_path),
        Err(Error::BackupError(_))
    ));

    let mut bad_lsn_manifest = manifest.clone();
    bad_lsn_manifest.min_recovery_lsn = bad_lsn_manifest.checkpoint_lsn + 10;
    let bkp_bad_lsn = temp_dir.join("bkp_bad_lsn");
    let _ =
        BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_bad_lsn).unwrap();
    std::fs::write(
        bkp_bad_lsn.join("backup.manifest"),
        bad_lsn_manifest.serialize(),
    )
    .unwrap();
    assert!(matches!(
        BackupManager::validate_backup(&bkp_bad_lsn),
        Err(Error::BackupError(_))
    ));

    let bkp_future_page_lsn = temp_dir.join("bkp_future_page_lsn");
    let mut future_page_manifest =
        BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_future_page_lsn)
            .unwrap();
    {
        let db_file_path = bkp_future_page_lsn.join(&future_page_manifest.db_file.file_name);
        let bkp_disk = DiskManager::open_or_create(&db_file_path).unwrap();
        let mut p0_copy = bkp_disk.read_page(0).unwrap();
        p0_copy.set_page_lsn(future_page_manifest.backup_end_lsn + 50);
        bkp_disk.write_page(0, &p0_copy).unwrap();
        bkp_disk.sync().unwrap();
        let db_bytes = std::fs::read(&db_file_path).unwrap();
        future_page_manifest.db_file.size_bytes = db_bytes.len() as u64;
        future_page_manifest.db_file.crc32c = mitrdb_storage::crc32c::checksum(&db_bytes);
        std::fs::write(
            bkp_future_page_lsn.join("backup.manifest"),
            future_page_manifest.serialize(),
        )
        .unwrap();
    }
    assert!(matches!(
        BackupManager::validate_backup(&bkp_future_page_lsn),
        Err(Error::BackupError(_))
    ));

    let bkp_extra_wal = temp_dir.join("bkp_extra_wal");
    let mut extra_wal_manifest =
        BackupManager::create_online_backup(&disk, &bpm, &wal, &recovery, &bkp_extra_wal).unwrap();
    {
        let last_seg = extra_wal_manifest.wal_segments.last_mut().unwrap();
        let seg_path = bkp_extra_wal.join(&last_seg.file_name);
        let extra_rec = LogRecord::new_begin(extra_wal_manifest.backup_end_lsn + 1, 999);
        let mut seg_file = OpenOptions::new().append(true).open(&seg_path).unwrap();
        seg_file.write_all(&extra_rec.serialize()).unwrap();
        seg_file.sync_all().unwrap();
        let seg_bytes = std::fs::read(&seg_path).unwrap();
        last_seg.size_bytes = seg_bytes.len() as u64;
        last_seg.crc32c = mitrdb_storage::crc32c::checksum(&seg_bytes);
        std::fs::write(
            bkp_extra_wal.join("backup.manifest"),
            extra_wal_manifest.serialize(),
        )
        .unwrap();
    }
    assert!(matches!(
        BackupManager::validate_backup(&bkp_extra_wal),
        Err(Error::BackupError(_))
    ));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_online_backup_concurrent_page_allocations_updates_and_segment_rotations() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_bkp_cutoff_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("bkp_cutoff");
    let dst_dir = temp_dir.join("dst_cutoff");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 256).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));

    let mut init_lsn = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = Arc::new(
        BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, 1, &mut init_lsn).unwrap(),
    );
    for i in 0..12u32 {
        let k = format!("base_{:03}", i);
        let v = format!("val_{:03}", i);
        tree.insert_logged(1, &mut init_lsn, k.as_bytes(), v.as_bytes())
            .unwrap();
    }
    wal.commit(1, init_lsn).unwrap();
    bpm.flush_all().unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(3));

    let tree_writer = tree.clone();
    let wal_writer = wal.clone();
    let bar_writer = barrier.clone();
    let writer_handle = std::thread::spawn(move || {
        bar_writer.wait();
        for idx in 0..20u32 {
            let tid = 100 + idx as u64;
            let mut lsn = wal_writer
                .append_record(LogRecord::new_begin(0, tid))
                .unwrap();
            let k_new = format!("conc_{:03}", idx);
            let v_new = format!("cval_{:03}", idx);
            tree_writer
                .insert_logged(tid, &mut lsn, k_new.as_bytes(), v_new.as_bytes())
                .unwrap();
            let k_upd = format!("base_{:03}", idx % 12);
            let v_upd = format!("uval_{:03}_{:03}", idx % 12, idx);
            tree_writer
                .insert_logged(tid, &mut lsn, k_upd.as_bytes(), v_upd.as_bytes())
                .unwrap();
            wal_writer.commit(tid, lsn).unwrap();
        }
    });

    let rec_ckpt = recovery.clone();
    let bar_ckpt = barrier.clone();
    let ckpt_handle = std::thread::spawn(move || {
        bar_ckpt.wait();
        for _ in 0..4 {
            let _ = rec_ckpt.create_checkpoint_with_meta().unwrap();
        }
    });

    let disk_bkp = disk.clone();
    let bpm_bkp = bpm.clone();
    let wal_bkp = wal.clone();
    let rec_bkp = recovery.clone();
    let bkp_path = bkp_dir.clone();
    let bar_bkp = barrier.clone();
    let bkp_handle = std::thread::spawn(move || {
        bar_bkp.wait();
        BackupManager::create_online_backup(&disk_bkp, &bpm_bkp, &wal_bkp, &rec_bkp, &bkp_path)
            .unwrap()
    });

    writer_handle.join().unwrap();
    ckpt_handle.join().unwrap();
    let manifest = bkp_handle.join().unwrap();

    let validated = BackupManager::validate_backup(&bkp_dir).unwrap();
    assert_eq!(validated, manifest);
    assert!(manifest.backup_end_lsn >= manifest.checkpoint_lsn);
    assert!(manifest.checkpoint_lsn >= manifest.min_recovery_lsn);

    let report = BackupManager::restore_backup(&bkp_dir, &dst_dir).unwrap();
    assert_eq!(report.backup_id, manifest.backup_id);

    let restored_disk = Arc::new(DiskManager::open_or_create(dst_dir.join("test.db")).unwrap());
    let restored_wal =
        Arc::new(WalManager::open_with_segment_size(dst_dir.join("test.wal"), 256).unwrap());
    let restored_bpm = Arc::new(BufferPoolManager::new(restored_disk.clone(), 16));
    let restored_rec = RecoveryManager::new(restored_wal.clone(), restored_bpm.clone());
    let _ = restored_rec.recover().unwrap();

    let mut root_id = 0;
    while let Ok(p) = restored_bpm.fetch_page(root_id) {
        let node = mitrdb_storage::btree::BTreeNode::from_page(&p).unwrap();
        let _ = restored_bpm.unpin_page(root_id, false);
        if node.parent_page_id != mitrdb_common::INVALID_PAGE_ID {
            root_id = node.parent_page_id;
        } else {
            break;
        }
    }
    let restored_tree =
        BPlusTree::open(restored_bpm.clone(), Some(restored_wal.clone()), root_id, 4).unwrap();
    for i in 0..12u32 {
        let k = format!("base_{:03}", i);
        let val = restored_tree.get(k.as_bytes()).unwrap();
        assert!(val.is_some());
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_online_backup_concurrent_btree_merges_free_page_finalization_and_reuse() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_bkp_lockorder_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir_1 = temp_dir.join("bkp_lockorder_1");
    let bkp_dir_2 = temp_dir.join("bkp_lockorder_2");
    let dst_dir = temp_dir.join("dst_lockorder");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
    let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 256).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
    let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));

    let mut init_lsn = wal.append_record(LogRecord::new_begin(0, 1)).unwrap();
    let tree = Arc::new(
        BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, 1, &mut init_lsn).unwrap(),
    );
    for i in 0..20u32 {
        let k = format!("mkey_{:03}", i);
        let v = format!("mval_{:03}", i);
        tree.insert_logged(1, &mut init_lsn, k.as_bytes(), v.as_bytes())
            .unwrap();
    }
    wal.commit(1, init_lsn).unwrap();
    bpm.flush_all().unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(3));

    let tree_churn = tree.clone();
    let wal_churn = wal.clone();
    let bar_churn = barrier.clone();
    let churn_handle = std::thread::spawn(move || {
        bar_churn.wait();
        for round in 0..12u32 {
            let del_tid = 200 + round as u64 * 3;
            let mut del_lsn = wal_churn
                .append_record(LogRecord::new_begin(0, del_tid))
                .unwrap();
            for offset in 0..6u32 {
                let idx = (round * 3 + offset) % 20;
                let k = format!("mkey_{:03}", idx);
                let _ = tree_churn
                    .delete_logged(del_tid, &mut del_lsn, k.as_bytes())
                    .unwrap();
            }
            wal_churn.commit(del_tid, del_lsn).unwrap();

            let abt_tid = del_tid + 1;
            let abt_lsn = wal_churn
                .append_record(LogRecord::new_begin(0, abt_tid))
                .unwrap();
            let _ = wal_churn
                .append_record(LogRecord::new_abort(0, abt_tid, abt_lsn))
                .unwrap();

            let ins_tid = del_tid + 2;
            let mut ins_lsn = wal_churn
                .append_record(LogRecord::new_begin(0, ins_tid))
                .unwrap();
            for offset in 0..6u32 {
                let idx = (round * 3 + offset) % 20;
                let k = format!("mkey_{:03}", idx);
                let v = format!("mval_r{:02}_{:03}", round, idx);
                tree_churn
                    .insert_logged(ins_tid, &mut ins_lsn, k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
            wal_churn.commit(ins_tid, ins_lsn).unwrap();
        }
    });

    let rec_ckpt = recovery.clone();
    let bar_ckpt = barrier.clone();
    let ckpt_handle = std::thread::spawn(move || {
        bar_ckpt.wait();
        for _ in 0..6 {
            let _ = rec_ckpt.create_checkpoint_with_meta().unwrap();
        }
    });

    let disk_bkp = disk.clone();
    let bpm_bkp = bpm.clone();
    let wal_bkp = wal.clone();
    let rec_bkp = recovery.clone();
    let bkp1 = bkp_dir_1.clone();
    let bkp2 = bkp_dir_2.clone();
    let bar_bkp = barrier.clone();
    let bkp_handle = std::thread::spawn(move || {
        bar_bkp.wait();
        let m1 =
            BackupManager::create_online_backup(&disk_bkp, &bpm_bkp, &wal_bkp, &rec_bkp, &bkp1)
                .unwrap();
        let m2 =
            BackupManager::create_online_backup(&disk_bkp, &bpm_bkp, &wal_bkp, &rec_bkp, &bkp2)
                .unwrap();
        (m1, m2)
    });

    churn_handle.join().unwrap();
    ckpt_handle.join().unwrap();
    let (m1, m2) = bkp_handle.join().unwrap();

    assert_eq!(BackupManager::validate_backup(&bkp_dir_1).unwrap(), m1);
    assert_eq!(BackupManager::validate_backup(&bkp_dir_2).unwrap(), m2);

    let report = BackupManager::restore_backup(&bkp_dir_2, &dst_dir).unwrap();
    assert_eq!(report.backup_id, m2.backup_id);

    let restored_disk = Arc::new(DiskManager::open_or_create(dst_dir.join("test.db")).unwrap());
    let restored_wal =
        Arc::new(WalManager::open_with_segment_size(dst_dir.join("test.wal"), 256).unwrap());
    let restored_bpm = Arc::new(BufferPoolManager::new(restored_disk.clone(), 16));
    let restored_rec = RecoveryManager::new(restored_wal.clone(), restored_bpm.clone());
    let _ = restored_rec.recover().unwrap();

    let mut root_id = 0;
    while let Ok(p) = restored_bpm.fetch_page(root_id) {
        let node = mitrdb_storage::btree::BTreeNode::from_page(&p).unwrap();
        let _ = restored_bpm.unpin_page(root_id, false);
        if node.parent_page_id != mitrdb_common::INVALID_PAGE_ID {
            root_id = node.parent_page_id;
        } else {
            break;
        }
    }
    let restored_tree =
        BPlusTree::open(restored_bpm.clone(), Some(restored_wal.clone()), root_id, 4).unwrap();
    let scanned = restored_tree.range_scan(None, None).unwrap();
    assert!(!scanned.is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
}
