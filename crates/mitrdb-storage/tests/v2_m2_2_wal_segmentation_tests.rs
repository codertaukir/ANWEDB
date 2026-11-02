use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

use mitrdb_common::{Error, TxnId};
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::page::PAGE_TYPE_RAW;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::{CheckpointMetadata, LogRecord, MIN_WAL_SEGMENT_BYTES, WalManager};

#[test]
fn test_wal_segment_creation_rotation_and_record_boundaries() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_wal_seg_rot_{}", std::process::id()));
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    assert!(matches!(
        WalManager::open_with_segment_size(&wal_path, MIN_WAL_SEGMENT_BYTES - 1),
        Err(Error::WalCorrupted(_))
    ));

    let wal = WalManager::open_with_segment_size(&wal_path, 180).unwrap();
    assert_eq!(wal.max_segment_bytes(), 180);
    assert_eq!(wal.active_segment_id(), 0);
    assert_eq!(wal.segment_ids(), vec![0]);

    let mut assigned_lsns = Vec::new();
    for tid in 1..=12u64 {
        let l1 = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
        let l2 = wal
            .append_record(LogRecord::new_update(
                0,
                tid,
                l1,
                0,
                64,
                format!("redo_{:02}", tid).into_bytes(),
                format!("undo_{:02}", tid).into_bytes(),
            ))
            .unwrap();
        let l3 = wal.commit(tid, l2).unwrap();
        assigned_lsns.push(l1);
        assigned_lsns.push(l2);
        assigned_lsns.push(l3);
    }

    for (idx, &lsn) in assigned_lsns.iter().enumerate() {
        assert_eq!(lsn, (idx as u64) + 1);
    }

    let seg_ids = wal.segment_ids();
    assert!(seg_ids.len() >= 12);
    for (idx, &sid) in seg_ids.iter().enumerate() {
        assert_eq!(sid, idx as u64);
        let spath = wal.segment_path_for_id(sid);
        assert!(spath.exists());
        let len = std::fs::metadata(&spath).unwrap().len();
        assert!(len > 0 && len <= 180);
    }

    let big_redo = vec![b'R'; 300];
    let big_undo = vec![b'U'; 300];
    let l_big = wal
        .append_record(LogRecord::new_update(
            0,
            99,
            0,
            0,
            64,
            big_redo.clone(),
            big_undo.clone(),
        ))
        .unwrap();
    wal.flush_up_to(l_big).unwrap();

    let manual_seg = wal.rotate_segment().unwrap();
    assert_eq!(manual_seg, wal.active_segment_id());
    assert_eq!(wal.rotate_segment().unwrap(), manual_seg);

    let l_after_manual = wal.append_record(LogRecord::new_begin(0, 100)).unwrap();
    wal.commit(100, l_after_manual).unwrap();

    let all_records = wal.read_all_records().unwrap();
    assert_eq!(all_records.len(), 39);
    for (i, rec) in all_records.iter().enumerate() {
        assert_eq!(rec.lsn, (i as u64) + 1);
    }
    assert_eq!(all_records[36].redo_data, big_redo);
    assert_eq!(all_records[36].undo_data, big_undo);

    drop(wal);

    let reopened = WalManager::open_with_segment_size(&wal_path, 180).unwrap();
    assert_eq!(reopened.flushed_lsn(), 39);
    assert_eq!(reopened.next_lsn(), 40);
    let l40 = reopened
        .append_record(LogRecord::new_begin(0, 101))
        .unwrap();
    assert_eq!(l40, 40);
    reopened.commit(101, l40).unwrap();
    assert_eq!(reopened.read_all_records().unwrap().len(), 41);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_multi_segment_spanning_transactions_crash_recovery() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_wal_span_rec_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (p1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 160).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let mut prev_winner = wal.append_record(LogRecord::new_begin(0, 10)).unwrap();
        for step in 0..6u16 {
            let off = 64 + step * 32;
            let redo = format!("winner_step_{:02}_data", step).into_bytes();
            let undo = vec![0u8; redo.len()];
            prev_winner = wal
                .append_record(LogRecord::new_update(
                    0,
                    10,
                    prev_winner,
                    p0,
                    off,
                    redo,
                    undo,
                ))
                .unwrap();
        }
        wal.commit(10, prev_winner).unwrap();

        let mut prev_loser = wal.append_record(LogRecord::new_begin(0, 20)).unwrap();
        for step in 0..6u16 {
            let off = 64 + step * 32;
            let orig = format!("clean_base_slot_{:02}", step).into_bytes();
            let dirty = format!("dirty_uncomm_s_{:02}", step).into_bytes();
            bpm.update_page_data(p1, |p| {
                p.data[off as usize..off as usize + orig.len()].copy_from_slice(&orig);
                p.update_checksum();
                Ok(())
            })
            .unwrap();
            prev_loser = wal
                .append_record(LogRecord::new_update(
                    0,
                    20,
                    prev_loser,
                    p1,
                    off,
                    dirty.clone(),
                    orig,
                ))
                .unwrap();
            let lsn_now = prev_loser;
            bpm.update_page_data(p1, |p| {
                p.data[off as usize..off as usize + dirty.len()].copy_from_slice(&dirty);
                p.set_page_lsn(lsn_now);
                Ok(())
            })
            .unwrap();
        }
        wal.flush_up_to(prev_loser).unwrap();
        bpm.flush_all().unwrap();
        assert!(wal.segment_count() >= 6);
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 160).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.committed_txns, vec![10]);
        assert_eq!(report.aborted_txns, vec![20]);
        assert_eq!(report.redone_records, 6);
        assert_eq!(report.undone_records, 6);

        let page0 = disk.read_page(0).unwrap();
        let page1 = disk.read_page(1).unwrap();
        for step in 0..6u16 {
            let off = (64 + step * 32) as usize;
            let expected_w = format!("winner_step_{:02}_data", step).into_bytes();
            let expected_l = format!("clean_base_slot_{:02}", step).into_bytes();
            assert_eq!(
                &page0.data[off..off + expected_w.len()],
                expected_w.as_slice()
            );
            assert_eq!(
                &page1.data[off..off + expected_l.len()],
                expected_l.as_slice()
            );
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_checkpoint_reclaims_obsolete_segments_and_retains_active_txn_segments() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_wal_ckpt_ret_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let seg0_path;
    let seg1_path;
    let active_begin_lsn;
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let (p1, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 150).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        seg0_path = wal.segment_path_for_id(0);
        seg1_path = wal.segment_path_for_id(1);

        for tid in 1..=3u64 {
            let l1 = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
            let l2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    tid,
                    l1,
                    p0,
                    64,
                    format!("comm_v_{}", tid).into_bytes(),
                    b"old_v_0".to_vec(),
                ))
                .unwrap();
            bpm.update_page_data(p0, |p| {
                p.data[64..72].copy_from_slice(format!("comm_v_{}", tid).as_bytes());
                p.set_page_lsn(l2);
                Ok(())
            })
            .unwrap();
            wal.commit(tid, l2).unwrap();
        }
        assert!(wal.active_segment_id() >= 2);
        let active_seg_at_begin = wal.active_segment_id();

        let l_act_begin = wal.append_record(LogRecord::new_begin(0, 50)).unwrap();
        active_begin_lsn = l_act_begin;
        let l_act_upd = wal
            .append_record(LogRecord::new_update(
                0,
                50,
                l_act_begin,
                p1,
                64,
                b"uncommitted_50".to_vec(),
                b"original_p1___".to_vec(),
            ))
            .unwrap();
        bpm.update_page_data(p1, |p| {
            p.data[64..78].copy_from_slice(b"uncommitted_50");
            p.set_page_lsn(l_act_upd);
            Ok(())
        })
        .unwrap();

        for tid in 4..=6u64 {
            let l1 = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
            let l2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    tid,
                    l1,
                    p0,
                    96,
                    format!("post_v_{}", tid).into_bytes(),
                    b"old_v_1".to_vec(),
                ))
                .unwrap();
            bpm.update_page_data(p0, |p| {
                p.data[96..104].copy_from_slice(format!("post_v_{}", tid).as_bytes());
                p.set_page_lsn(l2);
                Ok(())
            })
            .unwrap();
            wal.commit(tid, l2).unwrap();
        }

        assert!(wal.active_segment_id() > active_seg_at_begin);

        let meta1 = recovery.create_checkpoint_with_meta().unwrap();
        assert_eq!(meta1.min_recovery_lsn, active_begin_lsn);
        assert_eq!(meta1.min_retained_segment_id, active_seg_at_begin);
        assert!(meta1.segments_reclaimed >= 2);
        assert!(!seg0_path.exists());
        assert!(!seg1_path.exists());
        assert!(wal.segment_path_for_id(active_seg_at_begin).exists());
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 150).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.aborted_txns, vec![50]);
        assert_eq!(report.undone_records, 1);

        let p0 = disk.read_page(0).unwrap();
        let p1 = disk.read_page(1).unwrap();
        assert_eq!(&p0.data[64..72], b"comm_v_3");
        assert_eq!(&p0.data[96..104], b"post_v_6");
        assert_eq!(&p1.data[64..78], b"original_p1___");

        let meta2 = recovery.create_checkpoint_with_meta().unwrap();
        assert_eq!(meta2.min_recovery_lsn, meta2.checkpoint_lsn);
        assert_eq!(meta2.min_retained_segment_id, meta2.checkpoint_segment_id);
        assert_eq!(wal.segment_count(), 1);
        assert_eq!(wal.max_user_txn_id(), 50);
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 150).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.aborted_txns, vec![50]);
        assert_eq!(report.redone_records, 0);
        assert_eq!(report.undone_records, 0);
        assert_eq!(wal.max_user_txn_id(), 50);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_crashes_during_checkpoint_steps_and_corrupt_or_missing_segments() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_wal_ckpt_crash_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let last_seg_path;
    let valid_last_seg_len;
    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let (p0, _) = disk.allocate_page(PAGE_TYPE_RAW).unwrap();
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 150).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        for tid in 1..=4u64 {
            let l1 = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
            let l2 = wal
                .append_record(LogRecord::new_update(
                    0,
                    tid,
                    l1,
                    p0,
                    64,
                    format!("val_{:02}", tid).into_bytes(),
                    b"base00".to_vec(),
                ))
                .unwrap();
            bpm.update_page_data(p0, |p| {
                p.data[64..70].copy_from_slice(format!("val_{:02}", tid).as_bytes());
                p.set_page_lsn(l2);
                Ok(())
            })
            .unwrap();
            wal.commit(tid, l2).unwrap();
        }

        let _meta = recovery.create_checkpoint_with_meta().unwrap();
        let active_id = wal.active_segment_id();
        last_seg_path = wal.segment_path_for_id(active_id);
        valid_last_seg_len = std::fs::metadata(&last_seg_path).unwrap().len();

        let partial_ckpt = LogRecord::new_checkpoint(wal.next_lsn(), &[]).serialize();
        let mut f = OpenOptions::new()
            .append(true)
            .open(&last_seg_path)
            .unwrap();
        f.write_all(&partial_ckpt[..20]).unwrap();
        f.sync_all().unwrap();

        let tmp_ckpt_path = wal_path.with_file_name("test.wal.ckpt.tmp");
        std::fs::write(&tmp_ckpt_path, b"interrupted_tmp_ckpt_bytes").unwrap();

        let ghost_old_seg = wal.segment_path_for_id(0);
        std::fs::write(&ghost_old_seg, b"leftover_obsolete_segment_0").unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 150).unwrap());
        assert!(!wal_path.with_file_name("test.wal.ckpt.tmp").exists());
        assert!(!wal.segment_path_for_id(0).exists());
        assert_eq!(
            std::fs::metadata(&last_seg_path).unwrap().len(),
            valid_last_seg_len
        );

        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 8));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
        let report = recovery.recover().unwrap();
        assert!(report.aborted_txns.is_empty());

        let l1 = wal.append_record(LogRecord::new_begin(0, 5)).unwrap();
        let l2 = wal
            .append_record(LogRecord::new_update(
                0,
                5,
                l1,
                0,
                64,
                b"val_05".to_vec(),
                b"val_04".to_vec(),
            ))
            .unwrap();
        wal.commit(5, l2).unwrap();
        let l3 = wal.append_record(LogRecord::new_begin(0, 6)).unwrap();
        wal.commit(6, l3).unwrap();
        assert!(wal.segment_count() >= 2);
    }

    let seg_ids_now = {
        let wal = WalManager::open_with_segment_size(&wal_path, 150).unwrap();
        wal.segment_ids()
    };
    assert!(seg_ids_now.len() >= 2);

    let first_seg_id = seg_ids_now[0];
    let first_seg_path = wal_path.with_file_name(if first_seg_id == 0 {
        "test.wal".to_string()
    } else {
        format!("test.wal.{:08}", first_seg_id)
    });
    let saved_first_seg_bytes = std::fs::read(&first_seg_path).unwrap();

    {
        std::fs::write(
            &first_seg_path,
            &saved_first_seg_bytes[..saved_first_seg_bytes.len() - 5],
        )
        .unwrap();
        let err = WalManager::open_with_segment_size(&wal_path, 150);
        assert!(matches!(err, Err(Error::WalCorrupted(_))));
        std::fs::write(&first_seg_path, &saved_first_seg_bytes).unwrap();
    }

    {
        std::fs::remove_file(&first_seg_path).unwrap();
        let err = WalManager::open_with_segment_size(&wal_path, 150);
        assert!(matches!(err, Err(Error::WalCorrupted(_))));
        std::fs::write(&first_seg_path, &saved_first_seg_bytes).unwrap();
    }

    let ckpt_meta_path = wal_path.with_file_name("test.wal.ckpt");
    let saved_ckpt_bytes = std::fs::read(&ckpt_meta_path).unwrap();
    {
        let mut corrupted_ckpt = saved_ckpt_bytes.clone();
        corrupted_ckpt[12] ^= 0xFF;
        std::fs::write(&ckpt_meta_path, &corrupted_ckpt).unwrap();
        assert!(matches!(
            CheckpointMetadata::deserialize(&corrupted_ckpt),
            Err(Error::WalCorrupted(_))
        ));
        let err = WalManager::open_with_segment_size(&wal_path, 150);
        assert!(matches!(err, Err(Error::WalCorrupted(_))));
        std::fs::write(&ckpt_meta_path, &saved_ckpt_bytes).unwrap();
    }

    assert!(WalManager::open_with_segment_size(&wal_path, 150).is_ok());
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_btree_splits_across_segmented_wal_and_repeated_checkpoints() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_wal_btree_ckpt_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let mut current_root_id = 0u32;
    for cycle in 0..4u32 {
        {
            let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
            let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 512).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let report = recovery.recover().unwrap();
            if cycle > 0 {
                assert_eq!(report.aborted_txns.len(), 1);
            }

            let tree = if cycle == 0 {
                let tid = 1;
                let mut lsn = wal.append_record(LogRecord::new_begin(0, tid)).unwrap();
                let t = BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, tid, &mut lsn)
                    .unwrap();
                wal.commit(tid, lsn).unwrap();
                t
            } else {
                BPlusTree::open(bpm.clone(), Some(wal.clone()), current_root_id, 4).unwrap()
            };

            let comm_tid: TxnId = (cycle as u64) * 10 + 2;
            let mut lsn_c = wal
                .append_record(LogRecord::new_begin(0, comm_tid))
                .unwrap();
            for k_idx in 0..6u32 {
                let k = format!("k_{:02}_{:02}", cycle, k_idx);
                let v = format!("committed_val_{:02}_{:02}", cycle, k_idx);
                tree.insert_logged(comm_tid, &mut lsn_c, k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
            wal.commit(comm_tid, lsn_c).unwrap();
            current_root_id = tree.root_page_id();

            let meta = recovery.create_checkpoint_with_meta().unwrap();
            assert_eq!(meta.min_recovery_lsn, meta.checkpoint_lsn);
            assert_eq!(wal.segment_count(), 1);

            let abort_tid: TxnId = (cycle as u64) * 10 + 3;
            let mut lsn_a = wal
                .append_record(LogRecord::new_begin(0, abort_tid))
                .unwrap();
            for k_idx in 0..4u32 {
                let k = format!("abort_k_{:02}_{:02}", cycle, k_idx);
                let v = format!("abort_val_{:02}_{:02}", cycle, k_idx);
                tree.insert_logged(abort_tid, &mut lsn_a, k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
            wal.flush_up_to(lsn_a).unwrap();
            bpm.flush_all().unwrap();
        }
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(&db_path).unwrap());
        let wal = Arc::new(WalManager::open_with_segment_size(&wal_path, 512).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert_eq!(report.aborted_txns.len(), 1);

        let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), current_root_id, 4).unwrap();
        let all = tree.range_scan(None, None).unwrap();
        assert_eq!(all.len(), 24);
        for (k, _) in &all {
            assert!(k.starts_with(b"k_"));
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
