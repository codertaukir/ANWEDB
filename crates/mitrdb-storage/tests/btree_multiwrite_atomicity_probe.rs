#![cfg(feature = "fault-injection")]
use std::sync::Arc;
use mitrdb_storage::btree::BPlusTree;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

#[test]
fn test_btree_multiwrite_atomicity_on_fault() {
    let dir = std::env::temp_dir().join(format!("mitrdb_multiwrite_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    for fail_at in 0..7 {
        let dir_case = dir.join(format!("case_{}", fail_at));
        let _ = std::fs::remove_dir_all(&dir_case);
        std::fs::create_dir_all(&dir_case).unwrap();

        let db_c = dir_case.join("c.db");
        let wal_c = dir_case.join("c.wal");
        let disk_c = Arc::new(DiskManager::open_or_create(&db_c).unwrap());
        let bpm_c = Arc::new(BufferPoolManager::new(disk_c, 32));
        let wal_c = Arc::new(WalManager::open_or_create(&wal_c).unwrap());
        let tree_c = BPlusTree::create(bpm_c.clone(), Some(wal_c.clone()), 4).unwrap();

        let mut p_lsn = 0;
        for i in [10, 20, 30, 40, 50, 60, 70, 80] {
            tree_c.insert_or_replace_logged(100, &mut p_lsn, &key(i), b"val").unwrap();
        }

        bpm_c.set_fail_page_op_after(fail_at);
        let res = tree_c.delete_logged(101, &mut p_lsn, &key(20));
        bpm_c.set_fail_page_op_after(-1);

        let k20_status = match tree_c.get(&key(20)) {
            Ok(Some(_)) => "PRESENT",
            Ok(None) => "MISSING_DELETED",
            Err(_) => "GET_ERR",
        };

        if res.is_err() {
            // Failure par koi unaffected key drop nahi honi chahiye
            for i in [10, 30, 40, 50, 60, 70, 80] {
                assert!(
                    matches!(tree_c.get(&key(i)), Ok(Some(_))),
                    "Fail-at {}: unaffected key {} became inaccessible",
                    fail_at,
                    i
                );
            }
            // Atomicity invariant: Agar operation fail hua to key mutate/delete nahi honi chahiye
            assert_eq!(
                k20_status, "PRESENT",
                "Fail-at {}: partial mutation detected; page modified despite operation failure",
                fail_at
            );
        } else {
            assert_eq!(k20_status, "MISSING_DELETED");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
