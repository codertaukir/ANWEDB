use std::sync::{Arc, Barrier, Mutex};

// These tests share GLOBAL_METRICS, so serialize them to avoid counter races.
static BACKUP_TEST_LOCK: Mutex<()> = Mutex::new(());
use std::thread;

use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::Server;

#[test]
fn test_server_hot_online_backup_restore_and_restart_lifecycle() {
    let _test_guard = BACKUP_TEST_LOCK.lock().unwrap();
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_srv_bkp_life_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("hot_backup_1");
    let dst_dir = temp_dir.join("restored_db_1");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let pre_backup_max_txn;
    {
        let (server, _) = Server::open_with_segment_size(&db_path, &wal_path, 32, 512).unwrap();
        server.start().unwrap();

        server
            .execute_sql("CREATE TABLE accounts (id INT PRIMARY KEY, code TEXT, balance INT)")
            .unwrap();
        server
            .execute_sql("CREATE UNIQUE INDEX idx_accounts_code ON accounts (code)")
            .unwrap();

        for i in 1..=25i64 {
            server
                .execute_sql(&format!(
                    "INSERT INTO accounts VALUES ({}, 'code_{:03}', {})",
                    i,
                    i,
                    i * 1000
                ))
                .unwrap();
        }

        for i in 1..=5i64 {
            server
                .execute_sql(&format!(
                    "UPDATE accounts SET balance = {} WHERE id = {}",
                    i * 1000 + 500,
                    i
                ))
                .unwrap();
        }

        server
            .execute_sql("DELETE FROM accounts WHERE id = 11")
            .unwrap();
        server
            .execute_sql("DELETE FROM accounts WHERE id = 12")
            .unwrap();

        let vac = server.vacuum();
        assert!(vac.versions_pruned >= 1);

        let s_open = server.create_session();
        server.execute_in_session(s_open, "BEGIN").unwrap();
        server
            .execute_in_session(
                s_open,
                "INSERT INTO accounts VALUES (999, 'code_uncommitted', 999999)",
            )
            .unwrap();

        let m_before = server.metrics_snapshot();
        let manifest = server.backup(&bkp_dir).unwrap();
        let m_after = server.metrics_snapshot();
        assert_eq!(m_after.backups_created, m_before.backups_created + 1);
        assert!(!manifest.wal_segments.is_empty());
        pre_backup_max_txn = manifest.max_user_txn_id;

        let validated = Server::validate_backup(&bkp_dir).unwrap();
        assert_eq!(validated, manifest);

        server.execute_in_session(s_open, "ROLLBACK").unwrap();
        server.close_session(s_open).unwrap();

        for i in 26..=30i64 {
            server
                .execute_sql(&format!(
                    "INSERT INTO accounts VALUES ({}, 'code_{:03}', {})",
                    i,
                    i,
                    i * 1000
                ))
                .unwrap();
        }
    }

    let m_pre_restore = Server::new().metrics_snapshot();
    let restore_rep = Server::restore(&bkp_dir, &dst_dir).unwrap();
    let m_post_restore = Server::new().metrics_snapshot();
    assert_eq!(
        m_post_restore.backups_restored,
        m_pre_restore.backups_restored + 1
    );
    assert!(restore_rep.max_user_txn_id >= pre_backup_max_txn);

    {
        let (restored_srv, _) = Server::open_with_segment_size(
            dst_dir.join("test.db"),
            dst_dir.join("test.wal"),
            32,
            512,
        )
        .unwrap();

        let r1 = restored_srv
            .execute_sql("SELECT id, balance FROM accounts WHERE id = 1")
            .unwrap();
        assert_eq!(
            r1,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "balance".to_string()],
                rows: vec![vec![Value::Int64(1), Value::Int64(1500)]],
            }
        );

        let r11 = restored_srv
            .execute_sql("SELECT id FROM accounts WHERE id = 11")
            .unwrap();
        assert_eq!(
            r11,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let r999 = restored_srv
            .execute_sql("SELECT id FROM accounts WHERE id = 999")
            .unwrap();
        assert_eq!(
            r999,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let r25 = restored_srv
            .execute_sql("SELECT id, balance FROM accounts WHERE code = 'code_025'")
            .unwrap();
        assert_eq!(
            r25,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "balance".to_string()],
                rows: vec![vec![Value::Int64(25), Value::Int64(25000)]],
            }
        );

        let r26 = restored_srv
            .execute_sql("SELECT id FROM accounts WHERE id = 26")
            .unwrap();
        assert_eq!(
            r26,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let new_tid = restored_srv.txn_manager().begin().unwrap();
        assert!(new_tid > pre_backup_max_txn);
        restored_srv.txn_manager().commit(new_tid).unwrap();

        restored_srv
            .execute_sql("INSERT INTO accounts VALUES (50, 'code_050', 50000)")
            .unwrap();
        let r50 = restored_srv
            .execute_sql("SELECT id, balance FROM accounts WHERE code = 'code_050'")
            .unwrap();
        assert_eq!(
            r50,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "balance".to_string()],
                rows: vec![vec![Value::Int64(50), Value::Int64(50000)]],
            }
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_server_concurrent_reads_writes_checkpoint_and_backup_barrier_synchronized() {
    let _test_guard = BACKUP_TEST_LOCK.lock().unwrap();
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_srv_bkp_conc_{}", std::process::id()));
    let src_dir = temp_dir.join("src_db");
    let bkp_dir = temp_dir.join("conc_backup");
    let dst_dir = temp_dir.join("conc_restored");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let db_path = src_dir.join("test.db");
    let wal_path = src_dir.join("test.wal");

    let (server, _) = Server::open_with_segment_size(&db_path, &wal_path, 64, 512).unwrap();
    let server = Arc::new(server);

    server
        .execute_sql("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, val INT)")
        .unwrap();
    server
        .execute_sql("CREATE TABLE audit_log (id INT PRIMARY KEY, note TEXT)")
        .unwrap();

    for i in 1..=20i64 {
        server
            .execute_sql(&format!(
                "INSERT INTO items VALUES ({}, 'item_{:03}', {})",
                i,
                i,
                i * 10
            ))
            .unwrap();
    }
    server.checkpoint().unwrap();

    let barrier = Arc::new(Barrier::new(3));

    let srv_reader = server.clone();
    let bar_reader = barrier.clone();
    let reader_handle = thread::spawn(move || {
        bar_reader.wait();
        for i in 1..=20i64 {
            let res = srv_reader
                .execute_sql(&format!("SELECT id, val FROM items WHERE id = {}", i))
                .unwrap();
            assert_eq!(
                res,
                ExecutionResult::Rows {
                    columns: vec!["id".to_string(), "val".to_string()],
                    rows: vec![vec![Value::Int64(i), Value::Int64(i * 10)]],
                }
            );
            if i % 7 == 0 {
                srv_reader.checkpoint().unwrap();
            }
        }
    });

    let srv_writer = server.clone();
    let bar_writer = barrier.clone();
    let writer_handle = thread::spawn(move || {
        bar_writer.wait();
        for i in 1..=10i64 {
            let s = srv_writer.create_session();
            srv_writer
                .execute_in_session(
                    s,
                    &format!("INSERT INTO audit_log VALUES ({}, 'log_{:03}')", i, i),
                )
                .unwrap();
            srv_writer.close_session(s).unwrap();
        }
    });

    let srv_bkp = server.clone();
    let bar_bkp = barrier.clone();
    let bkp_path = bkp_dir.clone();
    let bkp_handle = thread::spawn(move || {
        bar_bkp.wait();
        srv_bkp.backup(&bkp_path).unwrap()
    });

    reader_handle.join().unwrap();
    writer_handle.join().unwrap();
    let manifest = bkp_handle.join().unwrap();
    assert!(!manifest.wal_segments.is_empty());

    let _ = Server::restore(&bkp_dir, &dst_dir).unwrap();

    let (restored, _) =
        Server::open_with_segment_size(dst_dir.join("test.db"), dst_dir.join("test.wal"), 64, 512)
            .unwrap();

    let all_items = restored
        .execute_sql("SELECT id FROM items ORDER BY id ASC")
        .unwrap();
    match all_items {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 20),
        other => panic!("Unexpected query result: {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
