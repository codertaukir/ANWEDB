#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::Server;

#[test]
fn test_server_segmented_wal_checkpoint_vacuum_and_restart_lifecycle() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_srv_wal_seg_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let last_committed_txn_id;
    {
        let (server, _) =
            Server::open_with_segment_size(db_path.as_path(), wal_path.as_path(), 32, 512).unwrap();
        server.start().unwrap();

        server
            .execute_sql("CREATE TABLE ledger (id INT PRIMARY KEY, acct TEXT, amount INT)")
            .unwrap();
        server
            .execute_sql("CREATE UNIQUE INDEX idx_ledger_acct ON ledger (acct)")
            .unwrap();

        for i in 1..=20i64 {
            server
                .execute_sql(&format!(
                    "INSERT INTO ledger VALUES ({}, 'acct_{:03}', {})",
                    i,
                    i,
                    i * 100
                ))
                .unwrap();
        }

        for i in 1..=10i64 {
            server
                .execute_sql(&format!(
                    "UPDATE ledger SET amount = {} WHERE id = {}",
                    i * 100 + 50,
                    i
                ))
                .unwrap();
        }

        let wal = server.wal_manager().unwrap();
        assert!(wal.segment_count() > 3);

        let vac1 = server.vacuum();
        assert!(vac1.versions_pruned >= 10);

        let meta1 = server.checkpoint_with_meta().unwrap().unwrap();
        assert!(meta1.segments_reclaimed >= 2);
        assert_eq!(meta1.min_recovery_lsn, meta1.checkpoint_lsn);
        assert_eq!(wal.segment_count(), 1);
        last_committed_txn_id = meta1.max_user_txn_id;
        assert!(last_committed_txn_id >= 30);

        for i in 21..=28i64 {
            server
                .execute_sql(&format!(
                    "INSERT INTO ledger VALUES ({}, 'acct_{:03}', {})",
                    i,
                    i,
                    i * 100
                ))
                .unwrap();
        }

        let s_open = server.create_session();
        server.execute_in_session(s_open, "BEGIN").unwrap();
        server
            .execute_in_session(
                s_open,
                "INSERT INTO ledger VALUES (999, 'acct_uncommitted', 99900)",
            )
            .unwrap();
        server
            .execute_in_session(s_open, "UPDATE ledger SET amount = -1 WHERE id = 1")
            .unwrap();

        let meta2 = server.checkpoint_with_meta().unwrap().unwrap();
        assert!(meta2.min_recovery_lsn < meta2.checkpoint_lsn);

        let s_abort = server.create_session();
        server.execute_in_session(s_abort, "BEGIN").unwrap();
        server
            .execute_in_session(s_abort, "UPDATE ledger SET amount = 777777 WHERE id = 2")
            .unwrap();
        server.execute_in_session(s_abort, "ROLLBACK").unwrap();
        server.close_session(s_abort).unwrap();
    }

    {
        let (server, report) =
            Server::open_with_segment_size(db_path.as_path(), wal_path.as_path(), 32, 512).unwrap();
        assert_eq!(report.aborted_txns.len(), 2);
        assert!(report.undone_records >= 1);

        let next_tid = server.txn_manager().begin().unwrap();
        assert!(next_tid > last_committed_txn_id);
        server.txn_manager().commit(next_tid).unwrap();

        let r1 = server
            .execute_sql("SELECT id, amount FROM ledger WHERE id = 1")
            .unwrap();
        assert_eq!(
            r1,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "amount".to_string()],
                rows: vec![vec![Value::Int64(1), Value::Int64(150)]],
            }
        );

        let r2 = server
            .execute_sql("SELECT id, amount FROM ledger WHERE id = 2")
            .unwrap();
        assert_eq!(
            r2,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "amount".to_string()],
                rows: vec![vec![Value::Int64(2), Value::Int64(250)]],
            }
        );

        let r999 = server
            .execute_sql("SELECT id FROM ledger WHERE acct = 'acct_uncommitted'")
            .unwrap();
        assert_eq!(
            r999,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let r28 = server
            .execute_sql("SELECT id, amount FROM ledger WHERE acct = 'acct_028'")
            .unwrap();
        assert_eq!(
            r28,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "amount".to_string()],
                rows: vec![vec![Value::Int64(28), Value::Int64(2800)]],
            }
        );

        server.execute_sql("CHECKPOINT").unwrap();
        let wal = server.wal_manager().unwrap();
        assert_eq!(wal.segment_count(), 1);

        let m = server.metrics_snapshot();
        assert!(m.wal_rotations >= 5);
        assert!(m.wal_segments_reclaimed >= 2);
        assert!(m.checkpoints_created >= 3);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
