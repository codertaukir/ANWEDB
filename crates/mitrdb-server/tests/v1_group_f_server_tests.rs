#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::process::Command;
use std::sync::Arc;

use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::Server;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_txn::IsolationLevel;

#[test]
fn test_multi_session_isolation_begin_commit_rollback_and_metrics() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_f_sessions_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 32).unwrap();
    server.start().unwrap();

    server
        .execute_sql("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)")
        .unwrap();
    server
        .execute_sql("CREATE UNIQUE INDEX idx_accounts_name ON accounts (name)")
        .unwrap();

    let s1 = server.create_session();
    let s2 = server.create_session();

    server.execute_in_session(s1, "BEGIN").unwrap();
    server
        .execute_in_session(s1, "INSERT INTO accounts VALUES (1, 'alice', 100)")
        .unwrap();
    server
        .execute_in_session(s1, "INSERT INTO accounts VALUES (2, 'bob', 200)")
        .unwrap();

    let s2_before_commit = server
        .execute_in_session(s2, "SELECT id, name, balance FROM accounts")
        .unwrap();
    assert_eq!(
        s2_before_commit,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "name".to_string(), "balance".to_string(),],
            rows: vec![],
        }
    );

    server.execute_in_session(s1, "COMMIT").unwrap();

    server.execute_in_session(s2, "BEGIN").unwrap();
    server
        .execute_in_session(s2, "UPDATE accounts SET balance = 999 WHERE id = 1")
        .unwrap();
    server
        .execute_in_session(s2, "DELETE FROM accounts WHERE id = 2")
        .unwrap();

    let s1_during_s2 = server
        .execute_in_session(s1, "SELECT id, name, balance FROM accounts ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        s1_during_s2,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "name".to_string(), "balance".to_string(),],
            rows: vec![
                vec![
                    Value::Int64(1),
                    Value::Text("alice".to_string()),
                    Value::Int64(100),
                ],
                vec![
                    Value::Int64(2),
                    Value::Text("bob".to_string()),
                    Value::Int64(200),
                ],
            ],
        }
    );

    server.execute_in_session(s2, "ROLLBACK").unwrap();

    let idx_lookup = server
        .execute_in_session(s1, "SELECT id, balance FROM accounts WHERE name = 'bob'")
        .unwrap();
    assert_eq!(
        idx_lookup,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "balance".to_string()],
            rows: vec![vec![Value::Int64(2), Value::Int64(200)]],
        }
    );

    let metrics = server.metrics_snapshot();
    assert!(metrics.queries_executed >= 10);
    assert!(metrics.txns_begun >= 2);
    assert!(metrics.txns_committed >= 1);
    assert!(metrics.txns_aborted >= 1);
    assert!(metrics.seq_scans >= 1);
    assert!(metrics.index_scans >= 1);

    server.close_session(s1).unwrap();
    server.close_session(s2).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_session_close_after_btree_split_and_error_propagation() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_f_sess_close_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 64).unwrap();
        server
            .execute_sql("CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)")
            .unwrap();
        server
            .execute_sql("CREATE UNIQUE INDEX idx_logs_msg ON logs (msg)")
            .unwrap();
        server
            .execute_sql("INSERT INTO logs VALUES (1, 'baseline_entry')")
            .unwrap();

        let s_split = server.create_session();
        server.execute_in_session(s_split, "BEGIN").unwrap();
        let pad = "Z".repeat(220);
        for i in 2..=35 {
            let sql = format!("INSERT INTO logs VALUES ({}, 'msg_{:03}_{}')", i, i, pad);
            server.execute_in_session(s_split, &sql).unwrap();
        }

        server.close_session(s_split).unwrap();

        let rows = server
            .execute_sql("SELECT id, msg FROM logs ORDER BY id ASC")
            .unwrap();
        assert_eq!(
            rows,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "msg".to_string()],
                rows: vec![vec![
                    Value::Int64(1),
                    Value::Text("baseline_entry".to_string()),
                ]],
            }
        );
    }

    {
        let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 64).unwrap();
        let rows = server
            .execute_sql("SELECT id, msg FROM logs WHERE msg = 'baseline_entry'")
            .unwrap();
        assert_eq!(
            rows,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "msg".to_string()],
                rows: vec![vec![
                    Value::Int64(1),
                    Value::Text("baseline_entry".to_string()),
                ]],
            }
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_server_restart_durability_with_splits_and_uncommitted_txns() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_f_restart_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 64).unwrap();
        server
            .execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT, qty INT)")
            .unwrap();
        server
            .execute_sql("CREATE UNIQUE INDEX idx_orders_sku ON orders (sku)")
            .unwrap();

        for i in 1..=25 {
            let sql = format!(
                "INSERT INTO orders VALUES ({}, 'sku_{:04}_{}', {})",
                i,
                i,
                "P".repeat(120),
                i * 10
            );
            server.execute_sql(&sql).unwrap();
        }

        server
            .execute_sql("UPDATE orders SET qty = 777 WHERE id = 5")
            .unwrap();
        server
            .execute_sql("DELETE FROM orders WHERE id = 10")
            .unwrap();
        server.checkpoint().unwrap();

        server
            .execute_sql("INSERT INTO orders VALUES (26, 'sku_post_ckpt', 260)")
            .unwrap();

        let s_uncommitted = server.create_session();
        server.execute_in_session(s_uncommitted, "BEGIN").unwrap();
        server
            .execute_in_session(
                s_uncommitted,
                "INSERT INTO orders VALUES (99, 'sku_uncommitted', 990)",
            )
            .unwrap();
        server
            .execute_in_session(s_uncommitted, "UPDATE orders SET qty = 0 WHERE id = 5")
            .unwrap();
        server
            .execute_in_session(s_uncommitted, "DELETE FROM orders WHERE id = 1")
            .unwrap();
        server.checkpoint().unwrap();
    }

    {
        let (server, report) = Server::open(db_path.as_path(), wal_path.as_path(), 64).unwrap();
        assert!(!report.aborted_txns.is_empty());

        let r5 = server
            .execute_sql("SELECT id, qty FROM orders WHERE id = 5")
            .unwrap();
        assert_eq!(
            r5,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "qty".to_string()],
                rows: vec![vec![Value::Int64(5), Value::Int64(777)]],
            }
        );

        let r1 = server
            .execute_sql("SELECT id, qty FROM orders WHERE id = 1")
            .unwrap();
        assert_eq!(
            r1,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "qty".to_string()],
                rows: vec![vec![Value::Int64(1), Value::Int64(10)]],
            }
        );

        let r10 = server
            .execute_sql("SELECT id FROM orders WHERE id = 10")
            .unwrap();
        assert_eq!(
            r10,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let r26 = server
            .execute_sql("SELECT id, qty FROM orders WHERE sku = 'sku_post_ckpt'")
            .unwrap();
        assert_eq!(
            r26,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "qty".to_string()],
                rows: vec![vec![Value::Int64(26), Value::Int64(260)]],
            }
        );

        let r99 = server
            .execute_sql("SELECT id FROM orders WHERE sku = 'sku_uncommitted'")
            .unwrap();
        assert_eq!(
            r99,
            ExecutionResult::Rows {
                columns: vec!["id".to_string()],
                rows: vec![],
            }
        );

        let all_rows = server.execute_sql("SELECT id FROM orders").unwrap();
        match all_rows {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 25),
            other => panic!("Expected 25 rows, got {:?}", other),
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_serializable_key_locking_and_documented_v1_boundary() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_f_serial_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 32).unwrap();
    server
        .execute_sql("CREATE TABLE kv (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    server
        .execute_sql("INSERT INTO kv VALUES (1, 'v1')")
        .unwrap();

    let schema = server.catalog().get_table_schema("kv").unwrap().unwrap();
    let tree = server.catalog().table_tree(schema.table_id).unwrap();
    let txn_mgr = server.txn_manager();

    let t_ser1 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();
    let t_ser2 = txn_mgr
        .begin_with_isolation(IsolationLevel::Serializable)
        .unwrap();

    let k1 = Value::Int64(1).to_key_bytes();
    assert!(txn_mgr.get(t_ser1, &tree, &k1).unwrap().is_some());
    assert!(txn_mgr.get(t_ser2, &tree, &k1).unwrap().is_some());

    let conflict = txn_mgr.put(t_ser1, &tree, &k1, b"updated");
    assert!(matches!(conflict, Err(Error::LockTimeout)));

    txn_mgr.abort(t_ser1).unwrap();
    txn_mgr.commit(t_ser2).unwrap();

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_bounded_buffer_pool_stress_and_zero_pin_leak() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_f_bp_stress_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 8).unwrap();
    server
        .execute_sql("CREATE TABLE stress (id INT PRIMARY KEY, payload TEXT)")
        .unwrap();

    for i in 1..=250 {
        let sql = format!("INSERT INTO stress VALUES ({}, 'val_{:04}')", i, i);
        server.execute_sql(&sql).unwrap();
    }
    for i in (1..=250).step_by(2) {
        let sql = format!(
            "UPDATE stress SET payload = 'upd_{:04}' WHERE id = {}",
            i, i
        );
        server.execute_sql(&sql).unwrap();
    }
    for i in 1..=250 {
        let sql = format!("SELECT payload FROM stress WHERE id = {}", i);
        let res = server.execute_sql(&sql).unwrap();
        match res {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            other => panic!("Unexpected result: {:?}", other),
        }
    }
    for i in (2..=250).step_by(4) {
        let sql = format!("DELETE FROM stress WHERE id = {}", i);
        server.execute_sql(&sql).unwrap();
    }

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let bpm = BufferPoolManager::new(disk, 8);
    for (_, _, pin_count, _, _, _) in bpm.inspect_frames() {
        assert_eq!(pin_count, 0);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_cli_binary_batch_execution_and_persistence() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_group_f_cli_{}", std::process::id()));
    let db_path = temp_dir.join("cli.db");
    let wal_path = temp_dir.join("cli.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let out1 = Command::new(env!("CARGO"))
        .args([
            "run",
            "--quiet",
            "-p",
            "mitrdb-cli",
            "--",
            "--db",
            db_path.to_str().unwrap(),
            "--wal",
            wal_path.to_str().unwrap(),
            "--sql",
            "CREATE TABLE cli_t (id INT PRIMARY KEY, name TEXT); INSERT INTO cli_t VALUES (1, 'from_cli'); SELECT id, name FROM cli_t;",
        ])
        .output()
        .unwrap();
    assert!(
        out1.status.success(),
        "CLI run 1 failed: {}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&out1.stdout);
    assert!(stdout1.contains("Affected(1)"));
    assert!(stdout1.contains("from_cli"));

    let out2 = Command::new(env!("CARGO"))
        .args([
            "run",
            "--quiet",
            "-p",
            "mitrdb-cli",
            "--",
            "--db",
            db_path.to_str().unwrap(),
            "--wal",
            wal_path.to_str().unwrap(),
            "--sql",
            "SELECT id, name FROM cli_t WHERE id = 1;",
        ])
        .output()
        .unwrap();
    assert!(
        out2.status.success(),
        "CLI run 2 failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("from_cli"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
