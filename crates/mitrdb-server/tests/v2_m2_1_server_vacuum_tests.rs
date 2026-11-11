#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::sync::{Arc, Barrier};
use std::thread;

use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::Server;

#[test]
fn test_server_vacuum_sql_lifecycle_and_metrics() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_v2_server_vacuum_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let (server, _) = Server::open(db_path.as_path(), wal_path.as_path(), 32).unwrap();
    server.start().unwrap();

    server
        .execute_sql("CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT)")
        .unwrap();
    server
        .execute_sql("CREATE UNIQUE INDEX idx_items_label ON items (label)")
        .unwrap();

    server
        .execute_sql("INSERT INTO items VALUES (1, 'alpha', 10)")
        .unwrap();
    server
        .execute_sql("INSERT INTO items VALUES (2, 'beta', 20)")
        .unwrap();
    server
        .execute_sql("UPDATE items SET qty = 15 WHERE id = 1")
        .unwrap();
    server
        .execute_sql("DELETE FROM items WHERE id = 2")
        .unwrap();

    // Open a session with an active transaction before further updates
    let s_snap = server.create_session();
    server.execute_in_session(s_snap, "BEGIN").unwrap();
    let snap_initial = server
        .execute_in_session(s_snap, "SELECT id, qty FROM items ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        snap_initial,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "qty".to_string()],
            rows: vec![vec![Value::Int64(1), Value::Int64(15)]],
        }
    );

    // Perform more updates outside s_snap
    server
        .execute_sql("UPDATE items SET qty = 99 WHERE id = 1")
        .unwrap();

    let m_before = server.metrics_snapshot();
    let rep1 = server.vacuum();
    assert!(rep1.versions_pruned >= 1);

    // s_snap still sees qty = 15
    let snap_after_vacuum = server
        .execute_in_session(s_snap, "SELECT id, qty FROM items ORDER BY id ASC")
        .unwrap();
    assert_eq!(snap_initial, snap_after_vacuum);

    // Commit s_snap and run final vacuum
    server.execute_in_session(s_snap, "COMMIT").unwrap();
    server.close_session(s_snap).unwrap();

    let rep2 = server.vacuum();
    assert!(rep2.versions_pruned >= 1);
    assert_eq!(server.txn_manager().mvcc_key_count(), 0);
    assert_eq!(server.txn_manager().mvcc_version_count(), 0);
    assert_eq!(server.txn_manager().tracked_txn_count(), 0);

    // Verify table and secondary index queries after full vacuum
    let latest = server
        .execute_sql("SELECT id, qty FROM items WHERE label = 'alpha'")
        .unwrap();
    assert_eq!(
        latest,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "qty".to_string()],
            rows: vec![vec![Value::Int64(1), Value::Int64(99)]],
        }
    );

    let m_after = server.metrics_snapshot();
    assert!(m_after.vacuum_runs >= m_before.vacuum_runs + 2);
    assert!(m_after.mvcc_versions_pruned > m_before.mvcc_versions_pruned);
    assert!(m_after.mvcc_txns_reclaimed > m_before.mvcc_txns_reclaimed);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_concurrent_drop_table_and_vacuum_barrier_synchronized() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_v2_server_drop_vacuum_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let (server_raw, _) = Server::open(db_path.as_path(), wal_path.as_path(), 64).unwrap();
    server_raw.start().unwrap();
    let server = Arc::new(server_raw);

    server
        .execute_sql("CREATE TABLE perm_items (id INT PRIMARY KEY, v INT)")
        .unwrap();
    server
        .execute_sql("INSERT INTO perm_items VALUES (1, 100)")
        .unwrap();

    // Part A: 15 deterministic barrier-synchronized rounds of concurrent DROP TABLE + vacuum()
    for round in 0..15u32 {
        let tbl = format!("ephemeral_{}", round);
        let idx = format!("idx_ephemeral_{}", round);
        server
            .execute_sql(&format!(
                "CREATE TABLE {} (id INT PRIMARY KEY, tag TEXT, score INT)",
                tbl
            ))
            .unwrap();
        server
            .execute_sql(&format!("CREATE UNIQUE INDEX {} ON {} (tag)", idx, tbl))
            .unwrap();

        // Populate committed multi-version chains and tombstones on both table and index trees
        server
            .execute_sql(&format!("INSERT INTO {} VALUES (1, 'a', 10)", tbl))
            .unwrap();
        server
            .execute_sql(&format!("INSERT INTO {} VALUES (2, 'b', 20)", tbl))
            .unwrap();
        server
            .execute_sql(&format!("UPDATE {} SET score = 15 WHERE id = 1", tbl))
            .unwrap();
        server
            .execute_sql(&format!("DELETE FROM {} WHERE id = 2", tbl))
            .unwrap();
        server
            .execute_sql(&format!(
                "UPDATE perm_items SET v = {} WHERE id = 1",
                round + 200
            ))
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let srv_drop = server.clone();
        let bar_drop = barrier.clone();
        let drop_sql = format!("DROP TABLE {}", tbl);
        let h_drop = thread::spawn(move || {
            let s = srv_drop.create_session();
            bar_drop.wait();
            let res = srv_drop.execute_in_session(s, &drop_sql);
            srv_drop.close_session(s).unwrap();
            res
        });

        let srv_vac = server.clone();
        let bar_vac = barrier.clone();
        let h_vac = thread::spawn(move || {
            bar_vac.wait();
            srv_vac.vacuum()
        });

        let drop_res = h_drop.join().unwrap();
        let _vac_rep = h_vac.join().unwrap();
        assert_eq!(drop_res.unwrap(), ExecutionResult::Ok);

        // Final vacuum at the end of the round must leave zero stale MVCC keys, versions, or txns
        let _ = server.vacuum();
        assert_eq!(server.txn_manager().mvcc_key_count(), 0);
        assert_eq!(server.txn_manager().mvcc_version_count(), 0);
        assert_eq!(server.txn_manager().tracked_txn_count(), 0);
    }

    // Part B: Barrier-synchronized DROP TABLE + vacuum() while an active session holds a reference
    server
        .execute_sql("CREATE TABLE guarded_tbl (id INT PRIMARY KEY, label TEXT)")
        .unwrap();
    server
        .execute_sql("CREATE UNIQUE INDEX idx_guarded_label ON guarded_tbl (label)")
        .unwrap();
    server
        .execute_sql("INSERT INTO guarded_tbl VALUES (1, 'locked')")
        .unwrap();

    let s_active = server.create_session();
    server.execute_in_session(s_active, "BEGIN").unwrap();
    let rows = server
        .execute_in_session(
            s_active,
            "SELECT id FROM guarded_tbl WHERE label = 'locked'",
        )
        .unwrap();
    assert_eq!(
        rows,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::Int64(1)]],
        }
    );

    let barrier = Arc::new(Barrier::new(2));
    let srv_drop = server.clone();
    let bar_drop = barrier.clone();
    let h_drop = thread::spawn(move || {
        let s = srv_drop.create_session();
        bar_drop.wait();
        let res = srv_drop.execute_in_session(s, "DROP TABLE guarded_tbl");
        srv_drop.close_session(s).unwrap();
        res
    });

    let srv_vac = server.clone();
    let bar_vac = barrier.clone();
    let h_vac = thread::spawn(move || {
        bar_vac.wait();
        srv_vac.vacuum()
    });

    let drop_err = h_drop.join().unwrap();
    let _ = h_vac.join().unwrap();
    assert!(matches!(drop_err, Err(Error::WriteConflict(_))));

    // Active session must still be able to read guarded_tbl intact
    let rows_after = server
        .execute_in_session(s_active, "SELECT id FROM guarded_tbl WHERE id = 1")
        .unwrap();
    assert_eq!(rows, rows_after);

    server.execute_in_session(s_active, "COMMIT").unwrap();
    server.close_session(s_active).unwrap();

    // Now DROP TABLE and vacuum succeed and leave zero stale state
    assert_eq!(
        server.execute_sql("DROP TABLE guarded_tbl").unwrap(),
        ExecutionResult::Ok
    );
    let _ = server.vacuum();
    assert_eq!(server.txn_manager().mvcc_key_count(), 0);
    assert_eq!(server.txn_manager().mvcc_version_count(), 0);
    assert_eq!(server.txn_manager().tracked_txn_count(), 0);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
