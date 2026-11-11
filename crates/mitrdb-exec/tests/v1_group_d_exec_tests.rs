#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::sync::Arc;

use mitrdb_common::Error;
use mitrdb_exec::{ExecutionResult, Executor, Tuple};
use mitrdb_query::{Catalog, QueryEngine, Value};
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

fn run_sql(
    exec: &Executor,
    engine: &QueryEngine,
    catalog: &Catalog,
    txn_mgr: &TxnManager,
    active_txn: &mut Option<u64>,
    sql: &str,
) -> mitrdb_common::Result<ExecutionResult> {
    let plan = engine.parse_and_plan(sql)?;
    exec.execute_plan(plan, catalog, txn_mgr, active_txn)
}

#[test]
fn test_tuple_serialization_and_corruption() {
    let values = vec![
        Value::Null,
        Value::Int64(-9876543210),
        Value::Text("mitrdb tuple text".to_string()),
        Value::Bool(true),
        Value::Bool(false),
        Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    ];
    let bytes = Tuple::serialize(&values);
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(decoded, values);

    assert!(matches!(
        Tuple::deserialize(&[]),
        Err(Error::PageCorrupted(_))
    ));
    assert!(matches!(
        Tuple::deserialize(&[1, 0, 99]),
        Err(Error::PageCorrupted(_))
    ));
}

#[test]
fn test_multi_row_update_pk_shifts_and_unique_index_swaps() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_d_update_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();
    let mut session_txn = None;

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "CREATE UNIQUE INDEX idx_items_code ON items (code)",
    )
    .unwrap();

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO items VALUES (1, 'alpha')",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO items VALUES (2, 'beta')",
    )
    .unwrap();

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE items SET id = 2 WHERE id = 1",
    )
    .expect_err("Updating PK 1 -> 2 when 2 exists outside batch must fail");

    let schema = catalog.get_table_schema("items").unwrap().unwrap();
    let tree = catalog.table_tree(schema.table_id).unwrap();
    let idx_meta = &catalog.get_table_indexes(schema.table_id)[0];
    let idx_tree = catalog.index_tree(idx_meta.index_id).unwrap();

    let tid_swap = txn_mgr.begin().unwrap();
    let k1 = Value::Int64(1).to_key_bytes();
    let k2 = Value::Int64(2).to_key_bytes();
    let k3 = Value::Int64(3).to_key_bytes();
    txn_mgr.delete(tid_swap, &tree, &k1).unwrap();
    txn_mgr.delete(tid_swap, &tree, &k2).unwrap();
    txn_mgr
        .delete(
            tid_swap,
            &idx_tree,
            &Value::Text("alpha".to_string()).to_key_bytes(),
        )
        .unwrap();
    txn_mgr
        .delete(
            tid_swap,
            &idx_tree,
            &Value::Text("beta".to_string()).to_key_bytes(),
        )
        .unwrap();
    txn_mgr
        .put(
            tid_swap,
            &tree,
            &k2,
            &Tuple::serialize(&[Value::Int64(2), Value::Text("beta".to_string())]),
        )
        .unwrap();
    txn_mgr
        .put(
            tid_swap,
            &tree,
            &k3,
            &Tuple::serialize(&[Value::Int64(3), Value::Text("alpha".to_string())]),
        )
        .unwrap();
    txn_mgr
        .put(
            tid_swap,
            &idx_tree,
            &Value::Text("beta".to_string()).to_key_bytes(),
            &k2,
        )
        .unwrap();
    txn_mgr
        .put(
            tid_swap,
            &idx_tree,
            &Value::Text("alpha".to_string()).to_key_bytes(),
            &k3,
        )
        .unwrap();
    txn_mgr.commit(tid_swap).unwrap();

    let upd_res = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE items SET code = 'gamma' WHERE id = 2",
    )
    .unwrap();
    assert_eq!(upd_res, ExecutionResult::Affected(1));

    let old_idx_lookup = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "SELECT id, code FROM items WHERE code = 'beta'",
    )
    .unwrap();
    assert_eq!(
        old_idx_lookup,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "code".to_string()],
            rows: vec![],
        }
    );

    let new_idx_lookup = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "SELECT id, code FROM items WHERE code = 'gamma'",
    )
    .unwrap();
    assert_eq!(
        new_idx_lookup,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "code".to_string()],
            rows: vec![vec![Value::Int64(2), Value::Text("gamma".to_string())]],
        }
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_update_pk_and_unique_index_conflict_validation_and_rollback() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_d_conflicts_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();
    let mut session_txn = None;

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, role TEXT)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "CREATE UNIQUE INDEX idx_users_email ON users (email)",
    )
    .unwrap();

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO users VALUES (1, 'u1@example.com', 'member')",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO users VALUES (2, 'u2@example.com', 'member')",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO users VALUES (3, 'u3@example.com', 'admin')",
    )
    .unwrap();

    let dup_insert = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "INSERT INTO users VALUES (4, 'u1@example.com', 'member')",
    );
    assert!(matches!(dup_insert, Err(Error::WriteConflict(_))));

    let batch_dup_pk = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET id = 99 WHERE role = 'member'",
    );
    assert!(matches!(batch_dup_pk, Err(Error::WriteConflict(_))));

    let existing_pk_conflict = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET id = 3 WHERE id = 1",
    );
    assert!(matches!(existing_pk_conflict, Err(Error::WriteConflict(_))));

    let batch_dup_unique = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET email = 'same@example.com' WHERE role = 'member'",
    );
    assert!(matches!(batch_dup_unique, Err(Error::WriteConflict(_))));

    let existing_unique_conflict = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET email = 'u3@example.com' WHERE id = 1",
    );
    assert!(matches!(
        existing_unique_conflict,
        Err(Error::WriteConflict(_))
    ));

    let multi_row_non_unique_update = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET role = 'staff' WHERE role = 'member'",
    )
    .unwrap();
    assert_eq!(multi_row_non_unique_update, ExecutionResult::Affected(2));

    let staff_rows = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "SELECT id, email FROM users WHERE role = 'staff' ORDER BY id ASC",
    )
    .unwrap();
    assert_eq!(
        staff_rows,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "email".to_string()],
            rows: vec![
                vec![Value::Int64(1), Value::Text("u1@example.com".to_string())],
                vec![Value::Int64(2), Value::Text("u2@example.com".to_string())],
            ],
        }
    );

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "BEGIN",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET email = 'temp1@example.com' WHERE id = 1",
    )
    .unwrap();
    let fail_in_txn = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "UPDATE users SET email = 'u3@example.com' WHERE id = 2",
    );
    assert!(matches!(fail_in_txn, Err(Error::WriteConflict(_))));
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "ROLLBACK",
    )
    .unwrap();

    let after_rollback = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut session_txn,
        "SELECT id, email FROM users WHERE email = 'u1@example.com'",
    )
    .unwrap();
    assert_eq!(
        after_rollback,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "email".to_string()],
            rows: vec![vec![
                Value::Int64(1),
                Value::Text("u1@example.com".to_string()),
            ]],
        }
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_drop_table_active_transaction_guard_and_isolated_purge() {
    let temp_dir = std::env::temp_dir().join(format!("mitrdb_group_d_drop_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();

    let mut s1 = None;
    let mut s2 = None;

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "CREATE TABLE t_drop (id INT PRIMARY KEY, tag TEXT)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "CREATE UNIQUE INDEX idx_t_drop_tag ON t_drop (tag)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "CREATE TABLE t_keep (id INT PRIMARY KEY, label TEXT)",
    )
    .unwrap();

    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut s1, "BEGIN").unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "INSERT INTO t_drop VALUES (1, 'active_row')",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "INSERT INTO t_keep VALUES (10, 'keep_row')",
    )
    .unwrap();

    let drop_err = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s2,
        "DROP TABLE t_drop",
    );
    assert!(matches!(drop_err, Err(Error::WriteConflict(_))));

    let s1_rows = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s1,
        "SELECT id, tag FROM t_drop WHERE tag = 'active_row'",
    )
    .unwrap();
    assert_eq!(
        s1_rows,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "tag".to_string()],
            rows: vec![vec![Value::Int64(1), Value::Text("active_row".to_string()),]],
        }
    );

    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut s1, "COMMIT").unwrap();

    let mut s3 = None;
    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut s3, "BEGIN").unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s3,
        "INSERT INTO t_keep VALUES (20, 'second_keep')",
    )
    .unwrap();

    let drop_ok = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s2,
        "DROP TABLE t_drop",
    )
    .unwrap();
    assert_eq!(drop_ok, ExecutionResult::Ok);
    assert!(catalog.get_table_schema("t_drop").unwrap().is_none());

    run_sql(&exec, &engine, &catalog, &txn_mgr, &mut s3, "COMMIT").unwrap();
    let keep_rows = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut s2,
        "SELECT id, label FROM t_keep ORDER BY id ASC",
    )
    .unwrap();
    assert_eq!(
        keep_rows,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "label".to_string()],
            rows: vec![
                vec![Value::Int64(10), Value::Text("keep_row".to_string())],
                vec![Value::Int64(20), Value::Text("second_keep".to_string())],
            ],
        }
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sql_constraints_duplicate_pk_duplicate_doc_id_and_union_order_by_exec() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_group_d_constraints_union_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
    let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
    let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
    let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
    let engine = QueryEngine::new(catalog.clone());
    let exec = Executor::new();
    let mut sess = None;

    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut sess,
        "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT NOT NULL, score INT)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut sess,
        "INSERT INTO accounts VALUES (1, 'alice', 100)",
    )
    .unwrap();
    run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut sess,
        "INSERT INTO accounts VALUES (2, 'bob', 200)",
    )
    .unwrap();

    assert!(
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "INSERT INTO accounts VALUES (1, 'charlie', 300)",
        )
        .is_err()
    );
    assert!(
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "INSERT INTO accounts VALUES (3, NULL, 300)",
        )
        .is_err()
    );
    assert!(
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "INSERT INTO accounts VALUES (3, 'charlie', 'wrong_type')",
        )
        .is_err()
    );
    assert!(
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "UPDATE accounts SET name = NULL WHERE id = 1",
        )
        .is_err()
    );
    assert!(
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "UPDATE accounts SET score = 'bad' WHERE id = 1",
        )
        .is_err()
    );

    let union_res = run_sql(
        &exec,
        &engine,
        &catalog,
        &txn_mgr,
        &mut sess,
        "SELECT id, name FROM accounts WHERE id = 1 UNION SELECT id, name FROM accounts WHERE id = 2 ORDER BY name DESC",
    )
    .unwrap();
    assert_eq!(
        union_res,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![Value::Int64(2), Value::Text("bob".to_string())],
                vec![Value::Int64(1), Value::Text("alice".to_string())],
            ],
        }
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_create_unique_index_on_duplicate_data_rolls_back_and_logged_backfill_survives_recovery() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_group_d_index_backfill_recovery_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
        let engine = QueryEngine::new(catalog.clone());
        let exec = Executor::new();
        let mut sess = None;

        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "CREATE TABLE members (id INT PRIMARY KEY, email TEXT)",
        )
        .unwrap();
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "INSERT INTO members VALUES (1, 'dup@example.com')",
        )
        .unwrap();
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "INSERT INTO members VALUES (2, 'dup@example.com')",
        )
        .unwrap();

        let dup_idx_err = run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "CREATE UNIQUE INDEX idx_members_email ON members (email)",
        );
        assert!(matches!(dup_idx_err, Err(Error::WriteConflict(_))));
        assert!(catalog.get_index_by_name("idx_members_email").is_none());

        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "UPDATE members SET email = 'unique2@example.com' WHERE id = 2",
        )
        .unwrap();
        run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "CREATE UNIQUE INDEX idx_members_email ON members (email)",
        )
        .unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let recovery = mitrdb_storage::recovery::RecoveryManager::new(wal.clone(), bpm.clone());
        recovery.recover().unwrap();

        let catalog = Arc::new(Catalog::with_storage(bpm.clone(), Some(wal.clone())));
        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
        let engine = QueryEngine::new(catalog.clone());
        let exec = Executor::new();
        let mut sess = None;

        let res = run_sql(
            &exec,
            &engine,
            &catalog,
            &txn_mgr,
            &mut sess,
            "SELECT id, email FROM members WHERE email = 'unique2@example.com'",
        )
        .unwrap();
        assert_eq!(
            res,
            ExecutionResult::Rows {
                columns: vec!["id".to_string(), "email".to_string()],
                rows: vec![vec![
                    Value::Int64(2),
                    Value::Text("unique2@example.com".to_string()),
                ]],
            }
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
