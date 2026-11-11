#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::{IsolationLevel, Server};

static SRV_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> Result<TempDir, std::io::Error> {
    let id = SRV_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("mitrdb_m2_4_srv_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(TempDir(dir))
}

fn open_test_server() -> (TempDir, Server) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let (srv, _) = Server::open(&db_path, &wal_path, 64).unwrap();
    (dir, srv)
}

#[test]
fn m2_4_sql_serializable_index_equality_phantom_prevention() {
    let (_dir, srv) = open_test_server();
    srv.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, age INT, name TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_age ON users(age)")
        .unwrap();
    srv.execute_sql("INSERT INTO users VALUES (1, 20, 'alice')")
        .unwrap();
    srv.execute_sql("INSERT INTO users VALUES (2, 30, 'bob')")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // Session 1 scans WHERE age = 25 via IndexScan (0 matching rows, locks point/gap on idx_age)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let res1 = srv
        .execute_in_session(s1, "SELECT id, name FROM users WHERE age = 25")
        .unwrap();
    assert_eq!(
        res1,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
        }
    );

    // Session 2 attempts to insert a phantom row with age = 25 -> rejected with LockTimeout!
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO users VALUES (3, 25, 'phantom')"),
        Err(Error::LockTimeout)
    ));

    // Session 2 inserts a non-conflicting row with age = 26 -> succeeds!
    srv.execute_in_session(s2, "INSERT INTO users VALUES (4, 26, 'charlie')")
        .unwrap();

    // Session 1 re-runs the same query -> still 0 rows (no phantom!)
    let res2 = srv
        .execute_in_session(s1, "SELECT id, name FROM users WHERE age = 25")
        .unwrap();
    assert_eq!(res2, res1);
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // Now that Session 1 committed, Session 2 can insert age = 25
    srv.execute_in_session(s2, "INSERT INTO users VALUES (3, 25, 'now_allowed')")
        .unwrap();
    assert_eq!(srv.txn_manager().lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_sql_serializable_bounded_and_unbounded_range_predicates() {
    let (_dir, srv) = open_test_server();
    srv.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, amount INT, tag TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_amount ON orders(amount)")
        .unwrap();
    srv.execute_sql("INSERT INTO orders VALUES (10, 100, 'a')")
        .unwrap();
    srv.execute_sql("INSERT INTO orders VALUES (20, 200, 'b')")
        .unwrap();
    srv.execute_sql("INSERT INTO orders VALUES (30, 300, 'c')")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // 1. Primary-key range predicate: WHERE id >= 20
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let r = srv
        .execute_in_session(s1, "SELECT id FROM orders WHERE id >= 20")
        .unwrap();
    if let ExecutionResult::Rows { rows, .. } = &r {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected rows");
    }

    // Inserting id = 25 or id = 40 (inside [20, +inf)) is blocked
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO orders VALUES (25, 50, 'x')"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO orders VALUES (40, 50, 'y')"),
        Err(Error::LockTimeout)
    ));

    // Inserting id = 5 (outside [20, +inf)) succeeds!
    srv.execute_in_session(s2, "INSERT INTO orders VALUES (5, 50, 'ok')")
        .unwrap();
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // 2. Indexed column range predicate: WHERE amount <= 150
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "SELECT id FROM orders WHERE amount <= 150")
        .unwrap();

    // Inserting amount = 120 (inside (-inf, 150]) is blocked
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO orders VALUES (50, 120, 'blocked')"),
        Err(Error::LockTimeout)
    ));
    // Inserting amount = 250 (outside (-inf, 150]) succeeds!
    srv.execute_in_session(s2, "INSERT INTO orders VALUES (50, 250, 'allowed')")
        .unwrap();
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // 3. Unbounded scan: SELECT * FROM orders blocks any insert into the table
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "SELECT * FROM orders").unwrap();
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO orders VALUES (99, 999, 'blocked')"),
        Err(Error::LockTimeout)
    ));
    srv.execute_in_session(s1, "COMMIT").unwrap();
}

#[test]
fn m2_4_sql_serializable_update_and_delete_moving_into_or_out_of_range() {
    let (_dir, srv) = open_test_server();
    srv.execute_sql("CREATE TABLE emp (id INT PRIMARY KEY, salary INT, dept TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_salary ON emp(salary)")
        .unwrap();
    srv.execute_sql("INSERT INTO emp VALUES (1, 150, 'eng')")
        .unwrap();
    srv.execute_sql("INSERT INTO emp VALUES (2, 250, 'sales')")
        .unwrap();
    srv.execute_sql("INSERT INTO emp VALUES (3, 350, 'eng')")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // Session 1 locks salary >= 200 (matches id=2 and id=3; locks idx_salary [200, +inf) and PKs 2, 3)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let initial = srv
        .execute_in_session(
            s1,
            "SELECT id, salary FROM emp WHERE salary >= 200 ORDER BY id ASC",
        )
        .unwrap();

    // Case A: Session 2 attempts to UPDATE id=2 moving salary from 250 OUT of [200, +inf) to 180 -> blocked!
    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE emp SET salary = 180 WHERE id = 2"),
        Err(Error::LockTimeout)
    ));

    // Case B: Session 2 attempts to UPDATE id=1 moving salary from 150 INTO [200, +inf) to 220 -> blocked!
    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE emp SET salary = 220 WHERE id = 1"),
        Err(Error::LockTimeout)
    ));

    // Case C: Session 2 attempts to DELETE id=3 (inside locked range) -> blocked!
    assert!(matches!(
        srv.execute_in_session(s2, "DELETE FROM emp WHERE id = 3"),
        Err(Error::LockTimeout)
    ));

    // Case D: Session 2 updates id=1 from salary 150 to 160 (strictly outside [200, +inf)) -> succeeds!
    assert_eq!(
        srv.execute_in_session(s2, "UPDATE emp SET salary = 160 WHERE id = 1")
            .unwrap(),
        ExecutionResult::Affected(1)
    );

    // Case E: Session 2 deletes id=1 (strictly outside [200, +inf)) -> succeeds!
    assert_eq!(
        srv.execute_in_session(s2, "DELETE FROM emp WHERE id = 1")
            .unwrap(),
        ExecutionResult::Affected(1)
    );

    // Session 1 re-runs its range query -> sees identical rows!
    let repeat = srv
        .execute_in_session(
            s1,
            "SELECT id, salary FROM emp WHERE salary >= 200 ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(repeat, initial);
    srv.execute_in_session(s1, "COMMIT").unwrap();
}

#[test]
fn m2_4_sql_cross_access_path_and_catalog_ddl_conflicts() {
    let (_dir, srv) = open_test_server();
    srv.execute_sql("CREATE TABLE accounts (id INT PRIMARY KEY, code INT, balance INT)")
        .unwrap();
    srv.execute_sql("CREATE UNIQUE INDEX idx_code ON accounts(code)")
        .unwrap();
    srv.execute_sql("INSERT INTO accounts VALUES (1, 100, 500)")
        .unwrap();
    srv.execute_sql("INSERT INTO accounts VALUES (2, 200, 700)")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // 1. Cross-path protection: S1 reads via secondary index (WHERE code = 100), S2 tries to modify via PK (WHERE id = 1)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "SELECT balance FROM accounts WHERE code = 100")
        .unwrap();

    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE accounts SET balance = 999 WHERE id = 1"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "DELETE FROM accounts WHERE id = 1"),
        Err(Error::LockTimeout)
    ));
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // 2. Catalog DDL conflict: S1 holds a range lock ONLY on secondary index (0 matching rows on code = 999)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let empty_res = srv
        .execute_in_session(s1, "SELECT * FROM accounts WHERE code = 999")
        .unwrap();
    assert_eq!(
        empty_res,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "code".to_string(), "balance".to_string()],
            rows: vec![],
        }
    );

    // Neither DROP TABLE nor CREATE INDEX may proceed while S1 holds an index range lock on accounts
    assert!(matches!(
        srv.execute_in_session(s2, "CREATE INDEX idx_bal ON accounts(balance)"),
        Err(Error::WriteConflict(_))
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "DROP TABLE accounts"),
        Err(Error::WriteConflict(_))
    ));

    srv.execute_in_session(s1, "COMMIT").unwrap();

    // Once S1 commits, CREATE INDEX and DROP TABLE succeed
    srv.execute_in_session(s2, "CREATE INDEX idx_bal ON accounts(balance)")
        .unwrap();
    srv.execute_in_session(s2, "DROP TABLE accounts").unwrap();
}

#[test]
fn m2_4_sql_persistence_recovery_and_restart_after_serializable_workload() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("m2_4_srv.db");
    let wal_path = dir.path().join("m2_4_srv.wal");

    {
        let (srv, _) = Server::open(&db_path, &wal_path, 64).unwrap();
        srv.set_default_isolation(IsolationLevel::Serializable);
        srv.execute_sql("CREATE TABLE ledger (id INT PRIMARY KEY, acct INT, delta INT)")
            .unwrap();
        srv.execute_sql("CREATE INDEX idx_acct ON ledger(acct)")
            .unwrap();
        srv.execute_sql("INSERT INTO ledger VALUES (1, 10, 100)")
            .unwrap();
        srv.execute_sql("INSERT INTO ledger VALUES (2, 20, 200)")
            .unwrap();

        let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
        let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

        srv.execute_in_session(s1, "BEGIN").unwrap();
        srv.execute_in_session(s1, "SELECT * FROM ledger WHERE acct = 10")
            .unwrap();

        // Conflicting insert in s2 fails and rolls back cleanly
        srv.execute_in_session(s2, "BEGIN").unwrap();
        srv.execute_in_session(s2, "INSERT INTO ledger VALUES (3, 30, 300)")
            .unwrap();
        assert!(matches!(
            srv.execute_in_session(s2, "INSERT INTO ledger VALUES (4, 10, 400)"),
            Err(Error::LockTimeout)
        ));
        srv.execute_in_session(s2, "ROLLBACK").unwrap();

        srv.execute_in_session(s1, "INSERT INTO ledger VALUES (5, 10, 500)")
            .unwrap();
        srv.execute_in_session(s1, "COMMIT").unwrap();
        srv.checkpoint().unwrap();
    }

    let (srv_reopened, _) = Server::open(&db_path, &wal_path, 64).unwrap();
    let res = srv_reopened
        .execute_sql("SELECT id, acct, delta FROM ledger ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        res,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "acct".to_string(), "delta".to_string()],
            rows: vec![
                vec![Value::Int64(1), Value::Int64(10), Value::Int64(100)],
                vec![Value::Int64(2), Value::Int64(20), Value::Int64(200)],
                vec![Value::Int64(5), Value::Int64(10), Value::Int64(500)],
            ],
        }
    );
}

#[test]
fn m2_4_1_audit_sql_pk_read_protects_secondary_index_and_non_indexed_update_conflicts() {
    let (_dir, srv) = open_test_server();
    srv.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, category INT, price INT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_cat ON items(category)")
        .unwrap();
    srv.execute_sql("INSERT INTO items VALUES (1, 10, 100)")
        .unwrap();
    srv.execute_sql("INSERT INTO items VALUES (2, 20, 200)")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // 1. S1 locks category = 10 via secondary index scan; S2 tries to update non-indexed column (price) of id=1
    // Because S2 modifies the row at (id=1, category=10), it must acquire Exclusive locks on both PK and idx_cat(10)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "SELECT id, price FROM items WHERE category = 10")
        .unwrap();

    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE items SET price = 150 WHERE id = 1"),
        Err(Error::LockTimeout)
    ));
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // 2. Reverse direction: S1 reads row id=1 via PK filter (WHERE id = 1) and holds Shared locks on PK(1) and idx_cat(10)
    // S1 then updates price = 120 (acquiring Exclusive locks on PK(1) and idx_cat(10)).
    // S2 attempting a Serializable scan on WHERE category = 10 must conflict!
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "UPDATE items SET price = 120 WHERE id = 1")
        .unwrap();

    srv.execute_in_session(s2, "BEGIN").unwrap();
    assert!(matches!(
        srv.execute_in_session(s2, "SELECT id, price FROM items WHERE category = 10"),
        Err(Error::LockTimeout)
    ));
    srv.execute_in_session(s2, "ROLLBACK").unwrap();
    srv.execute_in_session(s1, "COMMIT").unwrap();

    assert_eq!(srv.txn_manager().lock_manager().active_lock_count(), 0);
}

#[test]
fn m2_4_1_audit_sql_concurrent_sessions_barrier_verification() {
    use std::sync::{Arc, Barrier};

    let (dir, raw_srv) = open_test_server();
    let srv = Arc::new(raw_srv);
    srv.execute_sql("CREATE TABLE bookings (id INT PRIMARY KEY, room INT, guest TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_room ON bookings(room)")
        .unwrap();
    srv.execute_sql("INSERT INTO bookings VALUES (1, 101, 'alice')")
        .unwrap();
    srv.execute_sql("INSERT INTO bookings VALUES (2, 102, 'bob')")
        .unwrap();

    // Two concurrent Serializable sessions both check room = 103 (empty gap on idx_room)
    // at a synchronization barrier, then both attempt to insert a booking for room = 103.
    // Under Serializable gap/range locking, neither can bypass the other's shared range lock!
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();

    for i in 0..2 {
        let s = srv.clone();
        let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let sess = s.create_session_with_isolation(IsolationLevel::Serializable);
            s.execute_in_session(sess, "BEGIN").unwrap();
            let check = s
                .execute_in_session(sess, "SELECT id FROM bookings WHERE room = 103")
                .unwrap();
            assert_eq!(
                check,
                ExecutionResult::Rows {
                    columns: vec!["id".to_string()],
                    rows: vec![],
                }
            );
            b.wait();
            let ins_sql = format!(
                "INSERT INTO bookings VALUES ({}, 103, 'guest_{}')",
                10 + i,
                i
            );
            let ins_res = s.execute_in_session(sess, &ins_sql);
            b.wait();
            let _ = s.execute_in_session(sess, "ROLLBACK");
            ins_res
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(
        results.iter().any(|r| matches!(
            r,
            Err(Error::LockTimeout | Error::Deadlock | Error::WriteConflict(_))
        )),
        "Expected at least one transaction to be rejected by serializable gap locking: {:?}",
        results
    );
    assert_eq!(srv.txn_manager().lock_manager().active_lock_count(), 0);
    drop(dir);
}
