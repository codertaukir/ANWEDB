#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;
use mitrdb_server::{IsolationLevel, Server};

static M25_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    let id = M25_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("mitrdb_m2_5_srv_{}_{}", std::process::id(), id));
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
fn m2_5_index_range_scan_boundaries_duplicates_splits_and_seq_equivalence() {
    let (_dir, srv) = open_test_server();

    // Create two identical tables: one with secondary indexes, one without (for SeqScan equivalence)
    srv.execute_sql(
        "CREATE TABLE items_idx (id INT PRIMARY KEY, category INT, price INT, pad TEXT)",
    )
    .unwrap();
    srv.execute_sql(
        "CREATE TABLE items_seq (id INT PRIMARY KEY, category INT, price INT, pad TEXT)",
    )
    .unwrap();
    srv.execute_sql("CREATE INDEX idx_items_price ON items_idx(price)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_items_cat ON items_idx(category)")
        .unwrap();

    // Verify EXPLAIN shows IndexRangeScan on items_idx and SeqScan on items_seq
    let explain_idx = srv
        .execute_sql("EXPLAIN SELECT id, price FROM items_idx WHERE price >= 100 AND price < 500")
        .unwrap();
    assert_eq!(
        explain_idx,
        ExecutionResult::Explained(
            "IndexRangeScan(table=items_idx, index=idx_items_price, column=price, ranges=1)"
                .to_string()
        )
    );
    let explain_seq = srv
        .execute_sql("EXPLAIN SELECT id, price FROM items_seq WHERE price >= 100 AND price < 500")
        .unwrap();
    assert_eq!(
        explain_seq,
        ExecutionResult::Explained("SeqScan(table=items_seq)".to_string())
    );

    // Populate 60 rows with duplicate indexed values and large padding to force B+Tree leaf & internal splits
    let payload = "P".repeat(160);
    for i in 1..=60i64 {
        let cat = (i % 5) * 10; // duplicate categories: 0, 10, 20, 30, 40
        let price = (i % 12) * 50; // duplicate prices: 0, 50, 100, ..., 550
        srv.execute_sql(&format!(
            "INSERT INTO items_idx VALUES ({}, {}, {}, '{}_{}')",
            i, cat, price, payload, i
        ))
        .unwrap();
        srv.execute_sql(&format!(
            "INSERT INTO items_seq VALUES ({}, {}, {}, '{}_{}')",
            i, cat, price, payload, i
        ))
        .unwrap();
    }

    // Perform updates and deletes on duplicate keys to test non-unique index maintenance across splits
    srv.execute_sql("UPDATE items_idx SET price = 250 WHERE id = 1")
        .unwrap();
    srv.execute_sql("UPDATE items_seq SET price = 250 WHERE id = 1")
        .unwrap();
    srv.execute_sql("DELETE FROM items_idx WHERE id = 12")
        .unwrap();
    srv.execute_sql("DELETE FROM items_seq WHERE id = 12")
        .unwrap();

    let predicates = [
        "price = 250",
        "price > 200",
        "price >= 200",
        "price < 150",
        "price <= 150",
        "price >= 100 AND price < 400",
        "price > 100 AND price <= 400",
        "price >= 250 AND price <= 250", // single-key range
        "price > 250 AND price < 250",   // empty range
        "price > 9999",                  // range with no matching keys
        "category = 10 OR category = 30",
        "(price >= 100 AND price <= 200) OR (price >= 450 AND price <= 550)",
        "price >= 150 AND category = 20",
    ];

    let metrics_before = srv.metrics_snapshot();
    for pred in predicates {
        let q_idx = format!(
            "SELECT id, category, price FROM items_idx WHERE {} ORDER BY id ASC",
            pred
        );
        let q_seq = format!(
            "SELECT id, category, price FROM items_seq WHERE {} ORDER BY id ASC",
            pred
        );
        let res_idx = srv.execute_sql(&q_idx).unwrap();
        let res_seq = srv.execute_sql(&q_seq).unwrap();
        assert_eq!(
            res_idx, res_seq,
            "Mismatch between IndexRangeScan and SeqScan for predicate: {}",
            pred
        );
    }
    let metrics_after = srv.metrics_snapshot();
    assert!(
        metrics_after.index_scans >= metrics_before.index_scans + predicates.len() as u64,
        "Expected index_scans metric to increment for every indexed predicate"
    );
}

#[test]
fn m2_5_compound_predicates_and_or_nested_parens_nulls_and_deduplication() {
    let (_dir, srv) = open_test_server();

    srv.execute_sql(
        "CREATE TABLE products (id INT PRIMARY KEY, category INT, price INT, tag TEXT)",
    )
    .unwrap();
    srv.execute_sql("CREATE INDEX idx_prod_cat ON products(category)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_prod_price ON products(price)")
        .unwrap();

    srv.execute_sql("INSERT INTO products VALUES (1, 10, 100, 'alpha')")
        .unwrap();
    srv.execute_sql("INSERT INTO products VALUES (2, 10, 250, 'beta')")
        .unwrap();
    srv.execute_sql("INSERT INTO products VALUES (3, 20, 150, 'gamma')")
        .unwrap();
    srv.execute_sql("INSERT INTO products VALUES (4, 20, 450, 'delta')")
        .unwrap();
    srv.execute_sql("INSERT INTO products VALUES (5, 30, 300, NULL)")
        .unwrap();
    srv.execute_sql("INSERT INTO products VALUES (6, NULL, NULL, 'orphan')")
        .unwrap();

    // 1. Overlapping OR conditions on indexed column must not return duplicate rows
    let overlap_idx = srv
        .execute_sql("SELECT id FROM products WHERE price >= 100 OR price >= 200 ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        overlap_idx,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
                vec![Value::Int64(4)],
                vec![Value::Int64(5)],
            ],
        }
    );

    // 2. Overlapping OR conditions across unindexed column (SeqScan) also return no duplicates
    let overlap_seq = srv
        .execute_sql("SELECT id FROM products WHERE tag >= 'a' OR tag >= 'b' ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        overlap_seq,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
                vec![Value::Int64(4)],
                vec![Value::Int64(6)],
            ],
        }
    );

    // 3. Nested parentheses with AND / OR
    let nested = srv
        .execute_sql(
            "SELECT id FROM products WHERE (category = 10 OR category = 20) AND (price >= 150 AND price <= 300) ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        nested,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::Int64(2)], vec![Value::Int64(3)]],
        }
    );

    // 4. Three-valued NULL semantics in comparisons and IS NULL / IS NOT NULL
    let null_eq = srv
        .execute_sql("SELECT id FROM products WHERE price = NULL")
        .unwrap();
    assert_eq!(
        null_eq,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![],
        }
    );
    let null_ne = srv
        .execute_sql("SELECT id FROM products WHERE tag != 'alpha' ORDER BY id ASC")
        .unwrap();
    // Row 5 has tag = NULL, so tag != 'alpha' is UNKNOWN (excluded!)
    assert_eq!(
        null_ne,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
                vec![Value::Int64(4)],
                vec![Value::Int64(6)],
            ],
        }
    );

    let is_null_res = srv
        .execute_sql("SELECT id FROM products WHERE price IS NULL OR tag IS NULL ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        is_null_res,
        ExecutionResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::Int64(5)], vec![Value::Int64(6)]],
        }
    );

    // 5. UPDATE and DELETE with compound WHERE predicates
    assert_eq!(
        srv.execute_sql("UPDATE products SET price = 275 WHERE category = 10 AND price >= 200")
            .unwrap(),
        ExecutionResult::Affected(1)
    );
    assert_eq!(
        srv.execute_sql(
            "DELETE FROM products WHERE category = 20 AND (price < 200 OR price > 400)"
        )
        .unwrap(),
        ExecutionResult::Affected(2)
    );
}

#[test]
fn m2_5_sql_aggregations_nulls_empty_overflow_precision_and_transactions() {
    let (_dir, srv) = open_test_server();

    srv.execute_sql("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_sales_amount ON sales(amount)")
        .unwrap();

    // 1. Empty table aggregation behavior
    let empty_agg = srv
        .execute_sql(
            "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM sales",
        )
        .unwrap();
    assert_eq!(
        empty_agg,
        ExecutionResult::Rows {
            columns: vec![
                "COUNT(*)".to_string(),
                "COUNT(amount)".to_string(),
                "SUM(amount)".to_string(),
                "MIN(amount)".to_string(),
                "MAX(amount)".to_string(),
                "AVG(amount)".to_string(),
            ],
            rows: vec![vec![
                Value::Int64(0),
                Value::Int64(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]],
        }
    );

    // 2. All-NULL column aggregation behavior
    srv.execute_sql("INSERT INTO sales VALUES (1, NULL, NULL)")
        .unwrap();
    srv.execute_sql("INSERT INTO sales VALUES (2, NULL, NULL)")
        .unwrap();
    let null_agg = srv
        .execute_sql(
            "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(region), MAX(region), AVG(amount) FROM sales",
        )
        .unwrap();
    assert_eq!(
        null_agg,
        ExecutionResult::Rows {
            columns: vec![
                "COUNT(*)".to_string(),
                "COUNT(amount)".to_string(),
                "SUM(amount)".to_string(),
                "MIN(region)".to_string(),
                "MAX(region)".to_string(),
                "AVG(amount)".to_string(),
            ],
            rows: vec![vec![
                Value::Int64(2),
                Value::Int64(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]],
        }
    );

    // 3. Mixed NULL and non-NULL values + filtered aggregation
    srv.execute_sql("INSERT INTO sales VALUES (3, 'east', 100)")
        .unwrap();
    srv.execute_sql("INSERT INTO sales VALUES (4, 'west', 200)")
        .unwrap();
    srv.execute_sql("INSERT INTO sales VALUES (5, 'north', 300)")
        .unwrap();

    let populated_agg = srv
        .execute_sql(
            "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount), AVG(amount), MIN(region), MAX(region) FROM sales",
        )
        .unwrap();
    assert_eq!(
        populated_agg,
        ExecutionResult::Rows {
            columns: vec![
                "COUNT(*)".to_string(),
                "COUNT(amount)".to_string(),
                "SUM(amount)".to_string(),
                "MIN(amount)".to_string(),
                "MAX(amount)".to_string(),
                "AVG(amount)".to_string(),
                "MIN(region)".to_string(),
                "MAX(region)".to_string(),
            ],
            rows: vec![vec![
                Value::Int64(5),
                Value::Int64(3),
                Value::Int64(600),
                Value::Int64(100),
                Value::Int64(300),
                Value::Int64(200),
                Value::Text("east".to_string()),
                Value::Text("west".to_string()),
            ]],
        }
    );

    // Filtered aggregate using IndexRangeScan
    let explain_filtered_agg = srv
        .execute_sql(
            "EXPLAIN SELECT COUNT(*), SUM(amount) FROM sales WHERE amount >= 150 AND amount <= 300",
        )
        .unwrap();
    assert_eq!(
        explain_filtered_agg,
        ExecutionResult::Explained(
            "Aggregate(funcs=[COUNT(*), SUM(amount)], input=IndexRangeScan(table=sales, index=idx_sales_amount, column=amount, ranges=1))".to_string()
        )
    );

    let filtered_agg = srv
        .execute_sql("SELECT COUNT(*), SUM(amount), AVG(amount) FROM sales WHERE amount >= 150 AND amount <= 300")
        .unwrap();
    assert_eq!(
        filtered_agg,
        ExecutionResult::Rows {
            columns: vec![
                "COUNT(*)".to_string(),
                "SUM(amount)".to_string(),
                "AVG(amount)".to_string(),
            ],
            rows: vec![vec![Value::Int64(2), Value::Int64(500), Value::Int64(250)]],
        }
    );

    // 4. Transaction visibility & rollback for aggregations
    let s1 = srv.create_session();
    let s2 = srv.create_session();
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(s1, "INSERT INTO sales VALUES (6, 'south', 400)")
        .unwrap();

    let s1_agg = srv
        .execute_in_session(
            s1,
            "SELECT COUNT(amount), SUM(amount), AVG(amount) FROM sales",
        )
        .unwrap();
    assert_eq!(
        s1_agg,
        ExecutionResult::Rows {
            columns: vec![
                "COUNT(amount)".to_string(),
                "SUM(amount)".to_string(),
                "AVG(amount)".to_string(),
            ],
            rows: vec![vec![Value::Int64(4), Value::Int64(1000), Value::Int64(250),]],
        }
    );

    // s2 does not see s1's uncommitted insert
    let s2_agg = srv
        .execute_in_session(s2, "SELECT COUNT(amount), SUM(amount) FROM sales")
        .unwrap();
    assert_eq!(
        s2_agg,
        ExecutionResult::Rows {
            columns: vec!["COUNT(amount)".to_string(), "SUM(amount)".to_string()],
            rows: vec![vec![Value::Int64(3), Value::Int64(600)]],
        }
    );
    srv.execute_in_session(s1, "ROLLBACK").unwrap();

    // 5. Overflow and 64-bit numeric precision boundary tests
    srv.execute_sql("CREATE TABLE big_nums (id INT PRIMARY KEY, val INT)")
        .unwrap();
    srv.execute_sql(&format!("INSERT INTO big_nums VALUES (1, {})", i64::MAX))
        .unwrap();
    srv.execute_sql(&format!(
        "INSERT INTO big_nums VALUES (2, {})",
        i64::MAX - 2
    ))
    .unwrap();

    // SUM overflows i64::MAX -> must return TransactionError rather than wrapping
    let sum_overflow = srv.execute_sql("SELECT SUM(val) FROM big_nums");
    assert!(matches!(sum_overflow, Err(Error::TransactionError(_))));

    // AVG uses 128-bit intermediate sum and preserves exact 64-bit precision (i64::MAX - 1),
    // whereas f64 would lose precision above 2^53!
    let exact_avg = srv.execute_sql("SELECT AVG(val) FROM big_nums").unwrap();
    assert_eq!(
        exact_avg,
        ExecutionResult::Rows {
            columns: vec!["AVG(val)".to_string()],
            rows: vec![vec![Value::Int64(i64::MAX - 1)]],
        }
    );
}

#[test]
fn m2_5_serializable_index_range_scan_compound_predicates_and_aggregates_concurrency() {
    let (_dir, raw_srv) = open_test_server();
    let srv = Arc::new(raw_srv);

    srv.execute_sql(
        "CREATE TABLE inventory (id INT PRIMARY KEY, category INT, price INT, stock INT)",
    )
    .unwrap();
    srv.execute_sql("CREATE INDEX idx_inv_price ON inventory(price)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_inv_cat ON inventory(category)")
        .unwrap();

    srv.execute_sql("INSERT INTO inventory VALUES (1, 10, 100, 5)")
        .unwrap();
    srv.execute_sql("INSERT INTO inventory VALUES (2, 10, 200, 8)")
        .unwrap();
    srv.execute_sql("INSERT INTO inventory VALUES (3, 20, 300, 12)")
        .unwrap();
    srv.execute_sql("INSERT INTO inventory VALUES (4, 30, 600, 20)")
        .unwrap();

    let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
    let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);

    // Part 1: Direct IndexRangeScan with compound AND (price >= 150 AND price <= 350)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let r1 = srv
        .execute_in_session(
            s1,
            "SELECT id, price FROM inventory WHERE price >= 150 AND price <= 350 ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        r1,
        ExecutionResult::Rows {
            columns: vec!["id".to_string(), "price".to_string()],
            rows: vec![
                vec![Value::Int64(2), Value::Int64(200)],
                vec![Value::Int64(3), Value::Int64(300)],
            ],
        }
    );

    // Conflicting insert inside [150, 350] is rejected
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO inventory VALUES (10, 40, 250, 1)"),
        Err(Error::LockTimeout)
    ));
    // Conflicting update moving id=1 into [150, 350] is rejected
    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE inventory SET price = 180 WHERE id = 1"),
        Err(Error::LockTimeout)
    ));
    // Conflicting update on non-indexed column (stock) of matched row id=2 is rejected
    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE inventory SET stock = 99 WHERE id = 2"),
        Err(Error::LockTimeout)
    ));
    // Conflicting delete of matched row id=3 is rejected
    assert!(matches!(
        srv.execute_in_session(s2, "DELETE FROM inventory WHERE id = 3"),
        Err(Error::LockTimeout)
    ));
    // Non-conflicting insert outside [150, 350] (price = 500) succeeds!
    srv.execute_in_session(s2, "INSERT INTO inventory VALUES (5, 30, 500, 15)")
        .unwrap();

    let r1_repeat = srv
        .execute_in_session(
            s1,
            "SELECT id, price FROM inventory WHERE price >= 150 AND price <= 350 ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(r1_repeat, r1);
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // Part 2: Compound OR predicate (category = 10 OR category = 20)
    srv.execute_in_session(s1, "BEGIN").unwrap();
    srv.execute_in_session(
        s1,
        "SELECT id FROM inventory WHERE category = 10 OR category = 20",
    )
    .unwrap();

    // Inserts into category = 10 or category = 20 are blocked
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO inventory VALUES (11, 10, 900, 1)"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO inventory VALUES (12, 20, 900, 1)"),
        Err(Error::LockTimeout)
    ));
    // Insert into category = 15 (between 10 and 20, outside both locked points) succeeds!
    srv.execute_in_session(s2, "INSERT INTO inventory VALUES (6, 15, 450, 7)")
        .unwrap();
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // Part 3: Aggregate query under Serializable isolation protects its scanned range
    srv.execute_in_session(s1, "BEGIN").unwrap();
    let agg1 = srv
        .execute_in_session(
            s1,
            "SELECT COUNT(*), SUM(stock), AVG(price) FROM inventory WHERE price >= 100 AND price <= 300",
        )
        .unwrap();

    // Concurrent writes that would alter the aggregate result in [100, 300] are blocked
    assert!(matches!(
        srv.execute_in_session(s2, "INSERT INTO inventory VALUES (20, 99, 250, 100)"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "UPDATE inventory SET stock = 50 WHERE id = 1"),
        Err(Error::LockTimeout)
    ));
    assert!(matches!(
        srv.execute_in_session(s2, "DELETE FROM inventory WHERE id = 2"),
        Err(Error::LockTimeout)
    ));

    let agg2 = srv
        .execute_in_session(
            s1,
            "SELECT COUNT(*), SUM(stock), AVG(price) FROM inventory WHERE price >= 100 AND price <= 300",
        )
        .unwrap();
    assert_eq!(agg1, agg2);
    srv.execute_in_session(s1, "COMMIT").unwrap();

    // Part 4: Barrier-synchronized concurrent Serializable aggregate range scans vs writers
    let barrier = Arc::new(Barrier::new(2));
    let srv_a = srv.clone();
    let bar_a = barrier.clone();
    let h1 = std::thread::spawn(move || {
        let sess = srv_a.create_session_with_isolation(IsolationLevel::Serializable);
        srv_a.execute_in_session(sess, "BEGIN").unwrap();
        let _ = srv_a
            .execute_in_session(
                sess,
                "SELECT COUNT(*), SUM(price) FROM inventory WHERE price >= 700 AND price <= 800",
            )
            .unwrap();
        bar_a.wait();
        let res = srv_a.execute_in_session(sess, "INSERT INTO inventory VALUES (31, 50, 750, 1)");
        bar_a.wait();
        assert!(matches!(res, Err(Error::LockTimeout | Error::Deadlock)));
        srv_a.execute_in_session(sess, "ROLLBACK").unwrap();
        srv_a.close_session(sess).unwrap();
    });

    let srv_b = srv.clone();
    let bar_b = barrier.clone();
    let h2 = std::thread::spawn(move || {
        let sess = srv_b.create_session_with_isolation(IsolationLevel::Serializable);
        srv_b.execute_in_session(sess, "BEGIN").unwrap();
        let _ = srv_b
            .execute_in_session(
                sess,
                "SELECT COUNT(*), SUM(price) FROM inventory WHERE price >= 700 AND price <= 800",
            )
            .unwrap();
        bar_b.wait();
        let res = srv_b.execute_in_session(sess, "INSERT INTO inventory VALUES (32, 50, 750, 2)");
        bar_b.wait();
        assert!(matches!(res, Err(Error::LockTimeout | Error::Deadlock)));
        srv_b.execute_in_session(sess, "ROLLBACK").unwrap();
        srv_b.close_session(sess).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();

    srv.close_session(s1).unwrap();
    srv.close_session(s2).unwrap();
    assert_eq!(srv.txn_manager().lock_manager().active_lock_count(), 0);
}
