use mitrdb_server::{ExecutionResult, Server, SessionId, val};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Condvar, Mutex};

struct SessionPool {
    server: Arc<Server>,
    idle: Mutex<VecDeque<SessionId>>,
    cv: Condvar,
}

impl SessionPool {
    fn new(server: Arc<Server>, size: usize) -> Self {
        let mut idle = VecDeque::with_capacity(size);
        for _ in 0..size {
            idle.push_back(server.create_session());
        }
        Self {
            server,
            idle: Mutex::new(idle),
            cv: Condvar::new(),
        }
    }

    fn checkout(&self) -> SessionId {
        let mut g = self.idle.lock().unwrap();
        loop {
            if let Some(sid) = g.pop_front() {
                return sid;
            }
            g = self.cv.wait(g).unwrap();
        }
    }

    fn checkin(&self, sid: SessionId) {
        let mut g = self.idle.lock().unwrap();
        g.push_back(sid);
        self.cv.notify_one();
    }

    fn close_all(&self) {
        let mut g = self.idle.lock().unwrap();
        while let Some(sid) = g.pop_front() {
            self.server.close_session(sid).unwrap();
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("mitrdb_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn axum_style_pooled_sessions_parameterized_joins_pagination_and_group_commit() {
    let dir = temp_dir("axum_stress");
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    let srv = Arc::new(srv);

    srv.execute_sql("CREATE TABLE tenants (id INT PRIMARY KEY, plan TEXT NOT NULL)")
        .unwrap();
    srv.execute_sql(
        "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id INT NOT NULL, amount INT NOT NULL, status TEXT NOT NULL)",
    )
    .unwrap();
    srv.execute_sql("CREATE INDEX idx_orders_tenant ON orders (tenant_id)")
        .unwrap();

    // Seed 16 tenants (one per worker)
    for w in 0..16i64 {
        srv.execute_sql_params(
            "INSERT INTO tenants VALUES ($1, $2)",
            &[val(w + 1), val(if w % 2 == 0 { "pro" } else { "starter" })],
        )
        .unwrap();
    }

    let m_before = srv.metrics_snapshot();
    let pool = Arc::new(SessionPool::new(Arc::clone(&srv), 16));
    let workers = 16usize;
    let ops_per_worker = 30i64;
    let barrier = Arc::new(Barrier::new(workers));

    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers as i64 {
        let srv = Arc::clone(&srv);
        let pool = Arc::clone(&pool);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let tenant_id = w + 1;
            barrier.wait();

            for i in 0..ops_per_worker {
                let sid = pool.checkout();
                let order_id = tenant_id * 10_000 + i;

                // Simulate an Axum POST/PUT/GET request lifecycle on a pooled session
                srv.execute_in_session(sid, "BEGIN").unwrap();
                srv.execute_sql_params_in_session(
                    sid,
                    "INSERT INTO orders VALUES ($1, $2, $3, $4)",
                    &[val(order_id), val(tenant_id), val(100 + i), val("pending")],
                )
                .unwrap();
                srv.execute_sql_params_in_session(
                    sid,
                    "UPDATE orders SET status = $1, amount = amount + $2 WHERE id = $3",
                    &[val("paid"), val(50), val(order_id)],
                )
                .unwrap();
                srv.execute_in_session(sid, "COMMIT").unwrap();

                // Read path: parameterized JOIN + LIMIT $1 OFFSET $2 using AST cache
                let res = srv
                    .execute_sql_params_in_session(
                        sid,
                        "SELECT orders.id, tenants.plan, orders.amount FROM orders JOIN tenants ON orders.tenant_id = tenants.id WHERE orders.tenant_id = $1 ORDER BY orders.id DESC LIMIT $2 OFFSET $3",
                        &[val(tenant_id), val(5), val(0)],
                    )
                    .unwrap();
                match res {
                    ExecutionResult::Rows { rows, .. } => {
                        let expected_len = ((i + 1) as usize).min(5);
                        assert_eq!(rows.len(), expected_len);
                        assert_eq!(rows[0][0], val(order_id));
                        assert_eq!(rows[0][2], val(150 + i));
                    }
                    other => panic!("unexpected result: {other:?}"),
                }

                pool.checkin(sid);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    pool.close_all();

    let m_after = srv.metrics_snapshot();
    let total_commits = (workers as i64 * ops_per_worker) as u64;
    let flushes_delta = m_after.wal_flushes.saturating_sub(m_before.wal_flushes);
    assert!(
        flushes_delta > 0 && flushes_delta <= total_commits,
        "expected valid WAL flushes under concurrent load, got {flushes_delta} for {total_commits} commits"
    );

    let total_rows = srv.table("orders").count().unwrap();
    assert_eq!(total_rows, (workers as i64 * ops_per_worker) as usize);

    // Verify paginated query across all committed rows with LIMIT $1 OFFSET $2
    let page = srv
        .execute_sql_params(
            "SELECT id, amount FROM orders ORDER BY id ASC LIMIT $1 OFFSET $2",
            &[val(10), val(25)],
        )
        .unwrap();
    match page {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 10),
        other => panic!("unexpected result: {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn serializable_isolation_under_concurrent_session_contention() {
    let dir = temp_dir("axum_serializable");
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap();
    let srv = Arc::new(srv);

    srv.execute_sql("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)")
        .unwrap();
    srv.execute_sql_params("INSERT INTO accounts VALUES ($1, $2)", &[val(1), val(1000)])
        .unwrap();

    // 12 concurrent workers decrementing the same hot account row with optimistic retry on serialization conflict
    let workers = 12usize;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let srv = Arc::clone(&srv);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let sid = srv.create_session();
            barrier.wait();
            loop {
                srv.execute_in_session(sid, "BEGIN").unwrap();
                let res = srv.execute_sql_params_in_session(
                    sid,
                    "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
                    &[val(10), val(1)],
                );
                match res {
                    Ok(_) => match srv.execute_in_session(sid, "COMMIT") {
                        Ok(_) => break,
                        Err(_) => {
                            let _ = srv.execute_in_session(sid, "ROLLBACK");
                        }
                    },
                    Err(_) => {
                        let _ = srv.execute_in_session(sid, "ROLLBACK");
                    }
                }
                std::thread::yield_now();
            }
            srv.close_session(sid).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_row = srv
        .table("accounts")
        .find()
        .columns(&["balance"])
        .filter("id", "=", val(1))
        .one()
        .unwrap()
        .unwrap();
    assert_eq!(final_row[0], val(1000 - (workers as i64 * 10)));
    let _ = std::fs::remove_dir_all(&dir);
}
