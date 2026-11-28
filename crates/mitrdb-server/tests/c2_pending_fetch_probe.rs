// C2 roadmap: WHERE device = ? AND state = 'pending' ORDER BY created_at LIMIT N
// Composite (device, state, created_at), 2-col Eq prefix + ORDER BY 3rd col
use std::fs;
use std::time::Instant;
const BASE: i64 = 1_700_000_000_000_000;
const STEP: i64 = 1_000;
fn open_server(dir: &std::path::Path) -> mitrdb_server::Server {
    mitrdb_server::Server::open(dir.join("t.db"), dir.join("t.wal"), 512)
        .expect("open")
        .0
}
fn exec_timed(s: &mitrdb_server::Server, sql: &str) {
    s.execute_sql(sql)
        .unwrap_or_else(|e| panic!("SQL {} | {}", e, sql));
}
fn exec_debug(s: &mitrdb_server::Server, sql: &str) -> String {
    format!(
        "{:?}",
        s.execute_sql(sql)
            .unwrap_or_else(|e| panic!("SQL {} | {}", e, sql))
    )
}
fn int64_vals(dbg: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut rest = dbg;
    while let Some(pos) = rest.find("Int64(") {
        let after = &rest[pos + 6..];
        if let Some(end) = after.find(')') {
            if let Ok(v) = after[..end].parse::<i64>() {
                out.push(v);
            }
            rest = &after[end + 1..];
        } else {
            break;
        }
    }
    out
}
fn created_vals(dbg: &str) -> Vec<i64> {
    int64_vals(dbg)
        .into_iter()
        .filter(|v| *v >= 1_000_000_000_000_000)
        .collect()
}
fn count_val(s: &mitrdb_server::Server, sql: &str) -> i64 {
    let dbg = exec_debug(s, sql);
    *int64_vals(&dbg).first().expect("no Int64 in COUNT debug")
}
fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}
fn measure(s: &mitrdb_server::Server, sql: &str) -> u128 {
    exec_timed(s, sql);
    let mut t = Vec::new();
    for _ in 0..5 {
        let st = Instant::now();
        exec_timed(s, sql);
        t.push(st.elapsed().as_micros());
    }
    median(t)
}
#[test]
fn c2_pending_fetch() {
    let dir = std::env::temp_dir().join(format!(
        "mitrdb-c2pf-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    let srv = open_server(&dir);
    exec_debug(
        &srv,
        "CREATE TABLE jobs (id INT PRIMARY KEY, device TEXT, state TEXT, created_at INT)",
    );
    let mut id: i64 = 1;
    for batch in 0..24 {
        exec_debug(&srv, "BEGIN");
        for k in 0..5_000 {
            let ts = BASE + ((batch * 5_000 + k) as i64) * STEP;
            exec_timed(
                &srv,
                &format!(
                    "INSERT INTO jobs (id, device, state, created_at) VALUES ({}, 'dev-A', 'pending', {})",
                    id, ts
                ),
            );
            id += 1;
            exec_timed(
                &srv,
                &format!(
                    "INSERT INTO jobs (id, device, state, created_at) VALUES ({}, 'dev-A', 'done', {})",
                    id, ts
                ),
            );
            id += 1;
        }
        exec_debug(&srv, "COMMIT");
    }
    exec_debug(
        &srv,
        "CREATE INDEX idx_device_state_created ON jobs (device, state, created_at)",
    );
    let m = count_val(
        &srv,
        "SELECT COUNT(*) FROM jobs WHERE device = 'dev-A' AND state = 'pending'",
    );
    assert_eq!(m, 120_000, "setup M galat {}", m);
    println!("C2 setup M=120k");
    let plan = exec_debug(
        &srv,
        "EXPLAIN SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 100",
    );
    println!("C2 EXPLAIN: {}", plan);
    let has_ordered = plan.contains("IndexOrderedScan");
    let has_seq = plan.contains("SeqScan");
    let has_top_n = plan.contains("top_n=true");
    println!(
        "C2 plan ordered={} seq={} top_n={}",
        has_ordered, has_seq, has_top_n
    );
    let dbg = exec_debug(
        &srv,
        "SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 1000",
    );
    let rows = created_vals(&dbg);
    assert_eq!(rows.len(), 1000);
    assert!(rows.windows(2).all(|w| w[0] <= w[1]));
    assert!(rows.iter().all(|v| *v >= BASE));
    assert_eq!(rows[0], BASE);
    let t10 = measure(
        &srv,
        "SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 10",
    );
    let t1000 = measure(
        &srv,
        "SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 1000",
    );
    let t100k = measure(
        &srv,
        "SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 100000",
    );
    println!("C2 LATENCY t10={} t1000={} t100k={}", t10, t1000, t100k);
    let dbg100k = exec_debug(
        &srv,
        "SELECT created_at FROM jobs WHERE device = 'dev-A' AND state = 'pending' ORDER BY created_at LIMIT 100000",
    );
    assert_eq!(created_vals(&dbg100k).len(), 100_000);
    println!(
        "C2 DONE ordered={} seq={} top_n={} t10={} t1000={} t100k={}",
        has_ordered, has_seq, has_top_n, t10, t1000, t100k
    );
}
