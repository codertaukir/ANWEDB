// C1 compact v4: WHERE conv = ? AND created_at > ? ORDER BY created_at LIMIT N
// Expected: IndexRangeScan(top_n=true), conv prefix only, no early stop. Fix nahi hai.
use std::fs;
use std::time::Instant;
const BASE: i64 = 1_700_000_000_000_000;
const STEP: i64 = 1_000;
fn open_server(dir: &std::path::Path) -> mitrdb_server::Server {
    mitrdb_server::Server::open(dir.join("t.db"), dir.join("t.wal"), 512).expect("open").0
}
fn exec_timed(s: &mitrdb_server::Server, sql: &str) {
    s.execute_sql(sql).unwrap_or_else(|e| panic!("SQL {} | {}", e, sql));
}
fn exec_debug(s: &mitrdb_server::Server, sql: &str) -> String {
    format!("{:?}", s.execute_sql(sql).unwrap_or_else(|e| panic!("SQL {} | {}", e, sql)))
}
fn all_ints(dbg: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in dbg.chars().chain(std::iter::once(' ')) {
        if ch.is_ascii_digit() || (ch == '-' && cur.is_empty()) { cur.push(ch); }
        else if !cur.is_empty() && cur != "-" {
            if let Ok(v) = cur.parse::<i64>() { out.push(v); }
            cur.clear();
        } else { cur.clear(); }
    }
    out
}
fn created_vals(dbg: &str) -> Vec<i64> {
    all_ints(dbg).into_iter().filter(|v| *v >= 1_000_000_000_000_000).collect()
}
fn count_val(s: &mitrdb_server::Server, sql: &str) -> i64 {
    let dbg = exec_debug(s, sql);
    *all_ints(&dbg).iter().filter(|v| **v < 1_000_000_000_000_000).max().unwrap_or(&-1)
}
fn median(mut v: Vec<u128>) -> u128 { v.sort_unstable(); v[v.len() / 2] }
fn measure(s: &mitrdb_server::Server, sql: &str) -> u128 {
    exec_timed(s, sql);
    let mut t = Vec::new();
    for _ in 0..5 { let st = Instant::now(); exec_timed(s, sql); t.push(st.elapsed().as_micros()); }
    median(t)
}
#[test]
fn c1_conv_created() {
    let dir = std::env::temp_dir().join(format!("mitrdb-c1-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    fs::create_dir_all(&dir).unwrap();
    let srv = open_server(&dir);
    exec_debug(&srv, "CREATE TABLE messages (id INT PRIMARY KEY, conv TEXT, created_at INT, body TEXT)");
    let mut id: i64 = 1;
    for batch in 0..24 {
        exec_debug(&srv, "BEGIN");
        for k in 0..5_000 {
            let ts = BASE + ((batch * 5_000 + k) as i64) * STEP;
            exec_timed(&srv, &format!("INSERT INTO messages (id, conv, created_at, body) VALUES ({}, 'conv-A', {}, 'm')", id, ts)); id += 1;
            exec_timed(&srv, &format!("INSERT INTO messages (id, conv, created_at, body) VALUES ({}, 'conv-B', {}, 'm')", id, ts)); id += 1;
        }
        exec_debug(&srv, "COMMIT");
    }
    exec_debug(&srv, "BEGIN");
    for k in 0..5_000 {
        let ts = BASE + (k as i64) * STEP;
        exec_timed(&srv, &format!("INSERT INTO messages (id, conv, created_at, body) VALUES ({}, 'conv-C', {}, 'm')", id, ts)); id += 1;
    }
    exec_debug(&srv, "COMMIT");
    exec_debug(&srv, "CREATE INDEX idx_conv_created ON messages (conv, created_at)");
    let thr = BASE - 1;
    let m = count_val(&srv, &format!("SELECT COUNT(*) FROM messages WHERE conv = 'conv-A' AND created_at > {}", thr));
    assert_eq!(m, 120_000, "setup M galat {}", m);
    println!("C1 setup M=120k");
    let plan = exec_debug(&srv, &format!("EXPLAIN SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 100", thr));
    println!("C1 EXPLAIN: {}", plan);
    let has_range = plan.contains("IndexRangeScan");
    let has_ordered = plan.contains("IndexOrderedScan");
    let has_seq = plan.contains("SeqScan");
    let has_top_n = plan.contains("top_n=true");
    println!("C1 plan range={} ordered={} seq={} top_n={}", has_range, has_ordered, has_seq, has_top_n);
    let dbg = exec_debug(&srv, &format!("SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 1000", thr));
    let rows = created_vals(&dbg);
    assert_eq!(rows.len(), 1000);
    assert!(rows.windows(2).all(|w| w[0] <= w[1]));
    assert!(rows.iter().all(|v| *v > thr));
    assert_eq!(rows[0], BASE);
    let t10 = measure(&srv, &format!("SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 10", thr));
    let t1000 = measure(&srv, &format!("SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 1000", thr));
    let t100k = measure(&srv, &format!("SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 100000", thr));
    println!("C1 LATENCY t10={} t1000={} t100k={}", t10, t1000, t100k);
    let dbg100k = exec_debug(&srv, &format!("SELECT created_at FROM messages WHERE conv = 'conv-A' AND created_at > {} ORDER BY created_at LIMIT 100000", thr));
    assert_eq!(created_vals(&dbg100k).len(), 100_000);
    println!("C1 DONE range={} ordered={} seq={} top_n={} t10={} t1000={} t100k={}", has_range, has_ordered, has_seq, has_top_n, t10, t1000, t100k);
}
