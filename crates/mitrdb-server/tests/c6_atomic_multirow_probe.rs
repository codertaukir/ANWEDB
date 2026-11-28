// C6: Atomic multi-row insert in txn: 1 message + N envelopes + N deliveries, rollback pura
use std::fs;
fn open_server(dir: &std::path::Path) -> mitrdb_server::Server {
    mitrdb_server::Server::open(dir.join("t.db"), dir.join("t.wal"), 512).expect("open").0
}
fn exec_timed(s: &mitrdb_server::Server, sql: &str) {
    s.execute_sql(sql).unwrap_or_else(|e| panic!("SQL {} | {}", e, sql));
}
fn exec_debug(s: &mitrdb_server::Server, sql: &str) -> String {
    format!("{:?}", s.execute_sql(sql).unwrap_or_else(|e| panic!("SQL {} | {}", e, sql)))
}
fn count_val(s: &mitrdb_server::Server, sql: &str) -> i64 {
    let dbg = exec_debug(s, sql);
    let marker = "Int64(";
    let pos = dbg.find(marker).expect("no Int64 in COUNT debug");
    let rest = &dbg[pos + marker.len()..];
    let end = rest.find(')').expect("no ) after Int64");
    rest[..end].parse::<i64>().expect("COUNT not int")
}
#[test]
fn c6_atomic_multirow() {
    let dir = std::env::temp_dir().join(format!("mitrdb-c6-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    fs::create_dir_all(&dir).unwrap();
    let srv = open_server(&dir);
    exec_debug(&srv, "CREATE TABLE messages (id INT PRIMARY KEY, conv TEXT, created_at INT)");
    exec_debug(&srv, "CREATE TABLE envelopes (id INT PRIMARY KEY, msg_id INT, seq INT)");
    exec_debug(&srv, "CREATE TABLE deliveries (id INT PRIMARY KEY, envelope_id INT, state TEXT)");
    // COMMIT case: 1 + 20 + 20
    exec_debug(&srv, "BEGIN");
    exec_timed(&srv, "INSERT INTO messages (id, conv, created_at) VALUES (1, 'conv-A', 1000)");
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO envelopes (id, msg_id, seq) VALUES ({}, 1, {})", 100 + i, i));
    }
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO deliveries (id, envelope_id, state) VALUES ({}, {}, 'queued')", 1000 + i, 100 + i));
    }
    exec_debug(&srv, "COMMIT");
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM messages"), 1);
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM envelopes"), 20);
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM deliveries"), 20);
    println!("C6 commit ok: 1+20+20");
    // ROLLBACK case: 1 + 20 + 20, pura rollback hona chahiye
    exec_debug(&srv, "BEGIN");
    exec_timed(&srv, "INSERT INTO messages (id, conv, created_at) VALUES (2, 'conv-A', 2000)");
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO envelopes (id, msg_id, seq) VALUES ({}, 2, {})", 200 + i, i));
    }
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO deliveries (id, envelope_id, state) VALUES ({}, {}, 'queued')", 2000 + i, 200 + i));
    }
    exec_debug(&srv, "ROLLBACK");
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM messages"), 1, "rollback ke baad message bach gaya");
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM envelopes"), 20, "rollback ke baad envelope bach gaya");
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM deliveries"), 20, "rollback ke baad delivery bach gayi");
    println!("C6 rollback ok: counts 1/20/20");
    // Rollback wale IDs dobara daal sakte hain, phantom nahi hona chahiye
    exec_debug(&srv, "BEGIN");
    exec_timed(&srv, "INSERT INTO messages (id, conv, created_at) VALUES (2, 'conv-A', 2000)");
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO envelopes (id, msg_id, seq) VALUES ({}, 2, {})", 200 + i, i));
    }
    for i in 0..20 {
        exec_timed(&srv, &format!("INSERT INTO deliveries (id, envelope_id, state) VALUES ({}, {}, 'queued')", 2000 + i, 200 + i));
    }
    exec_debug(&srv, "COMMIT");
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM messages"), 2);
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM envelopes"), 40);
    assert_eq!(count_val(&srv, "SELECT COUNT(*) FROM deliveries"), 40);
    println!("C6 DONE commit=1+20+20 rollback=0 reinsert=1+20+20");
}
