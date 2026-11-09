// UPDATE that leaves the indexed column (and pk) unchanged must not lock that index entry.
use mitrdb_server::Server;

fn open(tag: &str) -> (Server, std::path::PathBuf) {
    let d = std::env::temp_dir().join(format!("upd_idxlock_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 256).unwrap().0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, u INT, name TEXT)").unwrap();
    s.execute_sql("CREATE INDEX t_u ON t(u)").unwrap();
    s.execute_sql("INSERT INTO t VALUES (1, 5, 'a'), (2, 6, 'b')").unwrap();
    (s, d)
}

#[test]
fn unchanged_index_key_not_locked() {
    let (s, d) = open("a");
    let s1 = s.create_session();
    let s2 = s.create_session();
    s.execute_in_session(s1, "BEGIN").unwrap();
    s.execute_in_session(s1, "UPDATE t SET name = 'x' WHERE id = 1").unwrap();
    s.execute_in_session(s2, "BEGIN").unwrap();
    let r = s.execute_in_session(s2, "UPDATE t SET u = 5 WHERE id = 2");
    assert!(r.is_ok(), "s2 must not block on index key 5 held by an unchanged-key update: {r:?}");
    s.execute_in_session(s2, "COMMIT").unwrap();
    s.execute_in_session(s1, "COMMIT").unwrap();
    let got = format!("{:?}", s.execute_sql("SELECT id FROM t WHERE u = 5").unwrap());
    println!("AFTER {got}");
    assert!(got.contains('1') && got.contains('2'), "both rows must be visible under u=5: {got}");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn same_row_still_conflicts() {
    let (s, d) = open("b");
    let s1 = s.create_session();
    let s2 = s.create_session();
    s.execute_in_session(s1, "BEGIN").unwrap();
    s.execute_in_session(s1, "UPDATE t SET name = 'x' WHERE id = 1").unwrap();
    s.execute_in_session(s2, "BEGIN").unwrap();
    let r = s.execute_in_session(s2, "UPDATE t SET name = 'y' WHERE id = 1");
    assert!(r.is_err(), "same-row update must still conflict: {r:?}");
    let _ = s.execute_in_session(s2, "ROLLBACK");
    s.execute_in_session(s1, "COMMIT").unwrap();
    let _ = std::fs::remove_dir_all(&d);
}
