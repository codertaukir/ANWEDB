use mitrdb_server::Server;

#[test]
fn composite_prefix_scan() {
    let dir = std::env::temp_dir().join(format!("mitrdb_cplan_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, chat INT, ts INT, body TEXT)")
        .unwrap();
    db.table("m")
        .create_index("m_ct", &["chat", "ts"], false)
        .unwrap();
    let mut id = 0;
    for chat in 1..=3 {
        for ts in 0..50 {
            id += 1;
            db.execute_sql(&format!("INSERT INTO m VALUES ({id}, {chat}, {ts}, 'b')"))
                .unwrap();
        }
    }
    let n = |sql: &str| match db.execute_sql(sql).unwrap() {
        mitrdb_server::ExecutionResult::Rows { rows, .. } => rows.len(),
        o => panic!("{o:?}"),
    };
    let ex = format!(
        "{:?}",
        db.execute_sql("EXPLAIN SELECT * FROM m WHERE chat = 2 AND ts = 7")
            .unwrap()
    );
    assert!(ex.contains("IndexRangeScan") && ex.contains("m_ct"), "{ex}");
    let ex1 = format!(
        "{:?}",
        db.execute_sql("EXPLAIN SELECT * FROM m WHERE chat = 2 ORDER BY id DESC LIMIT 10")
            .unwrap()
    );
    assert!(ex1.contains("m_ct"), "{ex1}");

    assert_eq!(n("SELECT * FROM m WHERE chat = 2"), 50);
    assert_eq!(n("SELECT * FROM m WHERE chat = 2 AND ts = 7"), 1);
    assert_eq!(
        n("SELECT * FROM m WHERE chat = 2 ORDER BY id DESC LIMIT 10"),
        10
    );
    assert_eq!(n("SELECT * FROM m WHERE chat = 9"), 0);
    assert_eq!(n("SELECT * FROM m WHERE chat = 2 AND body = 'b'"), 50);
    assert_eq!(n("SELECT * FROM m WHERE chat = 2 OR chat = 3"), 100);
    db.execute_sql("DELETE FROM m WHERE chat = 2 AND ts < 10")
        .unwrap();
    assert_eq!(n("SELECT * FROM m WHERE chat = 2"), 40);
    db.execute_sql("UPDATE m SET chat = 3 WHERE chat = 1 AND ts = 0")
        .unwrap();
    assert_eq!(n("SELECT * FROM m WHERE chat = 1"), 49);
    assert_eq!(n("SELECT * FROM m WHERE chat = 3 AND ts = 0"), 2);
}
