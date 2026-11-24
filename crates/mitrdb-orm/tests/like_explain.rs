use mitrdb_orm::*;

#[test]
fn like_explain_plans() {
    let dir = std::env::temp_dir().join(format!("mitrdb_likeexp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.execute_sql("CREATE TABLE w (id INT PRIMARY KEY, name TEXT NOT NULL, note TEXT)")
        .unwrap();
    db.execute_sql("CREATE INDEX idx_w_name ON w(name)")
        .unwrap();
    let idx = "IndexRangeScan(table=w, index=idx_w_name, column=name, ranges=1)";
    let seq = "SeqScan(table=w)";
    for (q, want) in [
        ("EXPLAIN SELECT * FROM w WHERE name LIKE 'ab%'", idx),
        ("EXPLAIN SELECT * FROM w WHERE name LIKE 'ab_c%'", idx),
        ("EXPLAIN SELECT * FROM w WHERE name LIKE '%bc'", seq),
        ("EXPLAIN SELECT * FROM w WHERE name LIKE '_bc'", seq),
        ("EXPLAIN SELECT * FROM w WHERE note LIKE 'ab%'", seq),
    ] {
        let r = db.execute_sql(q).unwrap();
        assert_eq!(format!("{:?}", r), format!("Explained({:?})", want), "{q}");
    }
}
