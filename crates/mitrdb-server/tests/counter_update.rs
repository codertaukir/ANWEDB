use mitrdb_server::Server;

fn num(s: &str) -> i64 {
    s[s.find('(').unwrap() + 1..s.rfind(')').unwrap()]
        .trim()
        .parse()
        .unwrap()
}
fn n(s: &Server) -> i64 {
    let r = s.table("c").find().columns(&["n"]).run().unwrap().rows;
    assert_eq!(r.len(), 1);
    num(&format!("{:?}", r[0][0]))
}

#[test]
fn counter_n_plus_1_autocommit_txn_rollback_reopen() {
    let d = std::env::temp_dir().join(format!("mitrdb_counter_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let open = || Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0;
    {
        let s = open();
        s.execute_sql("CREATE TABLE c (k INT PRIMARY KEY, n INT)")
            .unwrap();
        s.execute_sql("INSERT INTO c VALUES (1, 0)").unwrap();
        for _ in 0..100 {
            s.execute_sql("UPDATE c SET n = n + 1 WHERE k = 1").unwrap();
        }
        assert_eq!(n(&s), 100);
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("UPDATE c SET n = n + 1 WHERE k = 1").unwrap();
        s.execute_sql("ROLLBACK").unwrap();
        assert_eq!(n(&s), 100);
        s.execute_sql("BEGIN").unwrap();
        s.execute_sql("UPDATE c SET n = n + 1 WHERE k = 1").unwrap();
        s.execute_sql("COMMIT").unwrap();
        assert_eq!(n(&s), 101);
        drop(s);
    }
    let s = open();
    assert_eq!(n(&s), 101);
}
