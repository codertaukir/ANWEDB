use mitrdb_server::{Server, Value};

#[test]
fn native_timestamp() {
    let dir = std::env::temp_dir().join(format!("mitrdb_tsn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db_p, wal_p) = (dir.join("t.db"), dir.join("t.wal"));
    {
        let (db, _) = Server::open(&db_p, &wal_p, 64).unwrap();
        db.execute_sql("CREATE TABLE ev (id INT PRIMARY KEY, at TIMESTAMP, n INT)")
            .unwrap();
        db.execute_sql("CREATE INDEX ev_at ON ev (at)").unwrap();
        for (i, t) in [30i64, -5, 10, 20].iter().enumerate() {
            db.execute_sql_params(
                "INSERT INTO ev VALUES ($1, $2, $3)",
                &[Value::Int64(i as i64), Value::Int64(*t), Value::Int64(1)],
            )
            .unwrap();
        }
        assert!(
            db.execute_sql_params(
                "INSERT INTO ev VALUES ($1, $2, $3)",
                &[Value::Int64(9), Value::Text("x".into()), Value::Int64(1)]
            )
            .is_err()
        );
    }
    let (db, _) = Server::open(&db_p, &wal_p, 64).unwrap(); // catalog tag 5 reload
    let t = db.table("ev");
    let r = t.find().order_by("at").run().unwrap();
    let ats: Vec<i64> = r
        .rows
        .iter()
        .map(|x| match x[1] {
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ats, vec![-5, 10, 20, 30]);
    assert_eq!(
        t.find()
            .filter("at", ">=", Value::Int64(20))
            .run()
            .unwrap()
            .rows
            .len(),
        2
    );
    t.sync("id int primary key, at timestamp, n int").unwrap(); // sync sees same type, no change
}
