use mitrdb_server::{F64, Server, Value};

fn f(x: f64) -> Value {
    Value::Float(F64::new(x))
}

#[test]
fn native_float() {
    let dir = std::env::temp_dir().join(format!("mitrdb_flt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db_p, wal_p) = (dir.join("t.db"), dir.join("t.wal"));
    {
        let (db, _) = Server::open(&db_p, &wal_p, 64).unwrap();
        db.execute_sql("CREATE TABLE m (id INT PRIMARY KEY, p FLOAT)")
            .unwrap();
        db.execute_sql("CREATE INDEX m_p ON m (p)").unwrap();
        for (i, x) in [3.5, -1.0, 0.0, -2.5, 1e-9].iter().enumerate() {
            db.execute_sql_params(
                "INSERT INTO m VALUES ($1, $2)",
                &[Value::Int64(i as i64), f(*x)],
            )
            .unwrap();
        }
        db.execute_sql("INSERT INTO m VALUES (10, 7.25)").unwrap(); // float literal
        assert!(
            db.execute_sql_params(
                "INSERT INTO m VALUES ($1, $2)",
                &[Value::Int64(11), Value::Int64(1)]
            )
            .is_err()
        );
        assert!(
            db.execute_sql_params(
                "INSERT INTO m VALUES ($1, $2)",
                &[Value::Int64(12), f(f64::NAN)]
            )
            .is_err()
        );
    }
    let (db, _) = Server::open(&db_p, &wal_p, 64).unwrap(); // tag 6 reload
    let t = db.table("m");
    let r = t.find().order_by("p").run().unwrap();
    let ps: Vec<f64> = r
        .rows
        .iter()
        .map(|x| match &x[1] {
            Value::Float(v) => v.0,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ps, vec![-2.5, -1.0, 0.0, 1e-9, 3.5, 7.25]);
    assert_eq!(
        t.find().filter("p", ">=", f(0.0)).run().unwrap().rows.len(),
        4
    );
    assert_eq!(
        t.find().filter("p", "<", f(-1.0)).run().unwrap().rows.len(),
        1
    );
    t.sync("id int primary key, p float").unwrap();
}
