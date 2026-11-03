use mitrdb_server::{ExecutionResult, Server, Value};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_im_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap().0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    s
}

fn count(s: &Server) -> usize {
    s.table("t").find().run().unwrap().rows.len()
}

#[test]
fn many_rows_in_one_command() {
    let s = open("a");
    let r = s
        .execute_sql("INSERT INTO t VALUES (1,'a'), (2,'b'),(3, NULL)")
        .unwrap();
    match r {
        ExecutionResult::Affected(n) => assert_eq!(n, 3),
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(count(&s), 3);
    s.execute_sql("INSERT INTO t VALUES (4,'single')").unwrap();
    assert_eq!(count(&s), 4);
}

#[test]
fn quotes_commas_and_brackets_inside_text_are_kept() {
    let s = open("b");
    s.execute_sql("INSERT INTO t VALUES (1,'a,b'),(2,'c)(d'),(3,'it''s')").unwrap();
    let names: Vec<Value> = s
        .table("t")
        .find()
        .run()
        .unwrap()
        .rows
        .iter()
        .map(|r| r[1].clone())
        .collect();
    assert!(names.contains(&Value::Text("a,b".to_string())));
    assert!(names.contains(&Value::Text("c)(d".to_string())));
    assert!(names.contains(&Value::Text("it's".to_string())));
}

#[test]
fn one_bad_row_rolls_back_everything() {
    let s = open("c");
    s.execute_sql("INSERT INTO t VALUES (1,'x')").unwrap();
    assert!(s.execute_sql("INSERT INTO t VALUES (2,'b'),(1,'dup'),(3,'c')").is_err());
    assert_eq!(count(&s), 1);
    assert!(s.execute_sql("INSERT INTO t VALUES (5,'a'),(5,'same')").is_err());
    assert_eq!(count(&s), 1);
    assert!(s.execute_sql("INSERT INTO t VALUES (7,'a'),('x','b')").is_err());
    assert!(s.execute_sql("INSERT INTO t VALUES (8,'a'),(9)").is_err());
    assert_eq!(count(&s), 1);
}

#[test]
fn malformed_and_injection_forms_are_rejected() {
    let s = open("d");
    for q in [
        "INSERT INTO t VALUES (1,'a'),",
        "INSERT INTO t VALUES (1,'a'),(2,'b'",
        "INSERT INTO t VALUES (1,'a') (2,'b')",
        "INSERT INTO t VALUES (1,'a'),(2,'b'); DROP TABLE t",
        "INSERT INTO t VALUES (1,'a'),,(2,'b')",
        "INSERT INTO t VALUES (1,'a'),((2),'b')",
    ] {
        assert!(s.execute_sql(q).is_err(), "should fail: {q}");
    }
    assert_eq!(count(&s), 0);
    s.execute_sql("INSERT INTO t VALUES (1,'ok')").unwrap();
    assert_eq!(count(&s), 1);
}

#[test]
fn row_limit_is_enforced() {
    let s = open("e");
    let rows = |n: usize| {
        (0..n).map(|i| format!("({i},'x')")).collect::<Vec<_>>().join(",")
    };
    assert!(s.execute_sql(&format!("INSERT INTO t VALUES {}", rows(1001))).is_err());
    assert_eq!(count(&s), 0);
    s.execute_sql(&format!("INSERT INTO t VALUES {}", rows(1000))).unwrap();
    assert_eq!(count(&s), 1000);
}
