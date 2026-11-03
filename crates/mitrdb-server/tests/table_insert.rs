use mitrdb_server::{val, ExecutionResult, Server, Value};

#[test]
fn insert_any_order_nulls_and_errors() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ins_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    t.sync("id int primary key, name text not null, age int").unwrap();

    // columns in different order, age missing -> NULL
    assert_eq!(t.insert(&[("name", val("Taukir")), ("id", val(1))]).unwrap(), 1);

    match server.execute_sql("SELECT * FROM users").unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(&rows[0][0], Value::Int64(1)));
            assert!(matches!(&rows[0][1], Value::Text(s) if s == "Taukir"));
            assert!(matches!(&rows[0][2], Value::Null));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // duplicate primary key
    assert!(t.insert(&[("id", val(1)), ("name", val("Rahul"))]).is_err());
    // NOT NULL column missing
    assert!(t.insert(&[("id", val(2))]).is_err());
    // wrong type
    assert!(t.insert(&[("id", val("two")), ("name", val("X"))]).is_err());
    // unknown column
    assert!(t.insert(&[("id", val(3)), ("name", val("X")), ("city", val("Y"))]).is_err());
    // value with a quote works (no SQL string involved)
    assert_eq!(t.insert(&[("id", val(4)), ("name", val("O'Brien"))]).unwrap(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
