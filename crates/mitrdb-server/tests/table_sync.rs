use mitrdb_server::{ExecutionResult, Server};

fn rows(server: &Server, sql: &str) -> usize {
    match server.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn sync_keeps_data_and_is_repeatable() {
    let dir = std::env::temp_dir().join(format!("mitrdb_sync_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    assert_eq!(t.sync("id int primary key, name text").unwrap().len(), 1);
    server.execute_sql("INSERT INTO users VALUES (1, 'Taukir')").unwrap();

    // same definition again: nothing to do
    assert!(t.sync("id int primary key, name text").unwrap().is_empty());

    // new column: added, data kept
    let plan = t.sync("id int primary key, name text, age int").unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(rows(&server, "SELECT * FROM users"), 1);

    // removing a column or changing type is refused
    assert!(t.sync("id int primary key, age int").is_err());
    assert!(t.sync("id int primary key, name int, age int").is_err());

    // rename keeps data
    t.rename("age", "years").unwrap();
    assert_eq!(rows(&server, "SELECT * FROM users"), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
