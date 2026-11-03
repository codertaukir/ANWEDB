use mitrdb_server::{ExecutionResult, Server};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_ine_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap().0
}

fn count(s: &Server) -> usize {
    s.table("t").find().run().unwrap().rows.len()
}

#[test]
fn create_table_if_not_exists() {
    let s = open("a");
    let r = s.execute_sql("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, name TEXT)");
    assert!(matches!(r.unwrap(), ExecutionResult::Ok));
    s.execute_sql("INSERT INTO t VALUES (1,'a')").unwrap();
    let r = s.execute_sql("create table if not exists t (id INT PRIMARY KEY)");
    assert!(matches!(r.unwrap(), ExecutionResult::Affected(0)));
    assert_eq!(count(&s), 1);
    s.execute_sql("INSERT INTO t VALUES (2,'b')").unwrap();
    assert!(s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)").is_err());
}

#[test]
fn create_index_if_not_exists() {
    let s = open("b");
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    s.execute_sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON t (name)").unwrap();
    let r = s.execute_sql("create index if not exists idx_name on t (name)");
    assert!(matches!(r.unwrap(), ExecutionResult::Affected(0)));
    assert!(s.execute_sql("CREATE INDEX idx_name ON t (name)").is_err());
    s.execute_sql("INSERT INTO t VALUES (1,'a')").unwrap();
    assert!(s.execute_sql("INSERT INTO t VALUES (2,'a')").is_err());
}

#[test]
fn bad_forms_are_rejected() {
    let s = open("c");
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    for q in [
        "CREATE TABLE IF NOT EXISTS (id INT PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS",
        "CREATE TABLE IF NOT EXISTSx (id INT PRIMARY KEY)",
        "CREATE INDEX IF NOT EXISTS ON t (id)",
        "CREATE INDEX IF NOT EXISTS i ON missing (id)",
    ] {
        assert!(s.execute_sql(q).is_err(), "should fail: {q}");
    }
    assert_eq!(count(&s), 0);
}

#[test]
fn trailing_semicolon_is_accepted() {
    let s = open("d");
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT);").unwrap();
    s.execute_sql("INSERT INTO t VALUES (1,'a');").unwrap();
    s.execute_sql("INSERT INTO t VALUES (2,'b'),(3,'c');").unwrap();
    assert_eq!(count(&s), 3);
}
