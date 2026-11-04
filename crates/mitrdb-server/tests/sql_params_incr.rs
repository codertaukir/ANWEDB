use mitrdb_server::{val, Server};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_spi_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

#[test]
fn param_increment_add_sub_and_errors() {
    let d = dir("incr");
    let s = open(&d);
    let t = s.table("t");
    t.sync("id int primary key, n int").unwrap();
    t.insert(&[("id", val(1)), ("n", val(10))]).unwrap();
    t.insert(&[("id", val(2)), ("n", val(100))]).unwrap();

    s.execute_sql_params("UPDATE t SET n = n + $1 WHERE id = $2", &[val(5), val(1)]).unwrap();
    assert_eq!(t.find().filter("id", "=", val(1)).one().unwrap().unwrap()[1], val(15));

    s.execute_sql_params("UPDATE t SET n = n - $1 WHERE id = $2", &[val(3), val(1)]).unwrap();
    assert_eq!(t.find().filter("id", "=", val(1)).one().unwrap().unwrap()[1], val(12));

    // other row untouched
    assert_eq!(t.find().filter("id", "=", val(2)).one().unwrap().unwrap()[1], val(100));

    // non-integer param, missing param, plain execute_sql: all errors, data unchanged
    assert!(s.execute_sql_params("UPDATE t SET n = n + $1 WHERE id = $2", &[val("x"), val(1)]).is_err());
    assert!(s.execute_sql_params("UPDATE t SET n = n + $1 WHERE id = $2", &[val(1)]).is_err());
    assert!(s.execute_sql("UPDATE t SET n = n + $1 WHERE id = 1").is_err());
    assert_eq!(t.find().filter("id", "=", val(1)).one().unwrap().unwrap()[1], val(12));

    // literal increments still work
    s.execute_sql("UPDATE t SET n = n + 8 WHERE id = 1").unwrap();
    assert_eq!(t.find().filter("id", "=", val(1)).one().unwrap().unwrap()[1], val(20));
    let _ = std::fs::remove_dir_all(&d);
}
