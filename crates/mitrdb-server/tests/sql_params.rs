use mitrdb_server::{val, Server};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_sp_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

#[test]
fn injection_strings_are_plain_data() {
    let d = dir("inj");
    let s = open(&d);
    let t = s.table("users");
    t.sync("id int primary key, name text").unwrap();
    t.insert(&[("id", val(1)), ("name", val("x"))]).unwrap();
    t.insert(&[("id", val(2)), ("name", val("y"))]).unwrap();

    for evil in ["x' OR '1'='1", "a;b", "x\" OR \"1\"=\"1", "q AND r OR s", "$1", "'; DROP TABLE users; --"] {
        assert_eq!(t.find().filter("name", "=", val(evil)).count().unwrap(), 0, "{evil}");
        assert_eq!(t.delete().filter("name", "=", val(evil)).run().unwrap(), 0, "{evil}");
    }
    assert_eq!(t.find().count().unwrap(), 2);

    let weird = "it's $1; DROP TABLE users -- AND x OR y";
    assert_eq!(t.update().set("name", val(weird)).filter("id", "=", val(1)).run().unwrap(), 1);
    assert_eq!(t.find().filter("name", "=", val(weird)).count().unwrap(), 1);
    assert_eq!(t.find().count().unwrap(), 2);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn execute_sql_params_binds_and_validates_counts() {
    let d = dir("bind");
    let s = open(&d);
    let t = s.table("t");
    t.sync("id int primary key, name text").unwrap();

    assert!(s.execute_sql_params("INSERT INTO t VALUES ($1, $2)", &[val(1), val("a'b")]).is_ok());
    assert_eq!(t.find().filter("name", "=", val("a'b")).count().unwrap(), 1);

    // quoted $1 stays literal text, not a placeholder
    assert!(s.execute_sql_params("INSERT INTO t VALUES ($1, '$1')", &[val(2)]).is_ok());
    assert_eq!(t.find().filter("name", "=", val("$1")).count().unwrap(), 1);

    // missing / extra / bad placeholders
    assert!(s.execute_sql_params("INSERT INTO t VALUES ($1, $2)", &[val(3)]).is_err());
    assert!(s.execute_sql_params("INSERT INTO t VALUES ($1, 'z')", &[val(3), val(4)]).is_err());
    assert!(s.execute_sql_params("INSERT INTO t VALUES ($0, 'z')", &[val(3)]).is_err());
    assert!(s.execute_sql_params("INSERT INTO t VALUES ($x, 'z')", &[val(3)]).is_err());

    // plain execute_sql must not accept placeholders
    assert!(s.execute_sql("INSERT INTO t VALUES ($1, 'z')").is_err());
    assert_eq!(t.find().count().unwrap(), 2);
    let _ = std::fs::remove_dir_all(&d);
}
