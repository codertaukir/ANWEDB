#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, Value, val};

#[test]
fn count_and_one() {
    let dir = std::env::temp_dir().join(format!("mitrdb_cnt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("u");
    t.sync("id int primary key, name text, age int").unwrap();
    assert_eq!(t.count().unwrap(), 0);

    t.insert(&[("id", val(1)), ("name", val("O'Brien")), ("age", val(20))])
        .unwrap();
    t.insert(&[("id", val(2)), ("name", val("Rahul")), ("age", val(30))])
        .unwrap();
    t.insert(&[("id", val(3)), ("name", val("Amit")), ("age", val(40))])
        .unwrap();

    assert_eq!(t.count().unwrap(), 3);
    assert_eq!(t.find().filter("age", ">", val(25)).count().unwrap(), 2);
    assert_eq!(t.find().filter("age", ">", val(99)).count().unwrap(), 0);
    assert_eq!(
        t.find()
            .filter("name", "=", val("O'Brien"))
            .count()
            .unwrap(),
        1
    );

    // one()
    let row = t.find().filter("id", "=", val(2)).one().unwrap().unwrap();
    assert!(matches!(&row[1], Value::Text(s) if s == "Rahul"));
    assert!(t.find().filter("id", "=", val(99)).one().unwrap().is_none());

    // errors
    assert!(t.find().limit(1).count().is_err());
    assert!(t.find().filter("nope", "=", val(1)).count().is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
