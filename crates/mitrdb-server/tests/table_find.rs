#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, Value, val};

#[test]
fn find_filters_order_limit_and_safety() {
    let dir = std::env::temp_dir().join(format!("mitrdb_find_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    t.sync("id int primary key, name text not null, age int")
        .unwrap();
    t.insert(&[("id", val(1)), ("name", val("Taukir")), ("age", val(25))])
        .unwrap();
    t.insert(&[("id", val(2)), ("name", val("Rahul")), ("age", val(30))])
        .unwrap();
    t.insert(&[("id", val(3)), ("name", val("O'Brien")), ("age", val(35))])
        .unwrap();
    t.insert(&[("id", val(4)), ("name", val("a AND b")), ("age", val(40))])
        .unwrap();
    t.insert(&[("id", val(5)), ("name", val("x>=y")), ("age", Value::Null)])
        .unwrap();

    assert_eq!(t.find().run().unwrap().rows.len(), 5);
    assert_eq!(
        t.find()
            .filter("age", ">", val(26))
            .run()
            .unwrap()
            .rows
            .len(),
        3
    );
    assert_eq!(
        t.find()
            .filter("age", ">", val(26))
            .filter("age", "<", val(36))
            .run()
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        t.find()
            .filter("age", "=", Value::Null)
            .run()
            .unwrap()
            .rows
            .len(),
        1
    );

    // tricky text values
    assert_eq!(
        t.find()
            .filter("name", "=", val("O'Brien"))
            .run()
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        t.find()
            .filter("name", "=", val("a AND b"))
            .run()
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        t.find()
            .filter("name", "=", val("x>=y"))
            .run()
            .unwrap()
            .rows
            .len(),
        1
    );

    // order + limit
    let r = t
        .find()
        .filter("age", ">", val(0))
        .order_by_desc("age")
        .limit(2)
        .run()
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert!(matches!(&r.rows[0][0], Value::Int64(4)));

    // only some columns
    let r = t
        .find()
        .columns(&["name"])
        .filter("id", "=", val(1))
        .run()
        .unwrap();
    assert_eq!(r.rows[0].len(), 1);

    // errors
    assert!(t.find().filter("nope", "=", val(1)).run().is_err());
    assert!(t.find().filter("age", "LIKE", val(1)).run().is_err());
    assert!(t.find().filter("age", ">", Value::Null).run().is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
