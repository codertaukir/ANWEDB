use mitrdb_server::{val, Server, Value};

#[test]
fn update_and_delete_with_safety_rules() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ud_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    t.sync("id int primary key, name text not null, age int").unwrap();
    for (id, name, age) in [(1, "Taukir", 25), (2, "Rahul", 30), (3, "Amit", 35), (4, "Neha", 40)] {
        t.insert(&[("id", val(id)), ("name", val(name)), ("age", val(age))]).unwrap();
    }

    // update one row
    assert_eq!(t.update().set("age", val(26)).filter("id", "=", val(1)).run().unwrap(), 1);
    let r = t.find().filter("id", "=", val(1)).run().unwrap();
    assert!(matches!(&r.rows[0][2], Value::Int64(26)));

    // update many rows, tricky text value
    assert_eq!(
        t.update().set("name", val("O'Brien, a = b")).filter("age", ">", val(29)).run().unwrap(),
        3
    );
    assert_eq!(t.find().filter("name", "=", val("O'Brien, a = b")).run().unwrap().rows.len(), 3);

    // set to NULL
    assert_eq!(t.update().set("age", Value::Null).filter("id", "=", val(2)).run().unwrap(), 1);
    assert_eq!(t.find().filter("age", "=", Value::Null).run().unwrap().rows.len(), 1);

    // safety / error cases
    assert!(t.update().set("age", val(1)).run().is_err()); // no filter
    assert!(t.update().filter("id", "=", val(1)).run().is_err()); // no set
    assert!(t.update().set("id", val(9)).filter("id", "=", val(1)).run().is_err()); // pk
    assert!(t.update().set("age", val("x")).filter("id", "=", val(1)).run().is_err()); // type
    assert!(t.update().set("name", Value::Null).filter("id", "=", val(1)).run().is_err()); // not null
    assert!(t.update().set("nope", val(1)).filter("id", "=", val(1)).run().is_err());
    assert!(t.delete().run().is_err()); // no filter
    assert_eq!(t.find().run().unwrap().rows.len(), 4); // nothing was lost

    // update all
    assert_eq!(t.update().set("age", val(50)).all().run().unwrap(), 4);

    // delete with filter, then delete all
    assert_eq!(t.delete().filter("id", "=", val(1)).run().unwrap(), 1);
    assert_eq!(t.find().run().unwrap().rows.len(), 3);
    assert_eq!(t.delete().all().run().unwrap(), 3);
    assert_eq!(t.find().run().unwrap().rows.len(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}
