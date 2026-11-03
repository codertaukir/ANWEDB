use mitrdb_server::{val, Server, Value};

const ATTACKS: [&str; 8] = [
    "'; DROP TABLE users; --",
    "' OR '1'='1",
    "x' AND id > 0 --",
    "'); DELETE FROM users; --",
    "a'' OR ''1''=''1",
    "name = 'x' OR 1=1",
    "1; UPDATE users SET age = 0",
    "\\'; DROP TABLE users; --",
];

#[test]
fn attack_text_stays_plain_data() {
    let dir = std::env::temp_dir().join(format!("mitrdb_inj_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    t.sync("id int primary key, name text not null, age int").unwrap();
    t.insert(&[("id", val(1)), ("name", val("Safe")), ("age", val(10))]).unwrap();
    t.insert(&[("id", val(2)), ("name", val("Other")), ("age", val(20))]).unwrap();

    for (i, attack) in ATTACKS.iter().enumerate() {
        let id = 100 + i as i64;
        t.insert(&[("id", val(id)), ("name", val(*attack)), ("age", val(1))]).unwrap();

        let r = t.find().filter("name", "=", val(*attack)).run().unwrap();
        assert_eq!(r.rows.len(), 1, "attack not found back: {attack}");
        assert!(matches!(&r.rows[0][1], Value::Text(s) if s == attack));

        assert_eq!(
            t.update().set("name", val(*attack)).filter("id", "=", val(id)).run().unwrap(),
            1
        );
        assert_eq!(t.delete().filter("name", "=", val(*attack)).run().unwrap(), 1);
    }

    assert_eq!(t.count().unwrap(), 2);
    let r = t.find().filter("id", "=", val(1)).run().unwrap();
    assert!(matches!(&r.rows[0][1], Value::Text(s) if s == "Safe"));
}

#[test]
fn bad_names_and_operators_are_rejected() {
    let dir = std::env::temp_dir().join(format!("mitrdb_inj2_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (server, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    let t = server.table("users");
    t.sync("id int primary key, name text not null, age int").unwrap();
    t.insert(&[("id", val(1)), ("name", val("Safe")), ("age", val(10))]).unwrap();

    for bad in ["age; DROP TABLE users", "name OR 1=1", "id --", "*", "", "a b", "age)"] {
        assert!(t.find().filter(bad, "=", val(1)).run().is_err(), "filter col: {bad}");
        assert!(t.find().columns(&[bad]).run().is_err(), "columns: {bad}");
        assert!(t.find().order_by(bad).run().is_err(), "order_by: {bad}");
        assert!(t.update().set(bad, val(1)).filter("id", "=", val(1)).run().is_err(), "set: {bad}");
        assert!(t.delete().filter(bad, "=", val(1)).run().is_err(), "delete col: {bad}");
        assert!(t.insert(&[("id", val(5)), (bad, val(1))]).is_err(), "insert col: {bad}");
    }

    for op in ["=1 OR 1=1 --", "LIKE", "; DROP TABLE users", "==", "<>", ""] {
        assert!(t.find().filter("id", op, val(1)).run().is_err(), "op: {op}");
        assert!(t.delete().filter("id", op, val(1)).run().is_err(), "delete op: {op}");
    }

    for bad in ["users; DROP TABLE users", "users --", "us ers", "", "1users", "users)"] {
        let b = server.table(bad);
        assert!(b.find().run().is_err(), "find table: {bad}");
        assert!(b.count().is_err(), "count table: {bad}");
        assert!(b.insert(&[("id", val(1))]).is_err(), "insert table: {bad}");
        assert!(b.update().set("age", val(1)).all().run().is_err(), "update table: {bad}");
        assert!(b.delete().all().run().is_err(), "delete table: {bad}");
        assert!(b.sync("id int primary key").is_err(), "sync table: {bad}");
    }

    assert_eq!(t.count().unwrap(), 1);
}
