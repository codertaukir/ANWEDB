use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "g_parent")]
struct GParent {
    #[col(primary_key)]
    id: i64,
}

#[derive(Model, Debug)]
#[model(table = "g_child")]
struct GChild {
    #[col(primary_key)]
    id: i64,
    #[col(references = "g_parent.id")]
    parent_id: i64,
}

fn refused<T: std::fmt::Debug, E: std::fmt::Display>(r: std::result::Result<T, E>, tag: &str) {
    let e = r.expect_err(tag).to_string();
    assert!(e.contains("refused"), "{tag}: {e}");
}

#[test]
fn raw_sql_guard() {
    let dir = std::env::temp_dir().join(format!("mitrdb_guard_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    // no rules: raw SQL works as before
    db.execute_sql("CREATE TABLE g_plain (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute_sql("INSERT INTO g_plain VALUES (1, 1)").unwrap();
    db.execute_sql("UPDATE g_plain SET n = 2 WHERE id = 1").unwrap();
    db.execute_sql("DELETE FROM g_plain WHERE id = 1").unwrap();

    // CHECK
    let c = db.table("g_chk");
    c.sync("id int primary key, n int").unwrap();
    c.add_check("n", ">=", Value::Int64(0)).unwrap();
    refused(db.execute_sql("INSERT INTO g_chk VALUES (1, -5)"), "check insert");
    refused(db.execute_sql_params("INSERT INTO g_chk VALUES ($1, $2)", &[Value::Int64(1), Value::Int64(-5)]), "check params");
    c.insert(&[("id", Value::Int64(1)), ("n", Value::Int64(3))]).unwrap();
    refused(db.execute_sql("UPDATE g_chk SET n = -1 WHERE id = 1"), "check update");
    refused(db.execute_sql("DELETE FROM g_chk WHERE id = 1"), "check delete");
    assert!(c.insert(&[("id", Value::Int64(2)), ("n", Value::Int64(-1))]).is_err());
    db.execute_sql("SELECT * FROM g_chk").unwrap();

    // DEFAULT
    let d = db.table("g_def");
    d.sync("id int primary key, n int").unwrap();
    d.set_default("n", Value::Int64(5)).unwrap();
    refused(db.execute_sql("INSERT INTO g_def VALUES (1, 1)"), "default insert");
    d.insert(&[("id", Value::Int64(1))]).unwrap();

    // FK (child and parent side)
    db.sync::<GParent>().unwrap();
    db.sync::<GChild>().unwrap();
    db.insert(&GParent { id: 1 }).unwrap();
    refused(db.execute_sql("INSERT INTO g_child VALUES (1, 99)"), "fk child insert");
    refused(db.execute_sql("DELETE FROM g_parent WHERE id = 1"), "fk parent delete");
    db.insert(&GChild { id: 1, parent_id: 1 }).unwrap();
    assert_eq!(db.all::<GChild>().run().unwrap().len(), 1);
}
