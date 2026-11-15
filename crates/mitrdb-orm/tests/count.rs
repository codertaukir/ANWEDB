use mitrdb_orm::*;

#[derive(Model, Debug, PartialEq)]
struct Row {
    #[col(primary_key)]
    id: i64,
    age: Option<i32>,
}

#[test]
fn count_all_and_where() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_count_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    db.sync::<Row>().unwrap();
    assert_eq!(db.count::<Row>().unwrap(), 0);
    for (id, age) in [(1, Some(20)), (2, Some(30)), (3, Some(40)), (4, None)] {
        db.insert(&Row { id, age }).unwrap();
    }
    assert_eq!(db.count::<Row>().unwrap(), 4);
    assert_eq!(db.count_where(Row::age.gt(25)).unwrap(), 2);
    assert_eq!(db.count_where(Row::age.gt(99)).unwrap(), 0);
    assert!(db.all::<Row>().limit(2).count().is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
