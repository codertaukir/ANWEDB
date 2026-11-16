use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "eg_row")]
struct EgRow {
    #[col(primary_key)]
    id: i64,
    v: i64,
}

#[test]
fn empty_or_group_never_deletes_or_updates_everything() {
    let dir = std::env::temp_dir().join(format!("mitrdb_eg_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<EgRow>().unwrap();
    for i in 1..=5 {
        db.insert(&EgRow { id: i, v: i }).unwrap();
    }
    let none: Vec<i64> = vec![];
    // empty group alone: must not wipe the table (error or 0 rows are both fine)
    let r = db
        .delete::<EgRow>()
        .filter_any(EgRow::v.in_list(none.clone()))
        .run();
    eprintln!("EG delete empty group -> {:?}", r);
    assert!(r.is_err() || r.unwrap() == 0);
    assert_eq!(db.count::<EgRow>().unwrap(), 5);
    let r = db
        .update::<EgRow>()
        .set(EgRow::v, 99)
        .filter_any(EgRow::v.in_list(none))
        .run();
    eprintln!("EG update empty group -> {:?}", r);
    assert_eq!(db.count_where(EgRow::v.eq(99)).unwrap(), 0);
    // non-empty group works for delete and update
    assert_eq!(
        db.delete::<EgRow>()
            .filter_any(EgRow::v.in_list([1, 2]))
            .run()
            .unwrap(),
        2
    );
    assert_eq!(
        db.update::<EgRow>()
            .set(EgRow::v, 99)
            .filter_any(EgRow::v.in_list([3]))
            .run()
            .unwrap(),
        1
    );
    assert_eq!(db.count::<EgRow>().unwrap(), 3);
    // still no filter at all -> still refused
    assert!(db.delete::<EgRow>().run().is_err());
}
