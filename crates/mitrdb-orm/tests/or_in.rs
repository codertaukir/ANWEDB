use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Msg {
    #[col(primary_key, auto)]
    id: i64,
    age: i64,
    note: Option<String>,
}

#[test]
fn or_in_between_null() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orin_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Msg>().unwrap();
    for a in [1, 5, 10, 20, 70] {
        let n = if a == 5 { None } else { Some("x".to_string()) };
        db.insert(&Msg {
            id: 0,
            age: a,
            note: n,
        })
        .unwrap();
    }
    let ages = |v: Vec<Msg>| v.into_iter().map(|m| m.age).collect::<Vec<_>>();
    assert_eq!(
        ages(
            db.all::<Msg>()
                .filter_any(Msg::age.lt(5).or(Msg::age.gt(60)))
                .order_by(Msg::age)
                .run()
                .unwrap()
        ),
        vec![1, 70]
    );
    assert_eq!(
        ages(
            db.all::<Msg>()
                .filter_any(Msg::age.in_list([5, 20, 99]))
                .order_by_desc(Msg::age)
                .run()
                .unwrap()
        ),
        vec![20, 5]
    );
    assert_eq!(
        ages(
            db.all::<Msg>()
                .filter_between(Msg::age.between(5, 20))
                .order_by(Msg::age)
                .run()
                .unwrap()
        ),
        vec![5, 10, 20]
    );
    assert_eq!(db.count_where(Msg::note.is_null()).unwrap(), 1);
    assert_eq!(
        db.all::<Msg>()
            .filter_any(Msg::age.in_list([1, 5, 10]))
            .filter(Msg::note.is_not_null())
            .run()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.delete::<Msg>()
            .filter_any(Msg::age.in_list([1, 70]))
            .run()
            .unwrap(),
        2
    );
    assert_eq!(db.count::<Msg>().unwrap(), 3);
    assert!(
        db.all::<Msg>()
            .filter_any(Msg::age.in_list(Vec::<i64>::new()))
            .run()
            .is_err()
    );
}
