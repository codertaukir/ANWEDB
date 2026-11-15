use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Person {
    #[col(primary_key, auto)]
    id: i64,
    #[col(index)]
    name: String,
    note: Option<String>,
}

#[test]
fn orm_like() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_like_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Person>().unwrap();
    for (n, o) in [("anil", Some("hi")), ("amit", None), ("raj", Some("ami"))] {
        db.insert(&Person {
            id: 0,
            name: n.into(),
            note: o.map(String::from),
        })
        .unwrap();
    }
    assert_eq!(db.count_where(Person::name.like("a%")).unwrap(), 2);
    assert_eq!(db.count_where(Person::name.like("_aj")).unwrap(), 1);
    assert_eq!(db.count_where(Person::note.like("%i")).unwrap(), 2);
    assert_eq!(db.find(Person::name.like("am%")).run().unwrap().len(), 1);
}
