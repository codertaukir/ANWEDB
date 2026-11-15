use mitrdb_orm::*;

#[derive(Model, Debug, PartialEq)]
#[model(table = "people")]
struct Person {
    #[col(primary_key)]
    id: i64,
    #[col(unique)]
    email: String,
    #[col(index)]
    name: String,
    age: Option<i32>,
    active: bool,
}

#[derive(Model, Debug, PartialEq)]
struct Product {
    #[col(primary_key)]
    id: i64,
    title: String,
}

fn p(id: i64, email: &str, name: &str, age: Option<i32>, active: bool) -> Person {
    Person {
        id,
        email: email.into(),
        name: name.into(),
        age,
        active,
    }
}

#[test]
fn derive_roundtrip() {
    assert_eq!(Person::TABLE, "people");
    assert_eq!(Product::TABLE, "products");
    let spec = spec_of(Person::COLUMNS);
    println!("spec: {spec}");

    let dir = std::env::temp_dir().join(format!("mitrdb_orm_derive_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    db.sync::<Person>().unwrap();
    db.sync::<Person>().unwrap();
    db.sync::<Product>().unwrap();

    db.insert(&p(1, "a@x.com", "Asha", Some(25), true)).unwrap();
    db.insert(&p(2, "b@x.com", "Bilal", Some(30), false))
        .unwrap();
    db.insert(&p(3, "c@x.com", "Chitra", None, true)).unwrap();
    db.insert(&Product {
        id: 1,
        title: "pen".into(),
    })
    .unwrap();

    let adults = db.find(Person::age.ge(30)).run().unwrap();
    assert_eq!(adults, vec![p(2, "b@x.com", "Bilal", Some(30), false)]);

    let active = db
        .find(Person::active.eq(true))
        .order_by_desc(Person::id)
        .run()
        .unwrap();
    assert_eq!(active.iter().map(|x| x.id).collect::<Vec<_>>(), vec![3, 1]);

    let by_name = db.find(Person::name.eq("Asha")).run().unwrap();
    assert_eq!(by_name.len(), 1);

    let prods = db.all::<Product>().run().unwrap();
    assert_eq!(
        prods,
        vec![Product {
            id: 1,
            title: "pen".into()
        }]
    );

    let _ = std::fs::remove_dir_all(&dir);
}
