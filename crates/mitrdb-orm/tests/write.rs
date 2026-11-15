use mitrdb_orm::*;

#[derive(Model, Debug, PartialEq)]
struct Item {
    #[col(primary_key)]
    id: i64,
    name: String,
    qty: Option<i32>,
}

fn it(id: i64, name: &str, qty: Option<i32>) -> Item {
    Item {
        id,
        name: name.into(),
        qty,
    }
}

#[test]
fn update_delete_find_one_by_id() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_write_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    db.sync::<Item>().unwrap();
    for i in 1..=4 {
        db.insert(&it(i, &format!("n{i}"), Some(i as i32 * 10)))
            .unwrap();
    }

    assert_eq!(
        db.find_by_id::<Item, i64>(2).unwrap(),
        Some(it(2, "n2", Some(20)))
    );
    assert_eq!(db.find_by_id::<Item, i64>(99).unwrap(), None);
    assert_eq!(
        db.find_one(Item::name.eq("n3")).unwrap(),
        Some(it(3, "n3", Some(30)))
    );
    assert_eq!(db.find_one(Item::name.eq("nope")).unwrap(), None);

    let n = db
        .update::<Item>()
        .set(Item::qty, 99)
        .filter(Item::id.eq(1))
        .run()
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        db.find_by_id::<Item, i64>(1).unwrap().unwrap().qty,
        Some(99)
    );

    db.update::<Item>()
        .set(Item::qty, None)
        .filter(Item::id.eq(2))
        .run()
        .unwrap();
    assert_eq!(db.find_by_id::<Item, i64>(2).unwrap().unwrap().qty, None);

    assert!(db.update::<Item>().set(Item::name, "x").run().is_err());
    assert_eq!(
        db.update::<Item>()
            .set(Item::name, "x")
            .all()
            .run()
            .unwrap(),
        4
    );

    assert_eq!(db.delete::<Item>().filter(Item::id.eq(4)).run().unwrap(), 1);
    assert!(db.delete::<Item>().run().is_err());
    assert_eq!(db.all::<Item>().run().unwrap().len(), 3);
    assert_eq!(db.delete::<Item>().all().run().unwrap(), 3);
    assert_eq!(db.all::<Item>().run().unwrap().len(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}
