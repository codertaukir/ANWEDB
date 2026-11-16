use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Ord1 {
    #[col(primary_key, auto)]
    id: i64,
    shop: i64,
    qty: i64,
    price: f64,
    at: Timestamp,
    name: String,
}

#[test]
fn aggregates() {
    let dir = std::env::temp_dir().join(format!("mitrdb_agg_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Ord1>().unwrap();
    for (shop, qty, price, at, name) in [
        (1, 10, 2.5, 100, "b"),
        (1, 20, -1.5, 300, "a"),
        (2, 5, 9.0, 200, "c"),
    ] {
        db.insert(&Ord1 {
            id: 0,
            shop,
            qty,
            price,
            at: Timestamp(at),
            name: name.into(),
        })
        .unwrap();
    }
    assert_eq!(db.all::<Ord1>().sum(Ord1::qty).unwrap(), Some(35));
    assert_eq!(db.find(Ord1::shop.eq(1)).sum(Ord1::qty).unwrap(), Some(30));
    assert_eq!(db.find(Ord1::shop.eq(1)).avg(Ord1::qty).unwrap(), Some(15));
    assert_eq!(
        db.all::<Ord1>().max(Ord1::at).unwrap(),
        Some(Timestamp(300))
    );
    assert_eq!(
        db.all::<Ord1>().min(Ord1::at).unwrap(),
        Some(Timestamp(100))
    );
    assert_eq!(db.all::<Ord1>().min(Ord1::price).unwrap(), Some(-1.5));
    assert_eq!(db.all::<Ord1>().max(Ord1::price).unwrap(), Some(9.0));
    assert_eq!(
        db.all::<Ord1>().max(Ord1::name).unwrap(),
        Some("c".to_string())
    );
    assert_eq!(db.find(Ord1::shop.eq(99)).sum(Ord1::qty).unwrap(), None);
    assert!(db.all::<Ord1>().sum(Ord1::price).is_err()); // float SUM not supported yet
}
