use mitrdb_orm::*;

#[derive(Model, Debug, PartialEq)]
struct Prod {
    #[col(primary_key)]
    id: i64,
    price: f64,
}

#[test]
fn f64_roundtrip_and_order() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_f64_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Prod>().unwrap();
    for (i, p) in [3.5, -1.0, 0.0, -2.5, 1e-9].iter().enumerate() {
        db.insert(&Prod {
            id: i as i64,
            price: *p,
        })
        .unwrap();
    }
    let got: Vec<f64> = db
        .all::<Prod>()
        .order_by(Prod::price)
        .run()
        .unwrap()
        .into_iter()
        .map(|p| p.price)
        .collect();
    assert_eq!(got, vec![-2.5, -1.0, 0.0, 1e-9, 3.5]);
}
