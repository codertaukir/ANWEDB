use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Prod {
    #[col(primary_key, auto)]
    id: i64,
    #[col(check = ">= 0", check = "< 1000")]
    qty: i64,
    #[col(check = "like 'A%'")]
    code: String,
    #[col(check = "> 0")]
    opt: Option<i64>,
}

fn p(q: i64, c: &str, o: Option<i64>) -> Prod {
    Prod {
        id: 0,
        qty: q,
        code: c.into(),
        opt: o,
    }
}

#[test]
fn check_constraints() {
    let dir = std::env::temp_dir().join(format!("mitrdb_chk_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Prod>().unwrap();
    db.sync::<Prod>().unwrap(); // idempotent

    let id = db.insert_get_id(&p(5, "A1", None)).unwrap(); // NULL passes
    assert!(db.insert(&p(-1, "A1", None)).is_err());
    assert!(db.insert(&p(1000, "A1", None)).is_err());
    assert!(db.insert(&p(5, "B1", None)).is_err());
    assert!(db.insert(&p(5, "A1", Some(0))).is_err());
    assert!(
        db.update::<Prod>()
            .set(Prod::qty, -5i64)
            .filter(Prod::id.eq(id))
            .run()
            .is_err()
    );
    assert_eq!(
        db.update::<Prod>()
            .set(Prod::qty, 3i64)
            .filter(Prod::id.eq(id))
            .run()
            .unwrap(),
        1
    );
    assert_eq!(db.count::<Prod>().unwrap(), 1);
}
