use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "gp")]
struct Gp {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}
#[derive(Model, Debug)]
#[model(table = "pa")]
struct Pa {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "gp.id", on_delete = "cascade")]
    gp_id: i64,
}
#[derive(Model, Debug)]
#[model(table = "ch")]
struct Ch {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "pa.id", on_delete = "cascade")]
    pa_id: i64,
}
#[derive(Model, Debug)]
#[model(table = "rs")]
struct Rs {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "gp.id")]
    gp_id: i64,
}
#[derive(Model, Debug, PartialEq)]
#[model(table = "task")]
struct Task {
    #[col(primary_key, auto)]
    id: i64,
    title: String,
    #[col(default = "'open'")]
    status: Option<String>,
    #[col(default = "5")]
    prio: Option<i64>,
    #[col(default = "false")]
    done: Option<bool>,
}

#[test]
fn defaults_and_cascade() {
    let dir = std::env::temp_dir().join(format!("mitrdb_dc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Gp>().unwrap();
    db.sync::<Pa>().unwrap();
    db.sync::<Ch>().unwrap();
    db.sync::<Rs>().unwrap();
    db.sync::<Task>().unwrap();
    db.sync::<Pa>().unwrap(); // idempotent

    // defaults
    let a = db
        .insert_get_id(&Task {
            id: 0,
            title: "a".into(),
            status: None,
            prio: None,
            done: None,
        })
        .unwrap();
    let b = db
        .insert_get_id(&Task {
            id: 0,
            title: "b".into(),
            status: Some("done".into()),
            prio: Some(9),
            done: Some(true),
        })
        .unwrap();
    let ta = db.find_by_id::<Task, i64>(a).unwrap().unwrap();
    assert_eq!(
        (ta.status.as_deref(), ta.prio, ta.done),
        (Some("open"), Some(5), Some(false))
    );
    let tb = db.find_by_id::<Task, i64>(b).unwrap().unwrap();
    assert_eq!(
        (tb.status.as_deref(), tb.prio, tb.done),
        (Some("done"), Some(9), Some(true))
    );

    // cascade: gp -> pa -> ch
    let g = db
        .insert_get_id(&Gp {
            id: 0,
            name: "g".into(),
        })
        .unwrap();
    let p = db.insert_get_id(&Pa { id: 0, gp_id: g }).unwrap();
    db.insert(&Ch { id: 0, pa_id: p }).unwrap();
    db.insert(&Ch { id: 0, pa_id: p }).unwrap();
    assert_eq!(db.delete::<Gp>().filter(Gp::id.eq(g)).run().unwrap(), 1);
    assert_eq!(db.count::<Pa>().unwrap(), 0);
    assert_eq!(db.count::<Ch>().unwrap(), 0);

    // restrict still blocks
    let g2 = db
        .insert_get_id(&Gp {
            id: 0,
            name: "g2".into(),
        })
        .unwrap();
    db.insert(&Rs { id: 0, gp_id: g2 }).unwrap();
    assert!(db.delete::<Gp>().filter(Gp::id.eq(g2)).run().is_err());
    assert_eq!(db.count::<Gp>().unwrap(), 1);
}
