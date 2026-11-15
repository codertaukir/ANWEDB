use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Item {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[test]
fn deleted_top_id_not_reused() {
    let dir = std::env::temp_dir().join(format!("mitrdb_seq_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Item>().unwrap();
    let a = db
        .insert_get_id(&Item {
            id: 0,
            name: "a".into(),
        })
        .unwrap();
    let b = db
        .insert_get_id(&Item {
            id: 0,
            name: "b".into(),
        })
        .unwrap();
    assert_eq!((a, b), (1, 2));
    db.delete::<Item>().filter(Item::id.eq(2)).run().unwrap();
    let c = db
        .insert_get_id(&Item {
            id: 0,
            name: "c".into(),
        })
        .unwrap();
    assert_eq!(c, 3);
}
