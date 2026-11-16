use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(unique_together = "user_id,post_id", index = "post_id,user_id")]
struct Like {
    #[col(primary_key, auto)]
    id: i64,
    user_id: i64,
    post_id: i64,
}

#[test]
fn unique_together() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_comp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Like>().unwrap();
    db.sync::<Like>().unwrap(); // idempotent
    db.insert(&Like {
        id: 0,
        user_id: 1,
        post_id: 1,
    })
    .unwrap();
    db.insert(&Like {
        id: 0,
        user_id: 1,
        post_id: 2,
    })
    .unwrap();
    assert!(
        db.insert(&Like {
            id: 0,
            user_id: 1,
            post_id: 1
        })
        .is_err()
    ); // double like
    assert_eq!(db.count::<Like>().unwrap(), 2);
}
