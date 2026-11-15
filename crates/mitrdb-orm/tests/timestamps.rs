use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Post {
    #[col(primary_key, auto)]
    id: i64,
    title: String,
    #[col(created_at)]
    created_at: Timestamp,
    #[col(updated_at)]
    updated_at: Timestamp,
}

#[test]
fn created_updated_at() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_ts_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Post>().unwrap();

    let id = db
        .insert_get_id(&Post {
            id: 0,
            title: "a".into(),
            created_at: Timestamp(0),
            updated_at: Timestamp(0),
        })
        .unwrap();
    let p = db.find_by_id::<Post, i64>(id).unwrap().unwrap();
    assert!(p.created_at.0 > 0);
    assert_eq!(p.created_at, p.updated_at);

    std::thread::sleep(std::time::Duration::from_millis(5));
    db.update::<Post>()
        .set(Post::title, "b")
        .filter(Post::id.eq(id))
        .run()
        .unwrap();
    let q = db.find_by_id::<Post, i64>(id).unwrap().unwrap();
    assert_eq!(q.title, "b");
    assert_eq!(q.created_at, p.created_at);
    assert!(q.updated_at > p.updated_at);
}
