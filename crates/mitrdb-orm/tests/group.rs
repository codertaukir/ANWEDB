use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Lk {
    #[col(primary_key, auto)]
    id: i64,
    post: i64,
    user: Option<String>,
}

#[test]
fn group_count() {
    let dir = std::env::temp_dir().join(format!("mitrdb_grp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Lk>().unwrap();
    for i in 0..10 {
        db.insert(&Lk {
            id: 0,
            post: i % 3,
            user: if i % 2 == 0 { Some("u".into()) } else { None },
        })
        .unwrap();
    }
    assert_eq!(
        db.all::<Lk>().group_count(Lk::post).unwrap(),
        vec![(0, 4), (1, 3), (2, 3)]
    );
    assert_eq!(
        db.find(Lk::user.is_not_null())
            .group_count(Lk::post)
            .unwrap(),
        vec![(0, 2), (1, 1), (2, 2)]
    );
    assert_eq!(
        db.all::<Lk>()
            .group_count_top(Lk::post, Some(4), false, None)
            .unwrap(),
        vec![(0, 4)]
    );
    let top = db
        .all::<Lk>()
        .group_count_top(Lk::post, None, true, Some(1))
        .unwrap();
    assert_eq!(top, vec![(0, 4)]);
    assert_eq!(
        db.find(Lk::post.eq(99)).group_count(Lk::post).unwrap(),
        vec![]
    );
}
