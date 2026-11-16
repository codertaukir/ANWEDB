use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "rel_user")]
struct RUser {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[derive(Model, Debug)]
#[model(table = "rel_post")]
struct RPost {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "rel_user.id")]
    user_id: i64,
    title: String,
}

#[test]
fn relations() {
    let dir = std::env::temp_dir().join(format!("mitrdb_rel_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<RUser>().unwrap();
    db.sync::<RPost>().unwrap();
    // 450 users (> 2 chunks of 200); user id gets (id % 3) posts. ids start at 1.
    for i in 1..=450 {
        db.insert(&RUser {
            id: 0,
            name: format!("u{i}"),
        })
        .unwrap();
    }
    for uid in 1..=450i64 {
        for k in 0..(uid % 3) {
            db.insert(&RPost {
                id: 0,
                user_id: uid,
                title: format!("p{uid}_{k}"),
            })
            .unwrap();
        }
    }
    // has_many
    assert_eq!(db.has_many(RPost::user_id, 5).unwrap().len(), 2);
    assert_eq!(db.has_many(RPost::user_id, 3).unwrap().len(), 0);
    // belongs_to
    let p = db.has_many(RPost::user_id, 5).unwrap().remove(0);
    let u: RUser = db.belongs_to(p.user_id).unwrap().unwrap();
    assert_eq!(u.name, "u5");
    assert!(db.belongs_to::<RUser, _>(9999i64).unwrap().is_none());
    // load_many: all 450 ids + duplicates, crosses chunk boundary
    let mut ids: Vec<i64> = (1..=450).collect();
    ids.extend([1, 2, 450]);
    let m = db.load_many(RPost::user_id, &ids).unwrap();
    for uid in 1..=450i64 {
        assert_eq!(
            m.get(&uid).map_or(0, |v| v.len()) as i64,
            uid % 3,
            "uid {uid}"
        );
    }
    assert_eq!(m.len(), 300);
    assert!(
        db.load_many(RPost::user_id, &[] as &[i64])
            .unwrap()
            .is_empty()
    );
}

#[derive(Model, Debug)]
#[model(table = "rel_b_user")]
struct BUser {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[derive(Model, Debug)]
#[model(table = "rel_b_post")]
struct BPost {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "rel_b_user.id")]
    user_id: i64,
}

#[test]
fn load_many_chunk_boundaries() {
    let dir = std::env::temp_dir().join(format!("mitrdb_relb_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<BUser>().unwrap();
    db.sync::<BPost>().unwrap();
    for i in 1..=151 {
        db.insert(&BUser {
            id: 0,
            name: format!("u{i}"),
        })
        .unwrap();
    }
    // every user has exactly 1 post
    for uid in 1..=151i64 {
        db.insert(&BPost {
            id: 0,
            user_id: uid,
        })
        .unwrap();
    }
    for n in [1usize, 49, 50, 51, 99, 100, 101, 150, 151] {
        let ids: Vec<i64> = (1..=n as i64).collect();
        let m = db.load_many(BPost::user_id, &ids).unwrap();
        assert_eq!(m.len(), n, "n={n}");
        assert!(m.values().all(|v| v.len() == 1), "n={n}");
    }
}
