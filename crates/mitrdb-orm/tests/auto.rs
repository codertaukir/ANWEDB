use mitrdb_orm::*;

#[derive(Model, Debug, PartialEq)]
struct Note {
    #[col(primary_key, auto)]
    id: i64,
    title: String,
}

#[test]
fn auto_increment_id() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_auto_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Note>().unwrap();

    let a = db
        .insert_get_id(&Note {
            id: 0,
            title: "a".into(),
        })
        .unwrap();
    let b = db
        .insert_get_id(&Note {
            id: 0,
            title: "b".into(),
        })
        .unwrap();
    assert_eq!((a, b), (1, 2));
    assert_eq!(
        db.insert(&Note {
            id: 0,
            title: "c".into()
        })
        .unwrap(),
        1
    );
    assert_eq!(
        db.find_by_id::<Note, i64>(3).unwrap(),
        Some(Note {
            id: 3,
            title: "c".into()
        })
    );
    assert_eq!(db.count::<Note>().unwrap(), 3);
}
