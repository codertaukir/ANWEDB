use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Pm {
    #[col(primary_key, auto)]
    id: i64,
    chat: i64,
}

#[test]
fn cursor_pages() {
    let dir = std::env::temp_dir().join(format!("mitrdb_page_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Pm>().unwrap();
    for i in 0..30 {
        db.insert(&Pm { id: 0, chat: i % 2 }).unwrap();
    }
    let ids = |v: Vec<Pm>| v.into_iter().map(|m| m.id).collect::<Vec<_>>();
    assert_eq!(
        ids(db.all::<Pm>().page_after(Pm::id, 25, 3).unwrap()),
        vec![26, 27, 28]
    );
    assert_eq!(
        ids(db.all::<Pm>().page_after(Pm::id, 29, 5).unwrap()),
        vec![30]
    );
    assert_eq!(
        ids(db.all::<Pm>().page_after(Pm::id, 30, 5).unwrap()),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(db.all::<Pm>().page_before(Pm::id, 5, 3).unwrap()),
        vec![4, 3, 2]
    );
    assert_eq!(
        ids(db.find(Pm::chat.eq(0)).page_before(Pm::id, 10, 2).unwrap()),
        vec![9, 7]
    );
}
