use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Author {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[derive(Model, Debug)]
struct Book {
    #[col(primary_key, auto)]
    id: i64,
    title: String,
    #[col(references = "authors.id")]
    author_id: i64,
    #[col(references = "authors.id")]
    editor_id: Option<i64>,
}

fn book(a: i64, e: Option<i64>) -> Book {
    Book {
        id: 0,
        title: "t".into(),
        author_id: a,
        editor_id: e,
    }
}

#[test]
fn foreign_keys() {
    let dir = std::env::temp_dir().join(format!("mitrdb_fk_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Author>().unwrap();
    db.sync::<Book>().unwrap();
    db.sync::<Book>().unwrap(); // idempotent

    let a = db
        .insert_get_id(&Author {
            id: 0,
            name: "x".into(),
        })
        .unwrap();
    let b = db.insert_get_id(&book(a, None)).unwrap(); // NULL editor ok
    assert!(db.insert(&book(999, None)).is_err()); // no parent
    assert!(db.insert(&book(a, Some(999))).is_err()); // nullable fk checked too

    assert!(
        db.delete::<Author>()
            .filter(Author::id.eq(a))
            .run()
            .is_err()
    ); // in use
    assert!(
        db.update::<Book>()
            .set(Book::author_id, 999i64)
            .filter(Book::id.eq(b))
            .run()
            .is_err()
    );

    db.delete::<Book>().filter(Book::id.eq(b)).run().unwrap();
    assert_eq!(
        db.delete::<Author>()
            .filter(Author::id.eq(a))
            .run()
            .unwrap(),
        1
    );
}
