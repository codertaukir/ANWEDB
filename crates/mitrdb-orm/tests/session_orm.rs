use mitrdb_orm::*;
use std::sync::Arc;

#[derive(Model, Debug, PartialEq)]
struct Author {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
    #[col(default = "'member'")]
    tier: Option<String>,
}

#[derive(Model, Debug, PartialEq)]
struct Article {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "authors.id", on_delete = "cascade")]
    author_id: i64,
    title: String,
    #[col(check = ">= 0")]
    views: i64,
    #[col(created_at)]
    created_at: Timestamp,
}

#[test]
fn session_orm_typed_crud_relations_and_isolation() {
    let dir = std::env::temp_dir().join(format!("mitrdb_sess_orm_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 128).unwrap();
    let db = Arc::new(db);

    db.sync::<Author>().unwrap();
    db.sync::<Article>().unwrap();

    let s1 = db.create_session();
    let s2 = db.create_session();
    let orm1 = db.session_orm(s1);
    let orm2 = db.session_orm(s2);

    // Transactional typed insert on s1; s2 cannot see uncommitted state
    db.execute_in_session(s1, "BEGIN").unwrap();
    let aid = orm1
        .insert_get_id(&Author {
            id: 0,
            name: "Ada".into(),
            tier: None,
        })
        .unwrap();
    for i in 1..=4 {
        orm1.insert(&Article {
            id: 0,
            author_id: aid,
            title: format!("Post {i}"),
            views: i * 10,
            created_at: Timestamp(0),
        })
        .unwrap();
    }
    assert_eq!(orm2.count::<Author>().unwrap(), 0);
    assert_eq!(orm2.count::<Article>().unwrap(), 0);
    db.execute_in_session(s1, "COMMIT").unwrap();

    // s2 reads committed rows, default values, pagination, and relations
    let author = orm2.find_by_id::<Author, i64>(aid).unwrap().unwrap();
    assert_eq!(author.name, "Ada");
    assert_eq!(author.tier.as_deref(), Some("member"));

    let posts = orm2.has_many(Article::author_id, aid).unwrap();
    assert_eq!(posts.len(), 4);
    assert_eq!(orm2.belongs_to::<Author, i64>(posts[0].author_id).unwrap(), Some(author));

    let page1 = orm2
        .all::<Article>()
        .page_after(Article::id, 0, 2)
        .unwrap();
    assert_eq!(page1.len(), 2);
    let page2 = orm2
        .all::<Article>()
        .page_after(Article::id, page1.last().unwrap().id, 2)
        .unwrap();
    assert_eq!(page2.len(), 2);

    let grouped = orm2.load_many(Article::author_id, &[aid]).unwrap();
    assert_eq!(grouped.get(&aid).map(Vec::len), Some(4));

    // `.in_session(s2)` builder method works on default Server query builders too
    let updated = db
        .update::<Article>()
        .in_session(s2)
        .set(Article::views, 99)
        .filter(Article::id.eq(page1[0].id))
        .run()
        .unwrap();
    assert_eq!(updated, 1);

    db.close_session(s1).unwrap();
    db.close_session(s2).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
