use mitrdb_orm::*;

#[derive(Model, Debug)]
struct User {
    #[col(primary_key, auto)]
    id: i64,
    #[col(unique)]
    username: String,
    #[col(created_at)]
    created_at: Timestamp,
    #[col(updated_at)]
    updated_at: Timestamp,
}

#[derive(Model, Debug)]
#[model(unique_together = "follower_id,followee_id")]
struct Follow {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "users.id", on_delete = "cascade")]
    follower_id: i64,
    #[col(references = "users.id", on_delete = "cascade")]
    followee_id: i64,
}

#[derive(Model, Debug)]
#[model(index = "chat_id,id")]
struct Message {
    #[col(primary_key, auto)]
    id: i64,
    #[col(index)]
    chat_id: i64,
    #[col(references = "users.id", on_delete = "cascade")]
    sender_id: i64,
    body: String,
    #[col(default = "false")]
    is_read: Option<bool>,
    #[col(created_at)]
    created_at: Timestamp,
    #[col(updated_at)]
    updated_at: Timestamp,
}

#[test]
fn chat_app_flow() {
    let dir = std::env::temp_dir().join(format!("mitrdb_chat_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<User>().unwrap();
    db.sync::<Follow>().unwrap();
    db.sync::<Message>().unwrap();

    let mk = |n: &str| {
        db.insert_get_id(&User {
            id: 0,
            username: n.into(),
            created_at: Timestamp(0),
            updated_at: Timestamp(0),
        })
        .unwrap()
    };
    let (a, b, c) = (mk("amit"), mk("bina"), mk("chetan"));
    assert!(
        db.insert(&User {
            id: 0,
            username: "amit".into(),
            created_at: Timestamp(0),
            updated_at: Timestamp(0)
        })
        .is_err()
    );

    db.insert(&Follow {
        id: 0,
        follower_id: a,
        followee_id: b,
    })
    .unwrap();
    db.insert(&Follow {
        id: 0,
        follower_id: a,
        followee_id: c,
    })
    .unwrap();
    assert!(
        db.insert(&Follow {
            id: 0,
            follower_id: a,
            followee_id: b
        })
        .is_err()
    );

    for i in 0..30 {
        db.insert(&Message {
            id: 0,
            chat_id: 1,
            sender_id: if i % 2 == 0 { a } else { b },
            body: format!("m{i}"),
            is_read: None,
            created_at: Timestamp(0),
            updated_at: Timestamp(0),
        })
        .unwrap();
    }
    // latest 10 messages of chat 1
    let last = db
        .find(Message::chat_id.eq(1))
        .order_by_desc(Message::id)
        .limit(10)
        .run()
        .unwrap();
    assert_eq!(last.len(), 10);
    assert_eq!(last[0].body, "m29");
    assert_eq!(last[0].is_read, Some(false));
    // polling: messages newer than id 25
    assert_eq!(db.find(Message::id.gt(25)).run().unwrap().len(), 5);
    // unread count, search
    assert_eq!(db.count_where(Message::is_read.eq(false)).unwrap(), 30);
    assert_eq!(db.find(Message::body.like("m2%")).run().unwrap().len(), 11);
    // mark read
    db.update::<Message>()
        .set(Message::is_read, true)
        .filter(Message::chat_id.eq(1))
        .filter(Message::sender_id.eq(b))
        .run()
        .unwrap();
    assert_eq!(db.count_where(Message::is_read.eq(false)).unwrap(), 15);
    // feed: messages from people a follows
    let followees: Vec<i64> = db
        .find(Follow::follower_id.eq(a))
        .run()
        .unwrap()
        .iter()
        .map(|f| f.followee_id)
        .collect();
    assert_eq!(
        db.all::<Message>()
            .filter_any(Message::sender_id.in_list(followees))
            .run()
            .unwrap()
            .len(),
        15
    );
    // delete user b: follows and messages cascade
    db.delete::<User>().filter(User::id.eq(b)).run().unwrap();
    assert_eq!(db.count::<Message>().unwrap(), 15);
    assert_eq!(db.count::<Follow>().unwrap(), 1);
}
