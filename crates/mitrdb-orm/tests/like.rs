use mitrdb_orm::*;

fn t(s: &str) -> Value {
    Value::Text(s.into())
}

#[test]
fn like_filter() {
    let dir = std::env::temp_dir().join(format!("mitrdb_like_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    let tb = db.table("words");
    tb.sync("id int primary key, name text not null index, note text")
        .unwrap();
    for (i, (n, o)) in [
        ("abc", Some("xx")),
        ("abd", None),
        ("xbc", Some("y")),
        ("a", Some("axb")),
    ]
    .iter()
    .enumerate()
    {
        let note = o.map(t).unwrap_or(Value::Null);
        tb.insert(&[
            ("id", Value::Int64(i as i64)),
            ("name", t(n)),
            ("note", note),
        ])
        .unwrap();
    }
    let cnt = |col: &str, pat: &str| {
        tb.find()
            .filter(col, "like", t(pat))
            .run()
            .unwrap()
            .rows
            .len()
    };
    assert_eq!(cnt("name", "ab%"), 2);
    assert_eq!(cnt("name", "%bc"), 2);
    assert_eq!(cnt("name", "a_c"), 1);
    assert_eq!(cnt("name", "a%"), 3);
    assert_eq!(cnt("name", "%"), 4);
    assert_eq!(cnt("note", "%x%"), 2);
    assert_eq!(cnt("name", "ABC"), 0);
}
