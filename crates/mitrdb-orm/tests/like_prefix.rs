use mitrdb_orm::*;

fn t(s: &str) -> Value {
    Value::Text(s.into())
}

#[test]
fn like_prefix_index() {
    let dir = std::env::temp_dir().join(format!("mitrdb_likepfx_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    let tb = db.table("words");
    tb.sync("id int primary key, name text not null index")
        .unwrap();
    for (i, n) in [
        "abc", "abd", "ab", "a", "xbc", "ac", "abcd", "b", "अब", "अक",
    ]
    .iter()
    .enumerate()
    {
        tb.insert(&[("id", Value::Int64(i as i64)), ("name", t(n))])
            .unwrap();
    }
    let cnt = |pat: &str| {
        tb.find()
            .filter("name", "like", t(pat))
            .run()
            .unwrap()
            .rows
            .len()
    };
    for (pat, want) in [
        ("ab%", 4),
        ("abc%", 2),
        ("abc", 1),
        ("a%c", 2),
        ("ab_", 2),
        ("a%", 6),
        ("अ%", 2),
        ("अब%", 1),
        ("_bc", 2),
        ("%", 10),
        ("a_", 2),
        ("b%", 1),
        ("abcd%", 1),
        ("abz%", 0),
    ] {
        eprintln!("LP {pat} got={} want={want}", cnt(pat));
        assert_eq!(cnt(pat), want, "pattern {pat}");
    }
}
