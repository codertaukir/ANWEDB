use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "ts_msg")]
struct TsMsg {
    #[col(primary_key, auto)]
    id: i64,
    #[col(unique)]
    code: i64,
    body_ref: String,
}

fn count_files(d: &std::path::Path) -> usize {
    std::fs::read_dir(d).unwrap().count()
}

#[test]
fn text_store() {
    let dir = std::env::temp_dir().join(format!("mitrdb_ts_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    let fdir = dir.join("files");
    let ts = TextStore::open(&fdir).unwrap();
    db.sync::<TsMsg>().unwrap();

    // long Hindi text (~140 KB, far above the row limit) round-trips
    let long = "नमस्ते दुनिया ".repeat(10_000);
    assert!(long.len() > 100_000);
    let id = ts
        .put_and(&long, |name| {
            db.insert_get_id(&TsMsg {
                id: 0,
                code: 1,
                body_ref: name.to_string(),
            })
        })
        .unwrap();
    let m: TsMsg = db.find_by_id(id).unwrap().unwrap();
    assert_eq!(ts.get(&m.body_ref).unwrap(), long);
    assert_eq!(count_files(&fdir), 1);

    // row insert fails (unique code 1 again) -> file must be removed
    let r = ts.put_and("second", |name| {
        db.insert_get_id(&TsMsg {
            id: 0,
            code: 1,
            body_ref: name.to_string(),
        })
    });
    assert!(r.is_err());
    assert_eq!(count_files(&fdir), 1, "orphan file left behind");

    // delete, then get fails; bad names rejected
    ts.delete(&m.body_ref).unwrap();
    assert!(ts.get(&m.body_ref).is_err());
    assert!(ts.get("../etc/passwd").is_err());
    assert_eq!(count_files(&fdir), 0);
}
