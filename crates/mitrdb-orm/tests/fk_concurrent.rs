use mitrdb_orm::*;

#[derive(Model, Debug)]
struct P {
    #[col(primary_key, auto)]
    id: i64,
    name: String,
}

#[derive(Model, Debug)]
struct C {
    #[col(primary_key, auto)]
    id: i64,
    #[col(references = "ps.id")]
    pid: i64,
}

#[test]
fn fk_concurrent_no_orphans() {
    let dir = std::env::temp_dir().join(format!("mitrdb_fkc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 256).unwrap();
    db.sync::<P>().unwrap();
    db.sync::<C>().unwrap();
    let db = &db;
    let mut ids = Vec::new();
    for i in 0..200 {
        ids.push(
            db.insert_get_id(&P {
                id: 0,
                name: format!("p{i}"),
            })
            .unwrap(),
        );
    }
    std::thread::scope(|s| {
        let ids2 = ids.clone();
        s.spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            let mut ok = 0;
            for id in ids2.into_iter().rev() {
                if db.delete::<P>().filter(P::id.eq(id)).run().is_ok() {
                    ok += 1;
                }
            }
            println!("deleted_ok={ok}");
        });
        let ids3 = ids.clone();
        s.spawn(move || {
            let (mut ok, mut err) = (0, 0);
            let mut last = String::new();
            for id in ids3 {
                match db.insert(&C { id: 0, pid: id }) {
                    Ok(_) => ok += 1,
                    Err(e) => {
                        err += 1;
                        last = format!("{e:?}");
                    }
                }
            }
            println!("insert_ok={ok} insert_err={err} last_err={last}");
        });
    });
    let children = db.all::<C>().run().unwrap();
    let mut orphans = 0;
    for c in &children {
        if db.find_by_id::<P, i64>(c.pid).unwrap().is_none() {
            orphans += 1;
        }
    }
    println!("children={} orphans={}", children.len(), orphans);
    assert_eq!(orphans, 0);
}
