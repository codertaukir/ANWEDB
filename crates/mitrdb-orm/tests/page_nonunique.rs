use mitrdb_orm::*;

#[derive(Model, Debug)]
struct Pn {
    #[col(primary_key, auto)]
    id: i64,
    ts: i64,
}

#[test]
fn page_nonunique_pinned() {
    let dir = std::env::temp_dir().join(format!("mitrdb_pn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    db.sync::<Pn>().unwrap();
    // 10 distinct ts values (1..=10), 5 rows each = 50 rows
    for i in 0..50 {
        db.insert(&Pn {
            id: 0,
            ts: i / 5 + 1,
        })
        .unwrap();
    }

    // Forward: naive cursor loop on non-unique ts, page size 7, cursor = last row's ts
    let mut seen: Vec<i64> = vec![];
    let mut last = 0i64;
    let mut pages = 0;
    loop {
        let page = db.all::<Pn>().page_after(Pn::ts, last, 7).unwrap();
        if page.is_empty() {
            break;
        }
        pages += 1;
        eprintln!(
            "PN fwd page{pages} ids={:?} ts={:?}",
            page.iter().map(|m| m.id).collect::<Vec<_>>(),
            page.iter().map(|m| m.ts).collect::<Vec<_>>()
        );
        last = page.last().unwrap().ts;
        seen.extend(page.iter().map(|m| m.id));
    }
    eprintln!("PN fwd total_seen={} of 50 pages={pages}", seen.len());

    // Backward: same idea with page_before
    let mut seen_b: Vec<i64> = vec![];
    let mut first = 11i64;
    let mut pages_b = 0;
    loop {
        let page = db.all::<Pn>().page_before(Pn::ts, first, 7).unwrap();
        if page.is_empty() {
            break;
        }
        pages_b += 1;
        first = page.last().unwrap().ts;
        seen_b.extend(page.iter().map(|m| m.id));
    }
    eprintln!("PN back total_seen={} of 50 pages={pages_b}", seen_b.len());

    // Safe pattern: PK cursor with ts filter still complete
    let mut seen_pk: Vec<i64> = vec![];
    let mut lastid = 0i64;
    loop {
        let page = db.find(Pn::ts.ge(1)).page_after(Pn::id, lastid, 7).unwrap();
        if page.is_empty() {
            break;
        }
        lastid = page.last().unwrap().id;
        seen_pk.extend(page.iter().map(|m| m.id));
    }
    eprintln!("PN pk total_seen={} of 50", seen_pk.len());
}
