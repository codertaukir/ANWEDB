#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server};
use std::time::Instant;

fn open(tag: &str) -> Server {
    let d = std::env::temp_dir().join(format!("mitrdb_jh_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 256)
        .unwrap()
        .0;
    s.execute_sql("CREATE TABLE ja (id INT PRIMARY KEY, k INT)")
        .unwrap();
    s.execute_sql("CREATE TABLE jb (id INT PRIMARY KEY, k INT)")
        .unwrap();
    s
}

fn fill(s: &Server, table: &str, data: &[(i64, Option<i64>)]) {
    for chunk in data.chunks(200) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|(id, k)| match k {
                Some(k) => format!("({id},{k})"),
                None => format!("({id},NULL)"),
            })
            .collect();
        s.execute_sql(&format!("INSERT INTO {table} VALUES {}", vals.join(",")))
            .unwrap();
    }
}

fn rows(s: &Server, q: &str) -> usize {
    match s.execute_sql(q).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn hash_join_matches_naive_reference() {
    let s = open("ref");
    let a: Vec<(i64, Option<i64>)> = (0..300)
        .map(|i| (i, if i % 10 == 0 { None } else { Some(i % 7) }))
        .collect();
    let b: Vec<(i64, Option<i64>)> = (0..200)
        .map(|j| (j, if j % 9 == 0 { None } else { Some(j % 5) }))
        .collect();
    fill(&s, "ja", &a);
    fill(&s, "jb", &b);

    let mut inner = 0usize;
    let mut unmatched = 0usize;
    let mut lt = 0usize;
    for (_, ka) in &a {
        let m = b.iter().filter(|(_, kb)| ka.is_some() && ka == kb).count();
        inner += m;
        if m == 0 {
            unmatched += 1;
        }
        lt += b
            .iter()
            .filter(|(_, kb)| matches!((ka, kb), (Some(x), Some(y)) if x < y))
            .count();
    }
    assert!(inner > 0 && unmatched > 0);
    assert_eq!(rows(&s, "SELECT * FROM ja JOIN jb ON ja.k = jb.k"), inner);
    assert_eq!(
        rows(&s, "SELECT * FROM ja INNER JOIN jb ON ja.k = jb.k"),
        inner
    );
    assert_eq!(
        rows(&s, "SELECT * FROM ja LEFT JOIN jb ON ja.k = jb.k"),
        inner + unmatched
    );
    // non-Eq join: purana nested loop path
    assert_eq!(rows(&s, "SELECT * FROM ja JOIN jb ON ja.k < jb.k"), lt);
}

#[test]
fn join_order_by_column_must_be_selected() {
    let s = open("ob");
    fill(&s, "ja", &[(1, Some(1)), (2, Some(2))]);
    fill(&s, "jb", &[(1, Some(1)), (2, Some(2))]);
    assert!(
        s.execute_sql("SELECT ja.id FROM ja JOIN jb ON ja.k = jb.k ORDER BY jb.id")
            .is_err()
    );
    assert!(
        s.execute_sql("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.k = jb.k ORDER BY jb.id")
            .is_ok()
    );
}

#[test]
#[ignore]
fn probe_join_timing() {
    for n in [1000usize, 10000] {
        let s = open(&format!("p{n}"));
        let data: Vec<(i64, Option<i64>)> = (0..n as i64).map(|i| (i, Some(i))).collect();
        fill(&s, "ja", &data);
        fill(&s, "jb", &data);
        let t = Instant::now();
        let r = rows(&s, "SELECT * FROM ja JOIN jb ON ja.k = jb.k");
        println!("PROBE n={n} inner rows={r} took={:?}", t.elapsed());
        let t = Instant::now();
        let r = rows(&s, "SELECT * FROM ja LEFT JOIN jb ON ja.k = jb.k");
        println!("PROBE n={n} left rows={r} took={:?}", t.elapsed());
    }
}

#[test]
fn index_assisted_join_pk_and_secondary_index_with_pushdown() {
    let d = std::env::temp_dir().join(format!("mitrdb_idx_join_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 256)
        .unwrap()
        .0;

    s.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, org INT, active INT, name TEXT)")
        .unwrap();
    s.execute_sql("CREATE INDEX idx_users_org ON users (org)").unwrap();
    s.execute_sql("CREATE TABLE msgs (id INT PRIMARY KEY, room INT, user_id INT, org INT, body TEXT)")
        .unwrap();

    // Populate 400 users so outer query with <= 256 distinct keys exercises index-lookup join
    for chunk in (1..=400i64).collect::<Vec<_>>().chunks(100) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|id| format!("({id}, {}, {}, 'u{id}')", id % 50, if id % 2 == 0 { 1 } else { 0 }))
            .collect();
        s.execute_sql(&format!("INSERT INTO users VALUES {}", vals.join(",")))
            .unwrap();
    }

    s.execute_sql(
        "INSERT INTO msgs VALUES          (1, 10, 2, 2, 'm1'),          (2, 10, 3, 3, 'm2'),          (3, 10, 999, 99, 'orphan'),          (4, 10, NULL, NULL, 'null_fk'),          (5, 20, 4, 4, 'other_room')",
    )
    .unwrap();

    // 1. PK index-assisted INNER JOIN + pushed-down left & right filters
    assert_eq!(
        rows(
            &s,
            "SELECT msgs.id, users.name FROM msgs JOIN users ON msgs.user_id = users.id WHERE msgs.room = 10 AND users.active = 1"
        ),
        1
    );

    // 2. PK index-assisted LEFT JOIN keeps orphan & NULL foreign key rows
    assert_eq!(
        rows(
            &s,
            "SELECT msgs.id, users.name FROM msgs LEFT JOIN users ON msgs.user_id = users.id WHERE msgs.room = 10"
        ),
        4
    );

    // 3. Secondary index-assisted INNER JOIN (multiple users per org) + pushed-down right filter
    assert_eq!(
        rows(
            &s,
            "SELECT msgs.id, users.name FROM msgs JOIN users ON msgs.org = users.org WHERE msgs.room = 10 AND users.active = 1"
        ),
        8
    );

    // 4. Empty outer result short-circuits cleanly
    assert_eq!(
        rows(
            &s,
            "SELECT msgs.id, users.name FROM msgs JOIN users ON msgs.user_id = users.id WHERE msgs.room = 9999"
        ),
        0
    );

    let _ = std::fs::remove_dir_all(&d);
}
