use mitrdb_server::{ExecutionResult, Server};
use std::time::Instant;

fn open(tag: &str) -> Server {
    let d = std::env::temp_dir().join(format!("mitrdb_jh_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 256).unwrap().0;
    s.execute_sql("CREATE TABLE ja (id INT PRIMARY KEY, k INT)").unwrap();
    s.execute_sql("CREATE TABLE jb (id INT PRIMARY KEY, k INT)").unwrap();
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
        s.execute_sql(&format!("INSERT INTO {table} VALUES {}", vals.join(","))).unwrap();
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
    let a: Vec<(i64, Option<i64>)> =
        (0..300).map(|i| (i, if i % 10 == 0 { None } else { Some(i % 7) })).collect();
    let b: Vec<(i64, Option<i64>)> =
        (0..200).map(|j| (j, if j % 9 == 0 { None } else { Some(j % 5) })).collect();
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
        lt += b.iter().filter(|(_, kb)| matches!((ka, kb), (Some(x), Some(y)) if x < y)).count();
    }
    assert!(inner > 0 && unmatched > 0);
    assert_eq!(rows(&s, "SELECT * FROM ja JOIN jb ON ja.k = jb.k"), inner);
    assert_eq!(rows(&s, "SELECT * FROM ja INNER JOIN jb ON ja.k = jb.k"), inner);
    assert_eq!(rows(&s, "SELECT * FROM ja LEFT JOIN jb ON ja.k = jb.k"), inner + unmatched);
    // non-Eq join: purana nested loop path
    assert_eq!(rows(&s, "SELECT * FROM ja JOIN jb ON ja.k < jb.k"), lt);
}

#[test]
fn join_order_by_column_must_be_selected() {
    let s = open("ob");
    fill(&s, "ja", &[(1, Some(1)), (2, Some(2))]);
    fill(&s, "jb", &[(1, Some(1)), (2, Some(2))]);
    assert!(s
        .execute_sql("SELECT ja.id FROM ja JOIN jb ON ja.k = jb.k ORDER BY jb.id")
        .is_err());
    assert!(s
        .execute_sql("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.k = jb.k ORDER BY jb.id")
        .is_ok());
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
