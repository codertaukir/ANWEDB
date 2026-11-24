use mitrdb_server::Server;
use std::time::Instant;

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_csf_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 4096)
        .unwrap()
        .0
}
fn cnt(s: &Server, q: &str) -> String {
    format!("{:?}", s.execute_sql(q).unwrap())
}

#[test]
fn count_star_matches_normal_path() {
    let s = open("a");
    s.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, t TEXT)")
        .unwrap();
    let v: Vec<String> = (0..500).map(|i| format!("({i}, 'x{i}')")).collect();
    s.execute_sql(&format!("INSERT INTO c VALUES {}", v.join(",")))
        .unwrap();
    assert!(cnt(&s, "SELECT COUNT(*) FROM c").contains("Int64(500)"));
    s.execute_sql("DELETE FROM c WHERE id < 10").unwrap();
    assert!(cnt(&s, "SELECT COUNT(*) FROM c").contains("Int64(490)"));
    assert!(cnt(&s, "SELECT COUNT(id) FROM c").contains("Int64(490)"));
    s.execute_sql("BEGIN").unwrap();
    s.execute_sql("INSERT INTO c VALUES (9999, 'own')").unwrap();
    assert!(cnt(&s, "SELECT COUNT(*) FROM c").contains("Int64(491)"));
    s.execute_sql("ROLLBACK").unwrap();
    assert!(cnt(&s, "SELECT COUNT(*) FROM c").contains("Int64(490)"));
    assert!(cnt(&s, "SELECT COUNT(*) FROM c WHERE id >= 100").contains("Int64(400)"));
    s.execute_sql("CREATE TABLE e (id INT PRIMARY KEY)")
        .unwrap();
    assert!(cnt(&s, "SELECT COUNT(*) FROM e").contains("Int64(0)"));
}

#[test]
#[ignore]
fn count_star_timing_probe() {
    let s = open("b");
    s.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, n INT, t TEXT)")
        .unwrap();
    let mut id = 0;
    while id < 200_000 {
        let v: Vec<String> = (0..1000)
            .map(|k| format!("({}, 1, 'row-{}-xxxxxxxxxxxxxxxx')", id + k, id + k))
            .collect();
        s.execute_sql(&format!("INSERT INTO c VALUES {}", v.join(",")))
            .unwrap();
        id += 1000;
    }
    for _ in 0..3 {
        let t = Instant::now();
        let a = cnt(&s, "SELECT COUNT(*) FROM c");
        let fast = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let b = cnt(&s, "SELECT COUNT(id) FROM c");
        let slow = t.elapsed().as_secs_f64();
        println!(
            "CSF rows=200000 count_star_secs={fast:.3} count_col_secs={slow:.3} a_ok={} b_ok={}",
            a.contains("Int64(200000)"),
            b.contains("Int64(200000)")
        );
    }
}
