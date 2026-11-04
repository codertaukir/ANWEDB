use mitrdb_server::{val, Server};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_imd_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 256).unwrap().0
}

fn rows_in(s: &Server, sql: &str) -> usize {
    format!("{:?}", s.execute_sql(sql).unwrap()).matches("Int64(").count()
}

fn check(s: &Server, total: usize, g7: usize, g8: usize) {
    let t = s.table("t");
    assert_eq!(t.find().count().unwrap(), total);
    assert_eq!(t.find().filter("g", "=", val(7)).count().unwrap(), g7);
    assert_eq!(t.find().filter("g", "=", val(8)).count().unwrap(), g8);
    assert_eq!(t.find().filter("g", ">", val(0)).count().unwrap(), total);
    assert_eq!(t.find().filter("g", "<", val(100)).count().unwrap(), total);
    assert_eq!(rows_in(s, "SELECT id FROM t ORDER BY g"), total);
    assert_eq!(rows_in(s, "SELECT id FROM t WHERE g > 0 ORDER BY g"), total);
}

#[test]
fn thousands_of_rows_per_index_value() {
    let d = dir("big");
    const N: i64 = 2000;
    {
        let s = open(&d);
        let t = s.table("t");
        t.sync("id int primary key, g int index, u int unique").unwrap();
        s.execute_sql("BEGIN").unwrap();
        for i in 0..N {
            t.insert(&[("id", val(i)), ("g", val(7))]).unwrap(); // u stays NULL (shared unique entry)
        }
        s.execute_sql("COMMIT").unwrap();
        check(&s, N as usize, N as usize, 0);

        assert_eq!(t.delete().filter("id", "<", val(1000)).run().unwrap(), 1000);
        check(&s, 1000, 1000, 0);

        assert_eq!(t.update().set("g", val(8)).filter("id", "<", val(1500)).run().unwrap(), 500);
        check(&s, 1000, 500, 500);
    }
    let s = open(&d);
    check(&s, 1000, 500, 500);
    let t = s.table("t");
    assert_eq!(t.delete().filter("g", "=", val(8)).run().unwrap(), 500);
    check(&s, 500, 500, 0);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn create_index_over_large_duplicate_group() {
    let d = dir("ci");
    let s = open(&d);
    let t = s.table("t");
    t.sync("id int primary key, g int").unwrap();
    s.execute_sql("BEGIN").unwrap();
    for i in 0..1500i64 {
        t.insert(&[("id", val(i)), ("g", val(7))]).unwrap();
    }
    s.execute_sql("COMMIT").unwrap();
    let ran = t.sync("id int primary key, g int index").unwrap();
    assert!(ran.iter().any(|q| q.contains("CREATE INDEX")));
    assert_eq!(t.find().filter("g", "=", val(7)).count().unwrap(), 1500);
    t.insert(&[("id", val(5000)), ("g", val(7))]).unwrap();
    assert_eq!(t.find().filter("g", "=", val(7)).count().unwrap(), 1501);
    let _ = std::fs::remove_dir_all(&d);
}
