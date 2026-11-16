use mitrdb_server::{Server, val};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_vl_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

#[test]
fn value_limit_probe() {
    let s = open("one");
    let t = s.table("one");
    t.sync("id int primary key, data text").unwrap();
    for (i, n) in [3000usize, 3500, 3800, 3900, 3950, 4000, 4050, 4096, 5000]
        .iter()
        .enumerate()
    {
        let r = t.insert(&[("id", val(i as i64 + 1)), ("data", val("x".repeat(*n)))]);
        eprintln!("VL one_col n={n} ok={}", r.is_ok());
    }
    let s = open("three");
    let t = s.table("three");
    t.sync("id int primary key, a text, b text, c text")
        .unwrap();
    for (i, n) in [500usize, 1000, 1300, 1500, 2000].iter().enumerate() {
        let r = t.insert(&[
            ("id", val(i as i64 + 1)),
            ("a", val("x".repeat(*n))),
            ("b", val("y".repeat(*n))),
            ("c", val("z".repeat(*n))),
        ]);
        eprintln!("VL three_cols each={n} total={} ok={}", n * 3, r.is_ok());
    }
}
