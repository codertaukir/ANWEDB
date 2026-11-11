#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_common::Error;
use mitrdb_exec::ExecutionResult;
use mitrdb_server::{IsolationLevel, Server, Value};
use std::path::PathBuf;
use std::time::Instant;

fn open(tag: &str) -> (PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_uidx_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (srv, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 4096).unwrap();
    (dir, srv)
}

fn gval(k: i64) -> String {
    if k % 10 == 0 {
        "NULL".to_string()
    } else if k % 3 == 0 {
        "5".to_string()
    } else {
        (k % 7).to_string()
    }
}

fn load(srv: &Server, name: &str, n: i64, idx_before: bool, idx_after: bool) {
    srv.execute_sql(&format!(
        "CREATE TABLE {name} (id INT PRIMARY KEY, g INT, name TEXT)"
    ))
    .unwrap();
    if idx_before {
        srv.execute_sql(&format!("CREATE INDEX idx_{name} ON {name}(g)"))
            .unwrap();
    }
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end)
            .map(|k| format!("({k},{},'n{k}')", gval(k)))
            .collect();
        srv.execute_sql(&format!("INSERT INTO {name} VALUES {}", vals.join(",")))
            .unwrap();
        i = end;
    }
    if idx_after {
        srv.execute_sql(&format!("CREATE INDEX idx_{name} ON {name}(g)"))
            .unwrap();
    }
}

fn dump(srv: &Server, name: &str) -> Vec<Vec<Value>> {
    srv.table(name).find().order_by("id").run().unwrap().rows
}

fn run_all(srv: &Server, tpl: &str, tabs: &[&str]) -> Vec<ExecutionResult> {
    tabs.iter()
        .map(|t| srv.execute_sql(&tpl.replace("{t}", t)).unwrap())
        .collect()
}

#[test]
fn idx_dml_matches_unindexed_table() {
    let (dir, srv) = open("eq");
    let n = 2400;
    load(&srv, "tb", n, false, false);
    load(&srv, "ta", n, true, false);
    load(&srv, "tc", n, false, true);
    let tabs = ["tb", "ta", "tc"];
    let stmts = [
        "UPDATE {t} SET name = 'u1' WHERE g = 5",
        "UPDATE {t} SET name = 'u2' WHERE g = 3 OR g = 4",
        "UPDATE {t} SET name = 'u3' WHERE g IS NULL",
        "UPDATE {t} SET name = 'u4' WHERE g = NULL",
        "UPDATE {t} SET name = 'u5' WHERE g = 6 AND id > 700",
        "UPDATE {t} SET name = 'u6' WHERE g > 4",
        "UPDATE {t} SET g = 9 WHERE g = 1",
        "UPDATE {t} SET name = 'u7' WHERE g = 9",
        "UPDATE {t} SET name = 'u8' WHERE g = 12345",
        "DELETE FROM {t} WHERE g = 2",
        "DELETE FROM {t} WHERE g IS NULL",
        "DELETE FROM {t} WHERE g = 5 AND id < 300",
        "DELETE FROM {t} WHERE g = 9 OR g = 0",
        "DELETE FROM {t} WHERE g = 5",
    ];
    for st in stmts {
        let r = run_all(&srv, st, &tabs);
        assert_eq!(r[0], r[1], "affected tb vs ta for: {st}");
        assert_eq!(r[0], r[2], "affected tb vs tc for: {st}");
        let base = dump(&srv, "tb");
        assert_eq!(base, dump(&srv, "ta"), "rows tb vs ta after: {st}");
        assert_eq!(base, dump(&srv, "tc"), "rows tb vs tc after: {st}");
        for j in 0..=9 {
            let c = run_all(
                &srv,
                &format!("SELECT COUNT(*) FROM {{t}} WHERE g = {j}"),
                &tabs,
            );
            assert_eq!(c[0], c[1], "count g={j} tb vs ta after: {st}");
            assert_eq!(c[0], c[2], "count g={j} tb vs tc after: {st}");
        }
        let c = run_all(&srv, "SELECT COUNT(*) FROM {t} WHERE g IS NULL", &tabs);
        assert_eq!(c[0], c[1], "count null tb vs ta after: {st}");
        assert_eq!(c[0], c[2], "count null tb vs tc after: {st}");
    }
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn idx_dml_is_not_full_scan() {
    let (dir, srv) = open("ratio");
    let n = 8000i64;
    srv.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, h INT, name TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_g ON t(g)").unwrap();
    let mut i = 0i64;
    while i < n {
        let end = (i + 500).min(n);
        let vals: Vec<String> = (i..end)
            .map(|k| format!("({k},{},{},'n{k}')", k % 1000, k % 1000))
            .collect();
        srv.execute_sql(&format!("INSERT INTO t VALUES {}", vals.join(",")))
            .unwrap();
        i = end;
    }
    let ops = 20;
    let exp = format!("Affected({})", n / 1000);
    let t0 = Instant::now();
    for j in 0..ops {
        let r = srv
            .execute_sql(&format!("UPDATE t SET name = 'a' WHERE g = {j}"))
            .unwrap();
        assert_eq!(format!("{r:?}"), exp, "g j={j}");
    }
    let g_total = t0.elapsed();
    let t1 = Instant::now();
    for j in 0..ops {
        let r = srv
            .execute_sql(&format!("UPDATE t SET name = 'b' WHERE h = {j}"))
            .unwrap();
        assert_eq!(format!("{r:?}"), exp, "h j={j}");
    }
    let h_total = t1.elapsed();
    println!("TIMING indexed g_total={g_total:?} unindexed h_total={h_total:?}");
    assert!(
        g_total * 3 < h_total,
        "indexed UPDATE not faster than full scan: g={g_total:?} h={h_total:?}"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn idx_dml_serializable_phantom() {
    let (dir, srv) = open("phantom");
    srv.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, g INT, name TEXT)")
        .unwrap();
    srv.execute_sql("CREATE INDEX idx_g ON users(g)").unwrap();
    srv.execute_sql("INSERT INTO users VALUES (1, 20, 'a')")
        .unwrap();
    srv.execute_sql("INSERT INTO users VALUES (2, 30, 'b')")
        .unwrap();

    for (case, stmt, gv) in [
        ("update", "UPDATE users SET name = 'x' WHERE g = 25", 25),
        ("delete", "DELETE FROM users WHERE g = 35", 35),
    ] {
        let s1 = srv.create_session_with_isolation(IsolationLevel::Serializable);
        let s2 = srv.create_session_with_isolation(IsolationLevel::Serializable);
        srv.execute_in_session(s1, "BEGIN").unwrap();
        let r = srv.execute_in_session(s1, stmt).unwrap();
        assert_eq!(r, ExecutionResult::Affected(0), "{case}");
        let pid = if case == "update" { 100 } else { 200 };
        assert!(
            matches!(
                srv.execute_in_session(
                    s2,
                    &format!("INSERT INTO users VALUES ({pid}, {gv}, 'ph')")
                ),
                Err(Error::LockTimeout)
            ),
            "{case}: phantom insert g=25 not blocked"
        );
        srv.execute_in_session(
            s2,
            &format!("INSERT INTO users VALUES ({}, {}, 'ok')", pid + 1, gv + 1),
        )
        .unwrap();
        srv.execute_in_session(s1, "COMMIT").unwrap();
        srv.execute_in_session(
            s2,
            &format!("INSERT INTO users VALUES ({pid}, {gv}, 'now')"),
        )
        .unwrap();
    }
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}
