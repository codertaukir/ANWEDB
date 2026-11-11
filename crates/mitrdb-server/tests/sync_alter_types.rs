#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn dump(s: &Server, sql: &str) -> String {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn type_change_is_opt_in_and_guarded() {
    let d = fresh("synctype");
    {
        let s = open(&d);
        let t = s.table("t");
        t.sync("id int primary key, b int").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 1)").unwrap();

        // default sync refuses and names the opt-in method
        let e = t.sync("id int primary key, b text").unwrap_err();
        assert!(format!("{e:?}").contains("sync_allow_type_change"), "{e:?}");
        assert!(dump(&s, "SELECT b FROM t").contains("Int64(1)"));

        // opt-in applies it, data converted
        let plan = t
            .sync_allow_type_change("id int primary key, b text")
            .unwrap();
        assert!(plan.iter().any(|p| p.contains("TYPE text")), "{plan:?}");
        assert!(dump(&s, "SELECT b FROM t").contains("Text(\"1\")"));
        assert!(
            t.sync_allow_type_change("id int primary key, b text")
                .unwrap()
                .is_empty()
        );

        // PK type change refused, nothing half-applied
        assert!(
            t.sync_allow_type_change("id text primary key, b text")
                .is_err()
        );

        // indexed column refused up front (no partial plan)
        let u = s.table("u");
        u.sync("id int primary key, c text index").unwrap();
        s.execute_sql("INSERT INTO u VALUES (1, '7')").unwrap();
        assert!(
            u.sync_allow_type_change("id int primary key, c int index, d int")
                .is_err()
        );
        assert!(dump(&s, "SELECT * FROM u").contains("Text(\"7\")"));
        assert!(
            s.execute_sql("SELECT d FROM u").is_err(),
            "d must not have been added"
        );
    }
    let s = open(&d);
    assert!(dump(&s, "SELECT b FROM t").contains("Text(\"1\")"));
    let _ = std::fs::remove_dir_all(&d);
}
