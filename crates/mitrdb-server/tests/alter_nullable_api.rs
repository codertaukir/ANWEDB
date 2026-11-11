#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::Server;
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

#[test]
fn table_set_nullable_only_touches_named_column_and_persists() {
    let d = fresh("altnull_api");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text not null, c text not null)")
            .unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 'y')").unwrap();

        s.table("t").set_nullable("a").unwrap();
        assert!(s.table("t").set_nullable("id").is_err());
        assert!(s.table("t").set_nullable("bad name").is_err());

        s.execute_sql("INSERT INTO t (id, c) VALUES (2, 'k')")
            .unwrap();
        assert!(
            s.execute_sql("INSERT INTO t (id, a) VALUES (3, 'q')")
                .is_err(),
            "c must still be NOT NULL"
        );
    }
    let s = open(&d);
    s.execute_sql("INSERT INTO t (id, c) VALUES (4, 'm')")
        .unwrap();
    assert!(
        s.execute_sql("INSERT INTO t (id, a) VALUES (5, 'q')")
            .is_err()
    );
    let _ = std::fs::remove_dir_all(&d);
}
