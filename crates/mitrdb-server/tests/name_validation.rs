#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::Server;

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_nv_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0;
    s.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    s
}

#[test]
fn bad_table_names_are_rejected() {
    let s = open("a");
    let long = "a".repeat(65);
    for name in [
        "a b", "a;b", "a'b", "a\"b", "a-b", "1abc", "naïve", "a.b", "", &long,
    ] {
        let q = format!("CREATE TABLE {name} (id INT PRIMARY KEY)");
        assert!(s.execute_sql(&q).is_err(), "should fail: {q}");
    }
}

#[test]
fn bad_column_and_index_names_are_rejected() {
    let s = open("b");
    assert!(
        s.execute_sql("CREATE TABLE x (id INT PRIMARY KEY, a-b TEXT)")
            .is_err()
    );
    assert!(
        s.execute_sql("CREATE TABLE x (id INT PRIMARY KEY, 9a TEXT)")
            .is_err()
    );
    assert!(s.execute_sql("ALTER TABLE t ADD COLUMN a;b INT").is_err());
    assert!(s.execute_sql("ALTER TABLE t ADD COLUMN a'b INT").is_err());
    assert!(
        s.execute_sql("ALTER TABLE t RENAME COLUMN name TO a b")
            .is_err()
    );
    assert!(
        s.execute_sql("ALTER TABLE t RENAME COLUMN name TO a;b")
            .is_err()
    );
    assert!(s.execute_sql("CREATE INDEX i-1 ON t (name)").is_err());
    assert!(s.execute_sql("CREATE INDEX a;b ON t (name)").is_err());
}

#[test]
fn good_names_still_work() {
    let s = open("c");
    let ok64 = "a".repeat(64);
    s.execute_sql(&format!("CREATE TABLE {ok64} (id INT PRIMARY KEY)"))
        .unwrap();
    s.execute_sql("CREATE TABLE _x1 (id INT PRIMARY KEY, User_Name TEXT)")
        .unwrap();
    s.execute_sql("ALTER TABLE t ADD COLUMN age INT").unwrap();
    s.execute_sql("ALTER TABLE t RENAME COLUMN name TO nick_name")
        .unwrap();
    s.execute_sql("CREATE INDEX idx_nick ON t (nick_name)")
        .unwrap();
}

#[test]
fn reserved_dropped_prefix_is_rejected_for_columns() {
    let s = open("dropped_prefix");
    assert!(
        s.execute_sql("CREATE TABLE x (id INT PRIMARY KEY, __dropped_1 TEXT)")
            .is_err()
    );
    assert!(
        s.execute_sql("ALTER TABLE t ADD COLUMN __dropped_2 INT")
            .is_err()
    );
    assert!(
        s.execute_sql("ALTER TABLE t RENAME COLUMN name TO __dropped_3")
            .is_err()
    );
    assert!(
        s.table("y")
            .sync("id int primary key, __dropped_1 text")
            .is_err()
    );
    // nothing was created or changed
    assert!(s.execute_sql("SELECT * FROM x").is_err());
    assert!(s.execute_sql("SELECT id, name FROM t").is_ok());
}

// Separate on purpose: if this fails it is a real bug (a dropped column can be renamed back to life).
#[test]
fn dropped_column_cannot_be_renamed_back() {
    let s = open("dropped_rename");
    s.execute_sql("CREATE TABLE t2 (id INT PRIMARY KEY, a TEXT, b INT)")
        .unwrap();
    s.execute_sql("INSERT INTO t2 VALUES (1, 'x', 10)").unwrap();
    s.execute_sql("ALTER TABLE t2 DROP COLUMN b").unwrap();
    assert!(
        s.execute_sql("ALTER TABLE t2 RENAME COLUMN __dropped_2 TO c")
            .is_err()
    );
    assert!(s.execute_sql("SELECT c FROM t2").is_err());
}
