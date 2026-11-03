use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

fn fresh(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("anwedb_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn open(d: &PathBuf) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}
fn shape(s: &Server, sql: &str) -> (usize, usize) {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { columns, rows } => (columns.len(), rows.len()),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn drop_column_hides_keeps_insert_working_and_persists() {
    let d = fresh("dropcol");
    {
        let s = open(&d);
        s.execute_sql("CREATE TABLE t (id int primary key, a text, b int, c text)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 10, 'k')").unwrap();

        s.execute_sql("ALTER TABLE t DROP COLUMN b").unwrap();
        assert_eq!(shape(&s, "SELECT * FROM t"), (3, 1));
        assert!(s.execute_sql("SELECT b FROM t").is_err());

        // positional insert with live columns only
        s.execute_sql("INSERT INTO t VALUES (2, 'y', 'm')").unwrap();
        // column-list insert
        s.execute_sql("INSERT INTO t (id, a) VALUES (3, 'z')").unwrap();
        assert_eq!(shape(&s, "SELECT * FROM t"), (3, 3));

        // refusals
        assert!(s.execute_sql("ALTER TABLE t DROP COLUMN id").is_err());
        assert!(s.execute_sql("ALTER TABLE t DROP COLUMN nope").is_err());
        s.execute_sql("CREATE UNIQUE INDEX t_c_uq ON t (c)").unwrap();
        assert!(s.execute_sql("ALTER TABLE t DROP COLUMN c").is_err());
    }
    let s = open(&d);
    assert_eq!(shape(&s, "SELECT * FROM t"), (3, 3));
    // re-adding the old name gives a fresh NULL column, old data does not come back
    s.execute_sql("ALTER TABLE t ADD COLUMN b int").unwrap();
    assert_eq!(shape(&s, "SELECT * FROM t WHERE b IS NULL"), (4, 3));
    // define_table / sync ignore dropped columns
    let plan = s.table("t").sync("id int primary key, a text, c text unique, b int").unwrap();
    assert!(plan.is_empty(), "unexpected plan: {plan:?}");
    let _ = std::fs::remove_dir_all(&d);
}
