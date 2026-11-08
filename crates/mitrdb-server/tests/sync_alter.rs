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
fn rows(s: &Server, sql: &str) -> usize {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn sync_applies_nullable_changes_and_refuses_pk_change() {
    let d = fresh("syncalt");
    const NN: &str = "id int primary key, a text not null, b int";
    const NULLABLE: &str = "id int primary key, a text, b int";
    const BOTH: &str = "id int primary key, a text not null, b int not null";
    {
        let s = open(&d);
        let t = s.table("t");
        assert_eq!(t.sync(NN).unwrap().len(), 1);
        s.execute_sql("INSERT INTO t VALUES (1, 'x', 1)").unwrap();
        assert!(s.execute_sql("INSERT INTO t (id, b) VALUES (2, 2)").is_err());

        // NOT NULL -> nullable: one DROP NOT NULL step, nulls now allowed
        let plan = t.sync(NULLABLE).unwrap();
        assert_eq!(plan.len(), 1, "{plan:?}");
        assert!(plan[0].contains("DROP NOT NULL"), "{plan:?}");
        s.execute_sql("INSERT INTO t (id, b) VALUES (2, 2)").unwrap();

        // nullable -> NOT NULL with a NULL row: refused, names pk 2, column stays nullable
        let e = t.sync(NN).unwrap_err();
        assert!(format!("{e:?}").contains("Int64(2)"), "{e:?}");
        s.execute_sql("INSERT INTO t (id, b) VALUES (3, 3)").unwrap();
        s.execute_sql("DELETE FROM t WHERE id = 3").unwrap();

        // fix the data, then it applies
        s.execute_sql("UPDATE t SET a = 'z' WHERE id = 2").unwrap();
        let plan = t.sync(NN).unwrap();
        assert_eq!(plan.len(), 1, "{plan:?}");
        assert!(plan[0].contains("SET NOT NULL"), "{plan:?}");
        assert!(s.execute_sql("INSERT INTO t (id, b) VALUES (4, 4)").is_err());

        // second column: NULL in b blocks, then clears
        s.execute_sql("INSERT INTO t (id, a) VALUES (5, 'q')").unwrap();
        let e = t.sync(BOTH).unwrap_err();
        assert!(format!("{e:?}").contains("Int64(5)"), "{e:?}");
        s.execute_sql("DELETE FROM t WHERE id = 5").unwrap();
        assert_eq!(t.sync(BOTH).unwrap().len(), 1);

        // repeatable, and PK removal / change is still refused
        assert!(t.sync(BOTH).unwrap().is_empty());
        assert!(t.sync("id int, a text not null, b int not null").is_err());
        assert_eq!(rows(&s, "SELECT * FROM t"), 2);
    }
    let s = open(&d);
    assert!(s.execute_sql("INSERT INTO t (id, a) VALUES (6, 'n')").is_err(), "b must stay NOT NULL");
    assert!(s.table("t").sync("id int primary key, a text not null, b int not null").unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&d);
}
