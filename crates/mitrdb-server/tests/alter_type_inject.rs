// Page-op failure injected at every point of ALTER COLUMN TYPE: the table must stay usable,
// a retry must succeed, and the new type must survive a crash/reopen.
// Run: cargo test -p mitrdb-server --features fault-injection --test alter_type_inject -- --nocapture
use mitrdb_server::{ExecutionResult, Server};
use std::path::PathBuf;

const N: i64 = 300;

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
fn dump(s: &Server, sql: &str) -> String {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => format!("{:?}", rows),
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn page_op_failure_anywhere_in_alter_type_is_recoverable() {
    let points: Vec<i64> = (0..60).chain((60..3000).step_by(37)).collect();
    let (mut fired, mut ran) = (0usize, 0usize);
    for &n in &points {
        let d = fresh(&format!("atinj_{n}"));
        {
            let s = open(&d);
            s.execute_sql("CREATE TABLE t (id int primary key, a text, c text)").unwrap();
            s.execute_sql("BEGIN").unwrap();
            for i in 0..N {
                s.execute_sql(&format!("INSERT INTO t VALUES ({i}, 'a{i}', '{i}')")).unwrap();
            }
            s.execute_sql("COMMIT").unwrap();

            s.set_fail_page_op_after(n);
            let r = s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int");
            s.set_fail_page_op_after(-1);
            ran += 1;
            if r.is_err() {
                fired += 1;
                // table still usable, same width, same row count, value readable either way
                assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: shape after failure");
                assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains('7'), "n={n}: value lost");
                s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int")
                    .unwrap_or_else(|e| panic!("n={n}: retry failed: {e:?}"));
            }
            assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains("Int64(7)"), "n={n}: not int after change");
            assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: final shape");
            assert_eq!(shape(&s, "SELECT * FROM t WHERE c = 150"), (3, 1), "n={n}: filter on new type");
        } // crash
        let s = open(&d);
        assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: shape after reopen");
        assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains("Int64(7)"), "n={n}: type lost after reopen");
        assert!(s.execute_sql("INSERT INTO t VALUES (9999, 'z', 'text-not-int')").is_err(), "n={n}: type not enforced");
        let _ = std::fs::remove_dir_all(&d);
    }
    eprintln!("INJECT points_run={ran} fired={fired}");
    assert!(fired > 0, "injection never fired: test proves nothing");
}

#[test]
fn wal_append_failure_in_alter_type_is_recoverable() {
    let points: Vec<i64> = (0..20).collect();
    let (mut fired, mut ran) = (0usize, 0usize);
    for &n in &points {
        let d = fresh(&format!("atwal_{n}"));
        {
            let s = open(&d);
            s.execute_sql("CREATE TABLE t (id int primary key, a text, c text)").unwrap();
            s.execute_sql("BEGIN").unwrap();
            for i in 0..N {
                s.execute_sql(&format!("INSERT INTO t VALUES ({i}, 'a{i}', '{i}')")).unwrap();
            }
            s.execute_sql("COMMIT").unwrap();

            s.set_fail_wal_append_after(n);
            let r = s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int");
            s.set_fail_wal_append_after(-1);
            ran += 1;
            if r.is_err() {
                fired += 1;
                assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: shape after wal failure");
                assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains('7'), "n={n}: value lost");
                s.execute_sql("ALTER TABLE t ALTER COLUMN c TYPE int")
                    .unwrap_or_else(|e| panic!("n={n}: retry failed after wal error: {e:?}"));
            }
            assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains("Int64(7)"), "n={n}: not int after change");
            assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: final shape");
        } // crash/reopen
        let s = open(&d);
        assert_eq!(shape(&s, "SELECT * FROM t"), (3, N as usize), "n={n}: shape after reopen");
        assert!(dump(&s, "SELECT c FROM t WHERE id = 7").contains("Int64(7)"), "n={n}: type lost after reopen");
        let _ = std::fs::remove_dir_all(&d);
    }
    eprintln!("WAL INJECT points_run={ran} fired={fired}");
    assert!(fired > 0, "wal injection never fired");
}
