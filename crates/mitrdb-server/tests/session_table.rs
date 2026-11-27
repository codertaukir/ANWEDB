use mitrdb_server::{Server, val};
use std::path::PathBuf;
use std::sync::Arc;

fn dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("mitrdb_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn open(d: &std::path::Path) -> Arc<Server> {
    let (s, _) = Server::open(d.join("t.db"), d.join("t.wal"), 128).unwrap();
    Arc::new(s)
}

#[test]
fn session_table_isolation_fk_cascade_and_rollback() {
    let d = dir("sess_tbl_iso");
    let s = open(&d);

    s.table("orgs")
        .sync("id int primary key, name text not null")
        .unwrap();
    s.table("members")
        .sync("id int primary key, org_id int not null, role text, score int")
        .unwrap();
    s.table("members")
        .add_foreign_key_on("org_id", "orgs", "id", true)
        .unwrap();
    s.table("members")
        .add_check("score", ">=", val(0))
        .unwrap();
    s.table("members")
        .set_default("role", val("viewer"))
        .unwrap();

    let sid_a = s.create_session();
    let sid_b = s.create_session();

    // Session A inserts parent + child inside an explicit transaction
    s.execute_in_session(sid_a, "BEGIN").unwrap();
    s.session_table(sid_a, "orgs")
        .insert(&[("id", val(1)), ("name", val("Acme"))])
        .unwrap();
    s.session_table(sid_a, "members")
        .insert(&[("id", val(10)), ("org_id", val(1)), ("score", val(50))])
        .unwrap();

    // CHECK & FK violations inside session A are rejected cleanly without killing txn
    assert!(
        s.session_table(sid_a, "members")
            .insert(&[("id", val(11)), ("org_id", val(1)), ("score", val(-5))])
            .is_err()
    );
    assert!(
        s.session_table(sid_a, "members")
            .insert(&[("id", val(12)), ("org_id", val(999)), ("score", val(10))])
            .is_err()
    );

    // Session B does not see uncommitted rows from Session A
    assert_eq!(s.session_table(sid_b, "orgs").count().unwrap(), 0);
    assert_eq!(s.session_table(sid_b, "members").count().unwrap(), 0);

    s.execute_in_session(sid_a, "COMMIT").unwrap();

    // Now Session B sees committed rows + DEFAULT applied
    assert_eq!(s.session_table(sid_b, "orgs").count().unwrap(), 1);
    let m = s
        .session_table(sid_b, "members")
        .find()
        .columns(&["role", "score"])
        .filter("id", "=", val(10))
        .one()
        .unwrap()
        .unwrap();
    assert_eq!(m[0], val("viewer"));
    assert_eq!(m[1], val(50));

    // Session B cascades delete in a transaction, then rolls back -> both parent & child survive
    s.execute_in_session(sid_b, "BEGIN").unwrap();
    assert_eq!(
        s.session_table(sid_b, "orgs")
            .delete()
            .filter("id", "=", val(1))
            .run()
            .unwrap(),
        1
    );
    assert_eq!(s.session_table(sid_b, "members").count().unwrap(), 0);
    s.execute_in_session(sid_b, "ROLLBACK").unwrap();

    assert_eq!(s.session_table(sid_a, "orgs").count().unwrap(), 1);
    assert_eq!(s.session_table(sid_a, "members").count().unwrap(), 1);

    s.close_session(sid_a).unwrap();
    s.close_session(sid_b).unwrap();
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn session_table_concurrent_workers_bypass_default_session_txn() {
    let d = dir("sess_tbl_conc");
    let s = open(&d);
    s.table("events")
        .sync("id int primary key, worker int not null, seq int not null")
        .unwrap();

    // Hold an explicit transaction open on default_session; session_table workers must still proceed freely
    s.execute_sql("BEGIN").unwrap();
    s.table("events")
        .insert(&[("id", val(999_999)), ("worker", val(-1)), ("seq", val(0))])
        .unwrap();

    let mut handles = Vec::new();
    for w in 0..8i64 {
        let srv = Arc::clone(&s);
        handles.push(std::thread::spawn(move || {
            let sid = srv.create_session();
            for i in 0..25i64 {
                srv.execute_in_session(sid, "BEGIN").unwrap();
                let id = w * 1000 + i;
                srv.session_table(sid, "events")
                    .insert(&[("id", val(id)), ("worker", val(w)), ("seq", val(i))])
                    .unwrap();
                srv.session_table(sid, "events")
                    .update()
                    .set("seq", val(i + 100))
                    .filter("id", "=", val(id))
                    .run()
                    .unwrap();
                srv.execute_in_session(sid, "COMMIT").unwrap();
            }
            let cnt = srv
                .session_table(sid, "events")
                .find()
                .filter("worker", "=", val(w))
                .count()
                .unwrap();
            assert_eq!(cnt, 25);
            srv.close_session(sid).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    s.execute_sql("ROLLBACK").unwrap();
    assert_eq!(s.table("events").count().unwrap(), 200);
    let _ = std::fs::remove_dir_all(&d);
}
