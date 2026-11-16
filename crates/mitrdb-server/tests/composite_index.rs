use mitrdb_server::{Server, Value};

#[test]
fn composite_index() {
    let dir = std::env::temp_dir().join(format!("mitrdb_comp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (dp, wp) = (dir.join("t.db"), dir.join("t.wal"));
    {
        let (db, _) = Server::open(&dp, &wp, 64).unwrap();
        db.execute_sql("CREATE TABLE l (id INT PRIMARY KEY, u INT, p INT, a TEXT, b TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO l VALUES (1, 1, 1, 'ab', 'c')")
            .unwrap();
        db.execute_sql("INSERT INTO l VALUES (2, 1, 2, 'a', 'bc')")
            .unwrap();
        // backfill on existing rows
        db.table("l")
            .create_index("l_up", &["u", "p"], true)
            .unwrap();
        db.table("l")
            .create_index("l_up", &["u", "p"], true)
            .unwrap(); // idempotent
        db.table("l")
            .create_index("l_ab", &["a", "b"], true)
            .unwrap(); // ("ab","c") != ("a","bc")
        assert!(
            db.execute_sql("INSERT INTO l VALUES (3, 1, 1, 'x', 'y')")
                .is_err()
        ); // dup (u,p)
        assert!(
            db.execute_sql("INSERT INTO l VALUES (4, 9, 9, 'ab', 'c')")
                .is_err()
        ); // dup (a,b)
        db.execute_sql("INSERT INTO l VALUES (5, 1, NULL, 'q', 'r')")
            .unwrap(); // NULL part ok
        db.execute_sql("INSERT INTO l VALUES (6, 1, NULL, 's', 't')")
            .unwrap();
        assert!(db.execute_sql("UPDATE l SET p = 1 WHERE id = 2").is_err()); // update collides
        db.execute_sql("UPDATE l SET p = 7 WHERE id = 2").unwrap();
        db.execute_sql("INSERT INTO l VALUES (7, 1, 2, 'm', 'n')")
            .unwrap(); // old (1,2) freed
        db.execute_sql("DELETE FROM l WHERE id = 1").unwrap();
        db.execute_sql("INSERT INTO l VALUES (8, 1, 1, 'ab', 'c')")
            .unwrap(); // freed by delete
        assert!(
            db.execute_sql("CREATE UNIQUE INDEX bad ON l (u, id2)")
                .is_err()
        );
        assert!(db.execute_sql("ALTER TABLE l DROP COLUMN p").is_err()); // in composite index
    }
    let (db, _) = Server::open(&dp, &wp, 64).unwrap(); // composite meta reload
    assert!(
        db.execute_sql("INSERT INTO l VALUES (9, 1, 1, 'z', 'z')")
            .is_err()
    );
    assert_eq!(db.table("l").find().run().unwrap().rows.len(), 5);
    let _ = Value::Null;
}
