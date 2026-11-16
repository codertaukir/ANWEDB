use mitrdb_server::{ExecutionResult, F64, Server, Value};

// copy of the old ORM encoding (orm lib.rs f64_enc / f64_dec)
fn enc(f: f64) -> i64 {
    let f = if f == 0.0 { 0.0 } else { f };
    let b = f.to_bits() as i64;
    if b < 0 { b ^ i64::MAX } else { b }
}
fn dec(v: i64) -> f64 {
    f64::from_bits((if v < 0 { v ^ i64::MAX } else { v }) as u64)
}

fn rows(s: &Server, sql: &str) -> Vec<Vec<Value>> {
    match s.execute_sql(sql).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows,
        o => panic!("expected rows, got {o:?}"),
    }
}

#[test]
fn legacy_int_to_native_float_timestamp() {
    let d = std::env::temp_dir().join(format!("mitrdb_legacy_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let s = Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0;

    s.execute_sql("CREATE TABLE lg (id INT PRIMARY KEY, f INT, ts INT)")
        .unwrap();
    let vals = [-1e10, -2.5, 0.0, 1.5, 3.14159, 1e300];
    for (i, v) in vals.iter().enumerate() {
        s.execute_sql(&format!(
            "INSERT INTO lg VALUES ({}, {}, {})",
            i + 1,
            enc(*v),
            1_700_000_000_000_000i64 + i as i64
        ))
        .unwrap();
    }

    s.execute_sql("ALTER TABLE lg ADD COLUMN f__mig float")
        .unwrap();
    s.execute_sql("ALTER TABLE lg ADD COLUMN ts__mig timestamp")
        .unwrap();
    for r in rows(&s, "SELECT id, f, ts FROM lg") {
        let (id, f, ts) = match (&r[0], &r[1], &r[2]) {
            (Value::Int64(a), Value::Int64(b), Value::Int64(c)) => (*a, *b, *c),
            o => panic!("unexpected row {o:?}"),
        };
        s.execute_sql_params(
            "UPDATE lg SET f__mig = $1, ts__mig = $2 WHERE id = $3",
            &[
                Value::Float(F64::new(dec(f))),
                Value::Int64(ts),
                Value::Int64(id),
            ],
        )
        .unwrap();
    }
    s.execute_sql("ALTER TABLE lg DROP COLUMN f").unwrap();
    s.execute_sql("ALTER TABLE lg DROP COLUMN ts").unwrap();
    s.execute_sql("ALTER TABLE lg RENAME COLUMN f__mig TO f")
        .unwrap();
    s.execute_sql("ALTER TABLE lg RENAME COLUMN ts__mig TO ts")
        .unwrap();

    let out = rows(&s, "SELECT id, f, ts FROM lg ORDER BY id");
    eprintln!("MIG {:?}", out);
    assert_eq!(out.len(), vals.len());
    for (i, r) in out.iter().enumerate() {
        match (&r[1], &r[2]) {
            (Value::Float(x), Value::Int64(t)) => {
                assert_eq!(x.0, vals[i], "row {i}");
                assert_eq!(*t, 1_700_000_000_000_000i64 + i as i64);
            }
            o => panic!("row {i}: {o:?}"),
        }
    }
    // does engine ALTER TYPE int -> timestamp work at all? (info only)
    s.execute_sql("CREATE TABLE lg2 (id INT PRIMARY KEY, ts INT)")
        .unwrap();
    s.execute_sql("INSERT INTO lg2 VALUES (1, 5)").unwrap();
    eprintln!(
        "ALTER_TS_OK={}",
        s.execute_sql("ALTER TABLE lg2 ALTER COLUMN ts TYPE timestamp")
            .is_ok()
    );
}
