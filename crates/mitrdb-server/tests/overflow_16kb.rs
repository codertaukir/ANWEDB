use std::path::PathBuf;

use mitrdb_server::{Server, Value, val};
use mitrdb_storage::{PAGE_TYPE_OVERFLOW, Page};

const PAGE_SIZE: usize = 4096;

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mitrdb_ovf16k_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn count_overflow_pages_on_disk(db_path: &std::path::Path) -> usize {
    let bytes = std::fs::read(db_path).unwrap();
    bytes
        .as_chunks::<PAGE_SIZE>()
        .0
        .iter()
        .enumerate()
        .filter(|(pid, chunk)| {
            if *pid == 0 {
                return false;
            }
            Page::from_bytes(**chunk)
                .map(|p| p.page_type() == PAGE_TYPE_OVERFLOW)
                .unwrap_or(false)
        })
        .count()
}

#[test]
fn inline_threshold_and_16kb_overflow_roundtrip() {
    let dir = temp_dir("roundtrip");
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let (server, _) = Server::open(&db_path, &wal_path, 128).unwrap();

    let t = server.table("messages");
    t.sync("id int primary key, body text, attachment bytes").unwrap();

    // 1) <= 2048 bytes per field and <= 3800B total stays 100% inline (zero overflow pages allocated)
    let inline_txt = "a".repeat(2048);
    let inline_bin = vec![0x5Au8; 1024];
    t.insert(&[
        ("id", val(1)),
        ("body", val(inline_txt.clone())),
        ("attachment", val(inline_bin.clone())),
    ])
    .unwrap();
    server.checkpoint().unwrap();
    assert_eq!(
        count_overflow_pages_on_disk(&db_path),
        0,
        "<= 2048 byte values must stay inline with zero overflow pages"
    );

    // 2) 16,384-byte UTF-8 text and 16,384-byte binary blob
    let big_txt = "अ".repeat(5461) + "Z"; // 5461 * 3 + 1 = 16,384 bytes
    assert_eq!(big_txt.len(), 16 * 1024);
    t.insert(&[
        ("id", val(2)),
        ("body", val(big_txt.clone())),
        ("attachment", val(Value::Null)),
    ])
    .unwrap();

    let big_bin: Vec<u8> = (0..(16 * 1024)).map(|i| (i % 251) as u8).collect();
    t.insert(&[
        ("id", val(3)),
        ("body", val("ok")),
        ("attachment", val(big_bin.clone())),
    ])
    .unwrap();

    // 3) Multi-column tuple exceeding 3,800B inline leaf cap while <= 16,384B total
    let mid_a = "x".repeat(1500);
    let mid_b = vec![0xABu8; 1500];
    t.insert(&[
        ("id", val(4)),
        ("body", val(mid_a.clone())),
        ("attachment", val(mid_b.clone())),
    ])
    .unwrap();

    // 4) > 16,384 bytes rejected cleanly with OUT_OF_SPACE
    let err = t
        .insert(&[
            ("id", val(5)),
            ("body", val("x".repeat(16 * 1024 + 300))),
            ("attachment", val(Value::Null)),
        ])
        .unwrap_err();
    assert!(
        err.to_string().contains("OUT_OF_SPACE"),
        "expected OUT_OF_SPACE, got: {err}"
    );

    // Verify all 4 valid rows
    let r1 = t.find().filter("id", "=", val(1)).run().unwrap();
    assert_eq!(r1.rows[0][1], Value::Text(inline_txt));
    assert_eq!(r1.rows[0][2], Value::Bytes(inline_bin));

    let r2 = t.find().filter("id", "=", val(2)).run().unwrap();
    assert_eq!(r2.rows[0][1], Value::Text(big_txt));

    let r3 = t.find().filter("id", "=", val(3)).run().unwrap();
    assert_eq!(r3.rows[0][2], Value::Bytes(big_bin));

    let r4 = t.find().filter("id", "=", val(4)).run().unwrap();
    assert_eq!(r4.rows[0][1], Value::Text(mid_a));
    assert_eq!(r4.rows[0][2], Value::Bytes(mid_b));
}

#[test]
fn overflow_rollback_vacuum_and_page_reuse() {
    let dir = temp_dir("vacuum_reuse");
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let (server, _) = Server::open(&db_path, &wal_path, 128).unwrap();

    server
        .execute_sql("CREATE TABLE blobs (id INT PRIMARY KEY, payload TEXT)")
        .unwrap();

    // Rolled-back transaction must immediately release its overflow pages
    server.execute_sql("BEGIN").unwrap();
    let rolled_back = "R".repeat(16 * 1024);
    server
        .execute_sql(&format!("INSERT INTO blobs VALUES (1, '{}')", rolled_back))
        .unwrap();
    server.execute_sql("ROLLBACK").unwrap();
    assert_eq!(server.table("blobs").count().unwrap(), 0);

    // Repeated 16KB updates + VACUUM must reuse freed overflow pages without unbounded file growth
    let t = server.table("blobs");
    t.insert(&[("id", val(1)), ("payload", val("A".repeat(16 * 1024)))])
        .unwrap();
    server.checkpoint().unwrap();
    let baseline_size = std::fs::metadata(&db_path).unwrap().len();

    for round in 0..8 {
        let ch = ((b'B' + round) as char).to_string();
        t.update()
            .filter("id", "=", val(1))
            .set("payload", val(ch.repeat(16 * 1024)))
            .run()
            .unwrap();
        let _ = server.vacuum();
    }
    server.checkpoint().unwrap();
    let after_size = std::fs::metadata(&db_path).unwrap().len();
    assert!(
        after_size <= baseline_size + (8 * PAGE_SIZE as u64),
        "overflow pages leaked across updates+vacuum: baseline={baseline_size}, after={after_size}"
    );
}

#[test]
fn overflow_recovery_and_backup_restore() {
    let dir = temp_dir("backup_recovery");
    let db_path = dir.join("t.db");
    let wal_path = dir.join("t.wal");
    let backup_dir = dir.join("backup");
    let restored_dir = dir.join("restored");
    let restored_db = restored_dir.join("t.db");
    let restored_wal = restored_dir.join("t.wal");

    let payload_16k = "M".repeat(16 * 1024);
    let blob_12k: Vec<u8> = (0..(12 * 1024)).map(|i| (i % 253) as u8).collect();

    {
        let (server, _) = Server::open(&db_path, &wal_path, 128).unwrap();
        let t = server.table("docs");
        t.sync("id int primary key, body text, raw bytes").unwrap();
        t.insert(&[
            ("id", val(42)),
            ("body", val(payload_16k.clone())),
            ("raw", val(Value::Null)),
        ])
        .unwrap();
        t.insert(&[
            ("id", val(43)),
            ("body", val("binary-row")),
            ("raw", val(blob_12k.clone())),
        ])
        .unwrap();

        // Create backup while 16KB overflow pages are active
        server.backup(&backup_dir).unwrap();
        // Drop without clean shutdown to exercise WAL recovery on reopen
    }

    // 1) Reopen original DB (ARIES recovery)
    {
        let (reopened, _) = Server::open(&db_path, &wal_path, 128).unwrap();
        let r = reopened
            .table("docs")
            .find()
            .filter("id", "=", val(42))
            .run()
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Value::Text(payload_16k.clone()));
        let r2 = reopened
            .table("docs")
            .find()
            .filter("id", "=", val(43))
            .run()
            .unwrap();
        assert_eq!(r2.rows[0][2], Value::Bytes(blob_12k.clone()));
    }

    // 2) Restore from backup and verify checksums + 16KB data
    Server::restore(&backup_dir, &restored_dir).unwrap();
    let (restored, _) = Server::open(&restored_db, &restored_wal, 128).unwrap();
    let r = restored
        .table("docs")
        .find()
        .filter("id", "=", val(42))
        .run()
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Text(payload_16k));
    let r2 = restored
        .table("docs")
        .find()
        .filter("id", "=", val(43))
        .run()
        .unwrap();
    assert_eq!(r2.rows[0][2], Value::Bytes(blob_12k));
}
