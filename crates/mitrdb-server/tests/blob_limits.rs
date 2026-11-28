use mitrdb_common::Error;
use mitrdb_server::Server;
use mitrdb_storage::DEFAULT_MAX_BLOB_BYTES;

fn setup(tag: &str) -> (std::path::PathBuf, Server) {
    let dir = std::env::temp_dir().join(format!("mitrdb_a10_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (s, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    (dir, s)
}

#[test]
fn default_max_is_128mb_and_configurable() {
    let (dir, s) = setup("default");
    assert_eq!(s.max_blob_bytes(), DEFAULT_MAX_BLOB_BYTES);
    assert_eq!(DEFAULT_MAX_BLOB_BYTES, 128 * 1024 * 1024);
    s.set_max_blob_bytes(100);
    assert_eq!(s.max_blob_bytes(), 100);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn exceeding_configured_max_is_out_of_space_and_writer_stays_failed() {
    let (dir, s) = setup("max");
    s.set_max_blob_bytes(100);
    let sess = s.create_session();
    s.begin_blob_write_default(sess).unwrap();
    s.write_blob_chunk(sess, &[1u8; 100]).unwrap();
    let fin = s.finish_blob_write(sess).unwrap();
    assert_eq!(fin.total_len, 100);
    s.execute_in_session(sess, "COMMIT").unwrap();
    s.close_session(sess).unwrap();
    let sess2 = s.create_session();
    s.begin_blob_write_default(sess2).unwrap();
    s.write_blob_chunk(sess2, &[1u8; 100]).unwrap();
    let err = s.write_blob_chunk(sess2, &[1u8; 1]).unwrap_err();
    assert!(matches!(err, Error::OutOfSpace), "expected OutOfSpace, got {err:?}");
    assert!(s.finish_blob_write(sess2).is_err());
    s.close_session(sess2).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn sql_literal_inline_path_is_bounded() {
    let (dir, s) = setup("literal");
    s.execute_sql("CREATE TABLE t (id INT64 PRIMARY KEY, data TEXT)").unwrap();
    let big = "a".repeat(300_000);
    let sql = format!("INSERT INTO t VALUES (1, '{big}')");
    let err = s.execute_sql(&sql).unwrap_err();
    assert!(
        matches!(err, Error::OutOfSpace) || format!("{err:?}").contains("OutOfSpace"),
        "expected OutOfSpace, got {err:?}"
    );
    s.execute_sql("INSERT INTO t VALUES (1, 'ok')").unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
