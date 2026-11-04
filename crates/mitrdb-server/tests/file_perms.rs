use mitrdb_server::Server;
use std::os::unix::fs::PermissionsExt;

#[test]
fn db_and_wal_files_are_owner_only_after_open() {
    let dir = std::env::temp_dir().join(format!("mitrdb_fp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (s, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    s.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    let mut seen = 0;
    for e in std::fs::read_dir(&dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with("t.db") || name.starts_with("t.wal") {
            seen += 1;
            let mode = e.metadata().unwrap().permissions().mode() & 0o777;
            assert_eq!(mode & 0o077, 0, "{name} has mode {mode:o}");
        }
    }
    assert!(seen >= 2, "expected db and wal files, saw {seen}");
}
