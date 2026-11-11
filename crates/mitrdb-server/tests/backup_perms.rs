#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::Server;
use std::os::unix::fs::PermissionsExt;

#[test]
fn backup_is_owner_only_and_refuses_existing_destination() {
    let dir = std::env::temp_dir().join(format!("mitrdb_bp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (s, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();
    s.execute_sql("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    let bk = dir.join("bk");
    s.backup(&bk).unwrap();
    let dmode = std::fs::metadata(&bk).unwrap().permissions().mode() & 0o777;
    assert_eq!(dmode & 0o077, 0, "backup dir mode {dmode:o}");
    let mut seen = 0;
    for e in std::fs::read_dir(&bk).unwrap().flatten() {
        seen += 1;
        let mode = e.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode & 0o077, 0, "{:?} has mode {mode:o}", e.file_name());
    }
    assert!(seen >= 2, "expected backup files, saw {seen}");
    assert!(
        s.backup(&bk).is_err(),
        "existing destination must be refused"
    );
}
