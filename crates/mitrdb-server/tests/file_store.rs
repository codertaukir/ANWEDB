use mitrdb_server::FileStore;
use std::io::Read;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "anwedb_files_{}_{}_{}",
        tag,
        std::process::id(),
        nanos
    ))
}

struct FailReader {
    sent: bool,
}

impl Read for FailReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.sent {
            self.sent = true;
            buf[..10].fill(7);
            Ok(10)
        } else {
            Err(std::io::Error::other("boom"))
        }
    }
}

#[test]
fn big_file_round_trips_in_chunks() {
    let dir = temp_dir("roundtrip");
    let store = FileStore::open(&dir).unwrap();
    let data: Vec<u8> = (0..(3 * 1024 * 1024 + 123))
        .map(|i| (i % 251) as u8)
        .collect();
    let saved = store.save(&mut &data[..]).unwrap();
    assert_eq!(saved.size, data.len() as u64);
    assert_eq!(saved.name.len(), 32);
    let mut back = Vec::new();
    store
        .open_read(&saved.name)
        .unwrap()
        .read_to_end(&mut back)
        .unwrap();
    assert!(back == data);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn empty_file_and_unique_names() {
    let dir = temp_dir("empty");
    let store = FileStore::open(&dir).unwrap();
    let a = store.save(&mut &b""[..]).unwrap();
    let b = store.save(&mut &b""[..]).unwrap();
    assert_eq!(a.size, 0);
    assert_ne!(a.name, b.name);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bad_names_are_rejected() {
    let dir = temp_dir("badnames");
    let store = FileStore::open(&dir).unwrap();
    let outside = dir.parent().unwrap().join("anwedb_outside_target.txt");
    std::fs::write(&outside, b"secret").unwrap();
    let upper = "A".repeat(32);
    let slash = format!("{}/{}", "a".repeat(15), "b".repeat(16));
    let attacks = [
        "../anwedb_outside_target.txt",
        "..",
        ".",
        "",
        "abc",
        "/etc/passwd",
        upper.as_str(),
        slash.as_str(),
    ];
    for name in attacks {
        assert!(store.open_read(name).is_err(), "open_read accepted {:?}", name);
        assert!(store.delete(name).is_err(), "delete accepted {:?}", name);
    }
    assert!(outside.exists());
    std::fs::remove_file(&outside).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn delete_removes_the_file() {
    let dir = temp_dir("delete");
    let store = FileStore::open(&dir).unwrap();
    let saved = store.save(&mut &b"hello"[..]).unwrap();
    store.delete(&saved.name).unwrap();
    assert!(store.open_read(&saved.name).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn failed_upload_leaves_no_leftovers() {
    let dir = temp_dir("fail");
    let store = FileStore::open(&dir).unwrap();
    let mut reader = FailReader { sent: false };
    assert!(store.save(&mut reader).is_err());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[test]
fn permissions_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir("perm");
    let store = FileStore::open(&dir).unwrap();
    let saved = store.save(&mut &b"x"[..]).unwrap();
    let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    let file_mode = std::fs::metadata(dir.join(&saved.name))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(dir_mode, 0o700);
    assert_eq!(file_mode, 0o600);
    std::fs::remove_dir_all(&dir).unwrap();
}

fn make_old(path: &std::path::Path, secs_ago: u64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(secs_ago))
        .unwrap();
}

#[test]
fn cleanup_removes_only_old_tmp_files() {
    let dir = temp_dir("cleanup");
    let store = FileStore::open(&dir).unwrap();
    let saved = store.save(&mut &b"keep me"[..]).unwrap();
    make_old(&dir.join(&saved.name), 7200);

    let old_tmp = dir.join(format!("{}.tmp", "a".repeat(32)));
    let new_tmp = dir.join(format!("{}.tmp", "b".repeat(32)));
    let other = dir.join("notes.tmp");
    std::fs::write(&old_tmp, b"x").unwrap();
    std::fs::write(&new_tmp, b"y").unwrap();
    std::fs::write(&other, b"z").unwrap();
    make_old(&old_tmp, 7200);
    make_old(&other, 7200);

    let removed = store
        .cleanup_tmp(std::time::Duration::from_secs(3600))
        .unwrap();
    assert_eq!(removed, 1);
    assert!(!old_tmp.exists());
    assert!(new_tmp.exists());
    assert!(other.exists());
    let mut back = Vec::new();
    store
        .open_read(&saved.name)
        .unwrap()
        .read_to_end(&mut back)
        .unwrap();
    assert_eq!(back, b"keep me");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[test]
fn cleanup_does_not_follow_symlinks() {
    let dir = temp_dir("cleanup_link");
    let store = FileStore::open(&dir).unwrap();
    let outside = dir.parent().unwrap().join("anwedb_cleanup_outside.txt");
    std::fs::write(&outside, b"secret").unwrap();
    let link = dir.join(format!("{}.tmp", "d".repeat(32)));
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let removed = store
        .cleanup_tmp(std::time::Duration::from_secs(0))
        .unwrap();
    assert_eq!(removed, 0);
    assert!(outside.exists());
    std::fs::remove_file(&outside).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
