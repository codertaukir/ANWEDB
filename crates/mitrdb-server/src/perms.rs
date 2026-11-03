use std::path::Path;

#[cfg(unix)]
pub(crate) fn restrict_db_files(db: &Path, wal: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for base in [db, wal] {
        let Some(name) = base.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let dir = match base.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let fname = entry.file_name();
            let Some(f) = fname.to_str() else { continue };
            if !f.starts_with(name) {
                continue;
            }
            let path = entry.path();
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if meta.file_type().is_file() {
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
}

#[cfg(not(unix))]
pub(crate) fn restrict_db_files(_db: &Path, _wal: &Path) {}
