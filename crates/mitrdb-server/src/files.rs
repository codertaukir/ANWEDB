use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::{Error, Result};

const CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct StoredFile {
    pub name: String,
    pub size: u64,
}

pub struct FileStore {
    dir: PathBuf,
}

fn io_err(e: std::io::Error) -> Error {
    Error::Io(format!("{:?}", e.kind()))
}

fn valid_name(name: &str) -> bool {
    name.len() == 32
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn random_name() -> Result<String> {
    let mut raw = [0u8; 16];
    getrandom::getrandom(&mut raw).map_err(|_| Error::Io("RANDOM_FAILED".to_string()))?;
    let mut name = String::with_capacity(32);
    for b in raw {
        name.push_str(&format!("{:02x}", b));
    }
    Ok(name)
}

fn write_chunks<R: Read>(reader: &mut R, path: &Path) -> Result<u64> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path).map_err(io_err)?;
    let mut buf = vec![0u8; CHUNK_BYTES];
    let mut total = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(io_err(e)),
        };
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).map_err(io_err)?;
        total += n as u64;
    }
    file.sync_all().map_err(io_err)?;
    Ok(total)
}

impl FileStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        fs::create_dir_all(dir.as_ref()).map_err(io_err)?;
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.as_ref(), fs::Permissions::from_mode(0o700)).map_err(io_err)?;
        }
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
        })
    }

    pub fn save<R: Read>(&self, reader: &mut R) -> Result<StoredFile> {
        let name = random_name()?;
        let final_path = self.dir.join(&name);
        let tmp_path = self.dir.join(format!("{}.tmp", name));
        match write_chunks(reader, &tmp_path) {
            Ok(size) => {
                if let Err(e) = fs::rename(&tmp_path, &final_path) {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(io_err(e));
                }
                Ok(StoredFile { name, size })
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                Err(e)
            }
        }
    }

    pub fn open_read(&self, name: &str) -> Result<File> {
        if !valid_name(name) {
            return Err(Error::Io("INVALID_FILE_NAME".to_string()));
        }
        File::open(self.dir.join(name)).map_err(io_err)
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        if !valid_name(name) {
            return Err(Error::Io("INVALID_FILE_NAME".to_string()));
        }
        fs::remove_file(self.dir.join(name)).map_err(io_err)
    }
}

impl FileStore {
    /// Deletes leftover `<name>.tmp` files older than `max_age`.
    /// Saved files are never touched. Returns how many were removed.
    pub fn cleanup_tmp(&self, max_age: std::time::Duration) -> Result<usize> {
        let now = std::time::SystemTime::now();
        let mut removed = 0usize;
        for entry in fs::read_dir(&self.dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(stem) = file_name.strip_suffix(".tmp") else {
                continue;
            };
            if !valid_name(stem) {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let Ok(modified) = meta.modified() else {
                continue;
            };
            let age = now.duration_since(modified).unwrap_or_default();
            if age >= max_age && fs::remove_file(entry.path()).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}
