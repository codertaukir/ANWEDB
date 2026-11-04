use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mitrdb_common::{Error, Lsn, PAGE_SIZE, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;

use crate::buffer_pool::BufferPoolManager;
use crate::crc32c::{self, Crc32cHasher};
use crate::disk::DiskManager;
use crate::page::Page;
use crate::recovery::RecoveryManager;
use crate::wal::{CheckpointMetadata, LogRecord, LogRecordType, WalManager};

pub const BACKUP_MANIFEST_MAGIC: [u8; 8] = *b"MITRBKP1";
pub const BACKUP_MANIFEST_VERSION: u32 = 1;

static NEXT_BACKUP_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupFileMeta {
    pub file_name: String,
    pub size_bytes: u64,
    pub crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    pub manifest_version: u32,
    pub backup_id: u64,
    pub created_at_unix_ts: u64,
    pub checkpoint_lsn: Lsn,
    pub min_recovery_lsn: Lsn,
    pub backup_start_lsn: Lsn,
    pub backup_end_lsn: Lsn,
    pub max_segment_bytes: u64,
    pub max_user_txn_id: TxnId,
    pub max_sys_txn_id: TxnId,
    pub db_file: BackupFileMeta,
    pub ckpt_file: Option<BackupFileMeta>,
    pub wal_segments: Vec<BackupFileMeta>,
}

impl BackupManifest {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(512);
        buf.extend_from_slice(&BACKUP_MANIFEST_MAGIC);
        buf.extend_from_slice(&self.manifest_version.to_le_bytes());
        buf.extend_from_slice(&self.backup_id.to_le_bytes());
        buf.extend_from_slice(&self.created_at_unix_ts.to_le_bytes());
        buf.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf.extend_from_slice(&self.min_recovery_lsn.to_le_bytes());
        buf.extend_from_slice(&self.backup_start_lsn.to_le_bytes());
        buf.extend_from_slice(&self.backup_end_lsn.to_le_bytes());
        buf.extend_from_slice(&self.max_segment_bytes.to_le_bytes());
        buf.extend_from_slice(&self.max_user_txn_id.to_le_bytes());
        buf.extend_from_slice(&self.max_sys_txn_id.to_le_bytes());

        Self::encode_file_meta(&mut buf, &self.db_file);

        if let Some(ref ckpt) = self.ckpt_file {
            buf.push(1);
            Self::encode_file_meta(&mut buf, ckpt);
        } else {
            buf.push(0);
        }

        buf.extend_from_slice(&(self.wal_segments.len() as u32).to_le_bytes());
        for seg in &self.wal_segments {
            Self::encode_file_meta(&mut buf, seg);
        }

        let crc = crc32c::checksum(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 88 {
            return Err(Error::BackupError(
                "Backup manifest buffer too short".to_string(),
            ));
        }

        let stored_crc = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let computed_crc = crc32c::checksum(&buf[..buf.len() - 4]);
        if stored_crc != computed_crc {
            return Err(Error::BackupError(format!(
                "Backup manifest CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        if buf[0..8] != BACKUP_MANIFEST_MAGIC {
            return Err(Error::BackupError(
                "Invalid backup manifest magic bytes".to_string(),
            ));
        }

        let manifest_version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if manifest_version != BACKUP_MANIFEST_VERSION {
            return Err(Error::BackupError(format!(
                "Unsupported backup manifest version: {}",
                manifest_version
            )));
        }

        let backup_id = u64::from_le_bytes(buf[12..20].try_into().unwrap());
        let created_at_unix_ts = u64::from_le_bytes(buf[20..28].try_into().unwrap());
        let checkpoint_lsn = u64::from_le_bytes(buf[28..36].try_into().unwrap());
        let min_recovery_lsn = u64::from_le_bytes(buf[36..44].try_into().unwrap());
        let backup_start_lsn = u64::from_le_bytes(buf[44..52].try_into().unwrap());
        let backup_end_lsn = u64::from_le_bytes(buf[52..60].try_into().unwrap());
        let max_segment_bytes = u64::from_le_bytes(buf[60..68].try_into().unwrap());
        let max_user_txn_id = u64::from_le_bytes(buf[68..76].try_into().unwrap());
        let max_sys_txn_id = u64::from_le_bytes(buf[76..84].try_into().unwrap());

        let mut offset = 84;
        let db_file = Self::decode_file_meta(buf, &mut offset)?;

        if offset >= buf.len() - 4 {
            return Err(Error::BackupError(
                "Truncated backup manifest at ckpt flag".to_string(),
            ));
        }
        let has_ckpt = buf[offset];
        offset += 1;
        let ckpt_file = if has_ckpt == 1 {
            Some(Self::decode_file_meta(buf, &mut offset)?)
        } else {
            None
        };

        if offset + 4 > buf.len() - 4 {
            return Err(Error::BackupError(
                "Truncated backup manifest at segments count".to_string(),
            ));
        }
        let seg_count = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut wal_segments = Vec::with_capacity(seg_count);
        for _ in 0..seg_count {
            wal_segments.push(Self::decode_file_meta(buf, &mut offset)?);
        }

        Ok(BackupManifest {
            manifest_version,
            backup_id,
            created_at_unix_ts,
            checkpoint_lsn,
            min_recovery_lsn,
            backup_start_lsn,
            backup_end_lsn,
            max_segment_bytes,
            max_user_txn_id,
            max_sys_txn_id,
            db_file,
            ckpt_file,
            wal_segments,
        })
    }

    fn encode_file_meta(buf: &mut Vec<u8>, meta: &BackupFileMeta) {
        let name_bytes = meta.file_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&meta.size_bytes.to_le_bytes());
        buf.extend_from_slice(&meta.crc32c.to_le_bytes());
    }

    fn decode_file_meta(buf: &[u8], offset: &mut usize) -> Result<BackupFileMeta> {
        if *offset + 2 > buf.len() - 4 {
            return Err(Error::BackupError(
                "Truncated file meta name length".to_string(),
            ));
        }
        let nlen = u16::from_le_bytes(buf[*offset..*offset + 2].try_into().unwrap()) as usize;
        *offset += 2;
        if *offset + nlen + 8 + 4 > buf.len() - 4 {
            return Err(Error::BackupError(
                "Truncated file meta payload".to_string(),
            ));
        }
        let file_name = std::str::from_utf8(&buf[*offset..*offset + nlen])
            .map_err(|e| Error::BackupError(format!("Invalid file name utf8: {}", e)))?
            .to_string();
        *offset += nlen;
        let size_bytes = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
        *offset += 8;
        let crc32c = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
        *offset += 4;
        Ok(BackupFileMeta {
            file_name,
            size_bytes,
            crc32c,
        })
    }

    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        out.push_str("{\n");
        out.push_str(&format!(
            "  \"manifest_version\": {},\n",
            self.manifest_version
        ));
        out.push_str(&format!("  \"backup_id\": {},\n", self.backup_id));
        out.push_str(&format!(
            "  \"created_at_unix_ts\": {},\n",
            self.created_at_unix_ts
        ));
        out.push_str(&format!("  \"checkpoint_lsn\": {},\n", self.checkpoint_lsn));
        out.push_str(&format!(
            "  \"min_recovery_lsn\": {},\n",
            self.min_recovery_lsn
        ));
        out.push_str(&format!(
            "  \"backup_start_lsn\": {},\n",
            self.backup_start_lsn
        ));
        out.push_str(&format!("  \"backup_end_lsn\": {},\n", self.backup_end_lsn));
        out.push_str(&format!(
            "  \"max_segment_bytes\": {},\n",
            self.max_segment_bytes
        ));
        out.push_str(&format!(
            "  \"max_user_txn_id\": {},\n",
            self.max_user_txn_id
        ));
        out.push_str(&format!("  \"max_sys_txn_id\": {},\n", self.max_sys_txn_id));
        out.push_str(&format!(
            "  \"db_file\": {{\"name\": \"{}\", \"size\": {}, \"crc32c\": {}}},\n",
            self.db_file.file_name, self.db_file.size_bytes, self.db_file.crc32c
        ));
        if let Some(ref ckpt) = self.ckpt_file {
            out.push_str(&format!(
                "  \"ckpt_file\": {{\"name\": \"{}\", \"size\": {}, \"crc32c\": {}}},\n",
                ckpt.file_name, ckpt.size_bytes, ckpt.crc32c
            ));
        } else {
            out.push_str("  \"ckpt_file\": null,\n");
        }
        out.push_str("  \"wal_segments\": [\n");
        for (i, seg) in self.wal_segments.iter().enumerate() {
            let comma = if i + 1 < self.wal_segments.len() {
                ","
            } else {
                ""
            };
            out.push_str(&format!(
                "    {{\"name\": \"{}\", \"size\": {}, \"crc32c\": {}}}{}\n",
                seg.file_name, seg.size_bytes, seg.crc32c, comma
            ));
        }
        out.push_str("  ]\n");
        out.push_str("}\n");
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub dest_dir: PathBuf,
    pub backup_id: u64,
    pub checkpoint_lsn: Lsn,
    pub min_recovery_lsn: Lsn,
    pub redone_records: usize,
    pub undone_records: usize,
    pub committed_txns: Vec<TxnId>,
    pub aborted_txns: Vec<TxnId>,
    pub max_user_txn_id: TxnId,
    pub max_sys_txn_id: TxnId,
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    {
        if let Some(parent) = path.parent() {
            let dir_path = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let dir = File::open(dir_path)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn compute_file_crc<P: AsRef<Path>>(path: P) -> Result<(u64, u32)> {
    let mut file = OpenOptions::new().read(true).open(path.as_ref())?;
    let len = file.metadata()?.len();
    let mut hasher = Crc32cHasher::new();
    let mut buf = [0u8; 64 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let to_read = (buf.len() as u64).min(remaining) as usize;
        file.read_exact(&mut buf[..to_read])?;
        hasher.update(&buf[..to_read]);
        remaining -= to_read as u64;
    }
    Ok((len, hasher.finalize()))
}

struct BackupPinGuard<'a> {
    wal: &'a WalManager,
    pin_id: u64,
}

impl<'a> BackupPinGuard<'a> {
    fn from_pin_id(wal: &'a WalManager, pin_id: u64) -> Self {
        Self { wal, pin_id }
    }
}

impl<'a> Drop for BackupPinGuard<'a> {
    fn drop(&mut self) {
        self.wal.unpin_backup_horizon(self.pin_id);
    }
}

struct BackupTrackingGuard<'a> {
    disk: &'a DiskManager,
}

impl<'a> BackupTrackingGuard<'a> {
    fn new(disk: &'a DiskManager) -> Self {
        disk.begin_backup_tracking();
        Self { disk }
    }
}

impl<'a> Drop for BackupTrackingGuard<'a> {
    fn drop(&mut self) {
        self.disk.end_backup_tracking();
    }
}

fn validate_safe_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(Error::BackupError(format!(
            "Unsafe or invalid file name in backup manifest: '{}'",
            name
        )));
    }
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(_)), None) => Ok(()),
        _ => Err(Error::BackupError(format!(
            "Unsafe path component in backup manifest: '{}'",
            name
        ))),
    }
}

pub struct BackupManager;

impl BackupManager {
    pub fn create_online_backup(
        disk: &Arc<DiskManager>,
        bpm: &Arc<BufferPoolManager>,
        wal: &Arc<WalManager>,
        recovery: &Arc<RecoveryManager>,
        dest_dir: &Path,
    ) -> Result<BackupManifest> {
        if dest_dir.exists() {
            return Err(Error::BackupError(format!(
                "Backup destination already exists at {}",
                dest_dir.display()
            )));
        }

        if let Some(parent) = dest_dir.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let unique_id = format!(
            "{}_{}_{}",
            std::process::id(),
            NEXT_BACKUP_COUNTER.fetch_add(1, Ordering::SeqCst),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let staging_dir = dest_dir.with_extension(format!("tmp.{}", unique_id));
        if staging_dir.exists() {
            let _ = std::fs::remove_dir_all(&staging_dir);
        }
        std::fs::create_dir_all(&staging_dir)?;

        let backup_result = (|| -> Result<BackupManifest> {
            let _tracking_guard = BackupTrackingGuard::new(disk);
            let (ckpt_meta, pin_id) = recovery.create_checkpoint_with_backup_pin()?;
            let _pin_guard = BackupPinGuard::from_pin_id(wal, pin_id);
            let backup_start_lsn = ckpt_meta.checkpoint_lsn;

            let db_file_name = disk
                .path()
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("test.db")
                .to_string();
            let staging_db_path = staging_dir.join(&db_file_name);
            let (initial_db_size, _) = disk.copy_data_file(&staging_db_path)?;
            let initial_pages = (initial_db_size / (PAGE_SIZE as u64)) as u32;

            let (
                backup_end_lsn,
                max_user_txn_id,
                max_sys_txn_id,
                max_segment_bytes,
                copied_segments,
                (db_size, db_crc),
            ) = {
                let _cutoff_guard = bpm.acquire_op_exclusive();
                bpm.flush_all_and_run_cutoff(|| {
                    wal.flush_all_and_capture_cutoff(&staging_dir, |cutoff_end_lsn| {
                        disk.sync_backup_delta_pages(
                            initial_pages,
                            &staging_db_path,
                            cutoff_end_lsn,
                        )
                    })
                })?
            };

            let mut wal_segment_metas = Vec::with_capacity(copied_segments.len());
            for (seg_fname, seg_size, seg_crc) in copied_segments {
                wal_segment_metas.push(BackupFileMeta {
                    file_name: seg_fname,
                    size_bytes: seg_size,
                    crc32c: seg_crc,
                });
            }

            let wal_base_name = wal
                .path()
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("test.wal");
            let ckpt_fname = format!("{}.ckpt", wal_base_name);
            let staging_ckpt_path = staging_dir.join(&ckpt_fname);
            let ckpt_bytes = ckpt_meta.serialize();
            let ckpt_crc = crc32c::checksum(&ckpt_bytes);
            {
                let mut f_ckpt = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&staging_ckpt_path)?;
                f_ckpt.write_all(&ckpt_bytes)?;
                f_ckpt.sync_all()?;
            }
            let ckpt_file_meta = Some(BackupFileMeta {
                file_name: ckpt_fname,
                size_bytes: ckpt_bytes.len() as u64,
                crc32c: ckpt_crc,
            });

            drop(_pin_guard);
            drop(_tracking_guard);

            let backup_id = NEXT_BACKUP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let created_at_unix_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let manifest = BackupManifest {
                manifest_version: BACKUP_MANIFEST_VERSION,
                backup_id,
                created_at_unix_ts,
                checkpoint_lsn: ckpt_meta.checkpoint_lsn,
                min_recovery_lsn: ckpt_meta.min_recovery_lsn,
                backup_start_lsn,
                backup_end_lsn,
                max_segment_bytes,
                max_user_txn_id: max_user_txn_id.max(ckpt_meta.max_user_txn_id),
                max_sys_txn_id: max_sys_txn_id.max(ckpt_meta.max_sys_txn_id),
                db_file: BackupFileMeta {
                    file_name: db_file_name,
                    size_bytes: db_size,
                    crc32c: db_crc,
                },
                ckpt_file: ckpt_file_meta,
                wal_segments: wal_segment_metas,
            };

            let manifest_bin = manifest.serialize();
            let manifest_bin_path = staging_dir.join("backup.manifest");
            {
                let mut f_bin = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&manifest_bin_path)?;
                f_bin.write_all(&manifest_bin)?;
                f_bin.sync_all()?;
            }

            let manifest_json = manifest.to_json_string();
            let manifest_json_path = staging_dir.join("backup_manifest.json");
            {
                let mut f_json = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&manifest_json_path)?;
                f_json.write_all(manifest_json.as_bytes())?;
                f_json.sync_all()?;
            }

            sync_parent_dir(&manifest_bin_path)?;
            std::fs::rename(&staging_dir, dest_dir)?;
            sync_parent_dir(dest_dir)?;
            GLOBAL_METRICS.record_backup();

            Ok(manifest)
        })();

        if backup_result.is_err() {
            let _ = std::fs::remove_dir_all(&staging_dir);
        }

        backup_result
    }

    pub fn create_quiescent_backup(
        db_path: &Path,
        wal_path: &Path,
        dest_dir: &Path,
    ) -> Result<BackupManifest> {
        let disk = Arc::new(DiskManager::open_or_create(db_path)?);
        let wal = Arc::new(WalManager::open_or_create(wal_path)?);
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
        let recovery = Arc::new(RecoveryManager::new(wal.clone(), bpm.clone()));
        let report = recovery.recover()?;
        let _ = report;
        Self::create_online_backup(&disk, &bpm, &wal, &recovery, dest_dir)
    }

    pub fn validate_backup(backup_dir: &Path) -> Result<BackupManifest> {
        if !backup_dir.is_dir() {
            return Err(Error::BackupError(format!(
                "Backup path is not a directory: {}",
                backup_dir.display()
            )));
        }

        let manifest_path = backup_dir.join("backup.manifest");
        if !manifest_path.exists() {
            return Err(Error::BackupError(format!(
                "Missing backup manifest at {}",
                manifest_path.display()
            )));
        }

        let manifest_bytes = std::fs::read(&manifest_path)?;
        let manifest = BackupManifest::deserialize(&manifest_bytes)?;

        if manifest.min_recovery_lsn > manifest.checkpoint_lsn {
            return Err(Error::BackupError(format!(
                "Invalid backup manifest LSN horizon: min_recovery_lsn {} > checkpoint_lsn {}",
                manifest.min_recovery_lsn, manifest.checkpoint_lsn
            )));
        }
        if manifest.backup_start_lsn > manifest.backup_end_lsn {
            return Err(Error::BackupError(format!(
                "Invalid backup manifest LSN range: backup_start_lsn {} > backup_end_lsn {}",
                manifest.backup_start_lsn, manifest.backup_end_lsn
            )));
        }
        if manifest.checkpoint_lsn > manifest.backup_end_lsn {
            return Err(Error::BackupError(format!(
                "Invalid backup manifest checkpoint LSN: checkpoint_lsn {} > backup_end_lsn {}",
                manifest.checkpoint_lsn, manifest.backup_end_lsn
            )));
        }

        validate_safe_file_name(&manifest.db_file.file_name)?;
        if manifest.db_file.size_bytes % (PAGE_SIZE as u64) != 0 {
            return Err(Error::BackupError(format!(
                "Manifest database file size {} is not a multiple of page size {}",
                manifest.db_file.size_bytes, PAGE_SIZE
            )));
        }

        let db_path = backup_dir.join(&manifest.db_file.file_name);
        if !db_path.exists() {
            return Err(Error::BackupError(format!(
                "Missing database file '{}' in backup",
                manifest.db_file.file_name
            )));
        }
        let (db_len, db_crc) = compute_file_crc(&db_path)?;
        if db_len != manifest.db_file.size_bytes {
            return Err(Error::BackupError(format!(
                "Database file length mismatch: expected {}, got {}",
                manifest.db_file.size_bytes, db_len
            )));
        }
        if db_crc != manifest.db_file.crc32c {
            return Err(Error::BackupError(format!(
                "Database file CRC mismatch: expected {:#x}, got {:#x}",
                manifest.db_file.crc32c, db_crc
            )));
        }
        if db_len % (PAGE_SIZE as u64) != 0 {
            return Err(Error::BackupError(format!(
                "Database file size {} is not a multiple of page size {}",
                db_len, PAGE_SIZE
            )));
        }
        let num_pages = (db_len / (PAGE_SIZE as u64)) as u32;
        if num_pages > 0 {
            let mut db_file = OpenOptions::new().read(true).open(&db_path)?;
            let mut page_buf = [0u8; PAGE_SIZE];
            for pid in 0..num_pages {
                db_file.read_exact(&mut page_buf)?;
                let page = Page::from_bytes(page_buf).map_err(|e| {
                    Error::BackupError(format!("Database page {} corrupted in backup: {}", pid, e))
                })?;
                if page.page_id() != pid {
                    return Err(Error::BackupError(format!(
                        "Database page ID mismatch in backup: expected {}, got {}",
                        pid,
                        page.page_id()
                    )));
                }
                if page.page_lsn() > manifest.backup_end_lsn {
                    return Err(Error::BackupError(format!(
                        "Database page {} has page_lsn {} exceeding manifest backup_end_lsn {}",
                        pid,
                        page.page_lsn(),
                        manifest.backup_end_lsn
                    )));
                }
            }
        }

        if let Some(ref ckpt_meta) = manifest.ckpt_file {
            validate_safe_file_name(&ckpt_meta.file_name)?;
            let ckpt_path = backup_dir.join(&ckpt_meta.file_name);
            if !ckpt_path.exists() {
                return Err(Error::BackupError(format!(
                    "Missing checkpoint metadata file '{}' in backup",
                    ckpt_meta.file_name
                )));
            }
            let (ckpt_len, ckpt_crc) = compute_file_crc(&ckpt_path)?;
            if ckpt_len != ckpt_meta.size_bytes {
                return Err(Error::BackupError(format!(
                    "Checkpoint file length mismatch: expected {}, got {}",
                    ckpt_meta.size_bytes, ckpt_len
                )));
            }
            if ckpt_crc != ckpt_meta.crc32c {
                return Err(Error::BackupError(format!(
                    "Checkpoint file CRC mismatch: expected {:#x}, got {:#x}",
                    ckpt_meta.crc32c, ckpt_crc
                )));
            }
            let ckpt_bytes = std::fs::read(&ckpt_path)?;
            let decoded_ckpt = CheckpointMetadata::deserialize(&ckpt_bytes)?;
            if decoded_ckpt.checkpoint_lsn != manifest.checkpoint_lsn
                || decoded_ckpt.min_recovery_lsn != manifest.min_recovery_lsn
            {
                return Err(Error::BackupError(format!(
                    "Checkpoint metadata LSN mismatch with manifest: ckpt=({}, {}), manifest=({}, {})",
                    decoded_ckpt.checkpoint_lsn,
                    decoded_ckpt.min_recovery_lsn,
                    manifest.checkpoint_lsn,
                    manifest.min_recovery_lsn
                )));
            }
        }

        if manifest.wal_segments.is_empty() {
            return Err(Error::BackupError(
                "Backup manifest contains no WAL segments".to_string(),
            ));
        }

        let mut first_wal_lsn: Option<Lsn> = None;
        let mut last_wal_lsn: Option<Lsn> = None;
        let mut expected_next_lsn: Option<Lsn> = None;
        let mut saw_checkpoint_record = false;
        let num_segments = manifest.wal_segments.len();

        for (seg_idx, seg) in manifest.wal_segments.iter().enumerate() {
            validate_safe_file_name(&seg.file_name)?;
            let seg_path = backup_dir.join(&seg.file_name);
            if !seg_path.exists() {
                return Err(Error::BackupError(format!(
                    "Missing WAL segment '{}' in backup",
                    seg.file_name
                )));
            }
            let (seg_len, seg_crc) = compute_file_crc(&seg_path)?;
            if seg_len != seg.size_bytes {
                return Err(Error::BackupError(format!(
                    "WAL segment '{}' length mismatch: expected {}, got {}",
                    seg.file_name, seg.size_bytes, seg_len
                )));
            }
            if seg_crc != seg.crc32c {
                return Err(Error::BackupError(format!(
                    "WAL segment '{}' CRC mismatch: expected {:#x}, got {:#x}",
                    seg.file_name, seg.crc32c, seg_crc
                )));
            }

            let is_last_seg = seg_idx + 1 == num_segments;
            if !is_last_seg && seg_len == 0 {
                return Err(Error::BackupError(format!(
                    "Empty non-final WAL segment '{}' in backup",
                    seg.file_name
                )));
            }

            let mut seg_file = File::open(&seg_path)?;
            let mut offset = 0u64;
            while offset < seg_len {
                let remaining = seg_len - offset;
                if remaining < 4 {
                    return Err(Error::BackupError(format!(
                        "Truncated WAL record header in backup segment '{}'",
                        seg.file_name
                    )));
                }
                let mut size_buf = [0u8; 4];
                seg_file.read_exact(&mut size_buf)?;
                let total_size = u32::from_le_bytes(size_buf) as usize;
                if !(51..=131_121).contains(&total_size) || (total_size as u64) > remaining {
                    return Err(Error::BackupError(format!(
                        "Truncated or invalid WAL record size {} in backup segment '{}'",
                        total_size, seg.file_name
                    )));
                }
                let mut rec_buf = vec![0u8; total_size];
                rec_buf[0..4].copy_from_slice(&size_buf);
                seg_file.read_exact(&mut rec_buf[4..])?;
                let record = LogRecord::deserialize(&rec_buf).map_err(|e| {
                    Error::BackupError(format!(
                        "Corrupted WAL record in backup segment '{}': {}",
                        seg.file_name, e
                    ))
                })?;

                if let Some(exp) = expected_next_lsn
                    && record.lsn != exp
                {
                    return Err(Error::BackupError(format!(
                        "WAL LSN discontinuity in backup segment '{}': expected {}, got {}",
                        seg.file_name, exp, record.lsn
                    )));
                }
                expected_next_lsn = record.lsn.checked_add(1);
                if first_wal_lsn.is_none() {
                    first_wal_lsn = Some(record.lsn);
                }
                last_wal_lsn = Some(record.lsn);
                if record.lsn == manifest.checkpoint_lsn
                    && record.record_type == LogRecordType::Checkpoint
                {
                    saw_checkpoint_record = true;
                }
                if record.lsn > manifest.backup_end_lsn {
                    return Err(Error::BackupError(format!(
                        "Backup WAL segment '{}' contains record LSN {} exceeding manifest backup_end_lsn {}",
                        seg.file_name, record.lsn, manifest.backup_end_lsn
                    )));
                }
                offset += total_size as u64;
            }
        }

        if manifest.backup_end_lsn > 0 {
            let Some(first_lsn) = first_wal_lsn else {
                return Err(Error::BackupError(
                    "Backup WAL segments contain no records despite non-zero backup_end_lsn"
                        .to_string(),
                ));
            };
            if first_lsn > manifest.min_recovery_lsn {
                return Err(Error::BackupError(format!(
                    "Backup WAL starts at LSN {} which exceeds manifest min_recovery_lsn {}",
                    first_lsn, manifest.min_recovery_lsn
                )));
            }
            if manifest.checkpoint_lsn > 0 && !saw_checkpoint_record {
                return Err(Error::BackupError(format!(
                    "Checkpoint record at LSN {} missing from backup WAL segments",
                    manifest.checkpoint_lsn
                )));
            }
            if last_wal_lsn != Some(manifest.backup_end_lsn) {
                return Err(Error::BackupError(format!(
                    "Backup WAL final LSN {:?} does not match manifest backup_end_lsn {}",
                    last_wal_lsn, manifest.backup_end_lsn
                )));
            }
        }

        Ok(manifest)
    }

    pub fn restore_backup(backup_dir: &Path, dest_db_dir: &Path) -> Result<RestoreReport> {
        if dest_db_dir.exists() {
            return Err(Error::BackupError(format!(
                "Restore destination directory already exists at {}",
                dest_db_dir.display()
            )));
        }

        let manifest = Self::validate_backup(backup_dir)?;

        if let Some(parent) = dest_db_dir.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let unique_id = format!(
            "{}_{}_{}",
            std::process::id(),
            NEXT_BACKUP_COUNTER.fetch_add(1, Ordering::SeqCst),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let staging_dir = dest_db_dir.with_extension(format!("tmp.{}", unique_id));
        if staging_dir.exists() {
            let _ = std::fs::remove_dir_all(&staging_dir);
        }
        std::fs::create_dir_all(&staging_dir)?;

        let restore_result = (|| -> Result<RestoreReport> {
            let src_db_path = backup_dir.join(&manifest.db_file.file_name);
            let staging_db_path = staging_dir.join(&manifest.db_file.file_name);
            std::fs::copy(&src_db_path, &staging_db_path)?;

            if let Some(ref ckpt_meta) = manifest.ckpt_file {
                let src_ckpt = backup_dir.join(&ckpt_meta.file_name);
                let staging_ckpt = staging_dir.join(&ckpt_meta.file_name);
                std::fs::copy(&src_ckpt, &staging_ckpt)?;
            }

            for seg in &manifest.wal_segments {
                let src_seg = backup_dir.join(&seg.file_name);
                let staging_seg = staging_dir.join(&seg.file_name);
                std::fs::copy(&src_seg, &staging_seg)?;
            }
            sync_parent_dir(&staging_db_path)?;

            let wal_base_name = manifest
                .ckpt_file
                .as_ref()
                .and_then(|c| c.file_name.strip_suffix(".ckpt"))
                .or_else(|| {
                    manifest.wal_segments.iter().find_map(|s| {
                        if !s.file_name.contains('.') {
                            Some(s.file_name.as_str())
                        } else {
                            s.file_name.split('.').next()
                        }
                    })
                })
                .unwrap_or("test.wal");
            let staging_wal_path = staging_dir.join(wal_base_name);

            let disk = Arc::new(DiskManager::open_or_create(&staging_db_path)?);
            let wal = Arc::new(WalManager::open_with_segment_size(
                &staging_wal_path,
                manifest.max_segment_bytes,
            )?);
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover()?;
            let _ = recovery.create_checkpoint_with_meta()?;
            bpm.rebuild_freelist_from_disk()?;
            disk.sync()?;
            wal.flush_all()?;

            let final_max_user = wal.max_user_txn_id();
            let final_max_sys = wal.max_sys_txn_id();

            drop(recovery);
            drop(bpm);
            drop(wal);
            drop(disk);

            std::fs::rename(&staging_dir, dest_db_dir)?;
            sync_parent_dir(dest_db_dir)?;
            GLOBAL_METRICS.record_restore();

            Ok(RestoreReport {
                dest_dir: dest_db_dir.to_path_buf(),
                backup_id: manifest.backup_id,
                checkpoint_lsn: manifest.checkpoint_lsn,
                min_recovery_lsn: manifest.min_recovery_lsn,
                redone_records: report.redone_records,
                undone_records: report.undone_records,
                committed_txns: report.committed_txns,
                aborted_txns: report.aborted_txns,
                max_user_txn_id: final_max_user,
                max_sys_txn_id: final_max_sys,
            })
        })();

        if restore_result.is_err() {
            let _ = std::fs::remove_dir_all(&staging_dir);
        }

        restore_result
    }
}
