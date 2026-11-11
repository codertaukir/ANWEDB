use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "fault-injection")]
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::os::unix::io::AsRawFd;

use mitrdb_common::{Error, Lsn, MAGIC_BYTES, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId};
use mitrdb_observe::GLOBAL_METRICS;

use crate::crc32c::Crc32cHasher;
use crate::page::{PAGE_FLAG_FREE, PAGE_TYPE_RAW, Page};

pub struct DiskManager {
    op_gate: RwLock<()>,
    file: Mutex<File>,
    path: PathBuf,
    num_pages: Mutex<u32>,
    free_pages: Mutex<Vec<PageId>>,
    pending_free_pages: Mutex<HashMap<TxnId, Vec<PageId>>>,
    backup_dirty_pages: Mutex<Option<BTreeSet<PageId>>>,
    ring: Mutex<Option<io_uring::IoUring>>,
    #[cfg(feature = "fault-injection")]
    fail_alloc_after: AtomicI64,
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

impl DiskManager {
    fn freelist_path(db_path: &Path) -> PathBuf {
        let mut f = db_path.as_os_str().to_os_string();
        f.push(".free");
        PathBuf::from(f)
    }

    fn load_freelist(freelist_path: &Path, num_pages: u32) -> Vec<PageId> {
        let Ok(mut file) = File::open(freelist_path) else {
            return Vec::new();
        };
        let mut buf = Vec::new();
        if file.read_to_end(&mut buf).is_err() || buf.len() < 16 {
            return Vec::new();
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC_BYTES {
            return Vec::new();
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != 1 {
            return Vec::new();
        }
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        let expected_len = 12 + count * 4 + 4;
        if buf.len() != expected_len {
            return Vec::new();
        }
        let stored_crc =
            u32::from_le_bytes(buf[expected_len - 4..expected_len].try_into().unwrap());
        let computed_crc = crate::crc32c::checksum(&buf[0..expected_len - 4]);
        if stored_crc != computed_crc {
            return Vec::new();
        }
        let mut pages = Vec::with_capacity(count);
        let mut seen = std::collections::HashSet::new();
        for i in 0..count {
            let offset = 12 + i * 4;
            let pid = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
            if pid > 0 && pid < num_pages && seen.insert(pid) {
                pages.push(pid);
            }
        }
        pages
    }

    fn save_freelist_static(freelist_path: &Path, free_pages: &[PageId]) -> Result<()> {
        let count = free_pages.len() as u32;
        let mut buf = Vec::with_capacity(16 + free_pages.len() * 4);
        buf.extend_from_slice(&MAGIC_BYTES.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        for &pid in free_pages {
            buf.extend_from_slice(&pid.to_le_bytes());
        }
        let crc = crate::crc32c::checksum(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let tmp_path = freelist_path.with_extension("tmp");
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, freelist_path)?;
        sync_parent_dir(freelist_path)?;
        Ok(())
    }

    fn save_freelist_locked(&self, free_pages: &[PageId]) -> Result<()> {
        let freelist_path = Self::freelist_path(&self.path);
        Self::save_freelist_static(&freelist_path, free_pages)
    }

    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        if let Some(parent) = path_buf.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path_buf)?;
        sync_parent_dir(&path_buf)?;

        let file_len = file.metadata()?.len();
        if file_len % (PAGE_SIZE as u64) != 0 {
            return Err(Error::PageCorrupted(format!(
                "Database file length {} is not a multiple of page size {}",
                file_len, PAGE_SIZE
            )));
        }
        let num_pages = (file_len / (PAGE_SIZE as u64)) as u32;
        let freelist_path = Self::freelist_path(&path_buf);
        let mut free_pages = Self::load_freelist(&freelist_path, num_pages);

        if free_pages.is_empty() && num_pages > 1 {
            for pid in 1..num_pages {
                let offset = (pid as u64) * (PAGE_SIZE as u64);
                if file.seek(SeekFrom::Start(offset)).is_ok() {
                    let mut hdr = [0u8; 8];
                    if file.read_exact(&mut hdr).is_ok() {
                        let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
                        let ptype = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
                        let flags = u16::from_le_bytes(hdr[6..8].try_into().unwrap());
                        let valid_ptype = ptype == PAGE_TYPE_RAW
                            || ptype == crate::page::PAGE_TYPE_SLOTTED
                            || ptype == crate::page::PAGE_TYPE_BTREE_LEAF
                            || ptype == crate::page::PAGE_TYPE_BTREE_INTERNAL;
                        if magic == MAGIC_BYTES && valid_ptype && (flags & PAGE_FLAG_FREE) != 0 {
                            free_pages.push(pid);
                        }
                    }
                }
            }
            if !free_pages.is_empty() {
                let _ = Self::save_freelist_static(&freelist_path, &free_pages);
            }
        }
        let _ = file.seek(SeekFrom::Start(0));
        Ok(DiskManager {
            op_gate: RwLock::new(()),
            file: Mutex::new(file),
            path: path_buf,
            num_pages: Mutex::new(num_pages),
            free_pages: Mutex::new(free_pages),
            pending_free_pages: Mutex::new(HashMap::new()),
            backup_dirty_pages: Mutex::new(None),
            ring: Mutex::new(io_uring::IoUring::new(64).ok()),
            #[cfg(feature = "fault-injection")]
            fail_alloc_after: AtomicI64::new(-1),
        })
    }

    pub fn acquire_op_shared(&self) -> RwLockReadGuard<'_, ()> {
        self.op_gate.read().unwrap()
    }

    pub fn acquire_op_exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.op_gate.write().unwrap()
    }

    pub fn begin_backup_tracking(&self) {
        let mut tracker = self.backup_dirty_pages.lock().unwrap();
        *tracker = Some(BTreeSet::new());
    }

    pub fn end_backup_tracking(&self) {
        let mut tracker = self.backup_dirty_pages.lock().unwrap();
        *tracker = None;
    }

    fn record_backup_dirty_page(&self, page_id: PageId) {
        let mut tracker = self.backup_dirty_pages.lock().unwrap();
        if let Some(ref mut set) = *tracker {
            set.insert(page_id);
        }
    }

    /// Test hook: allow `n` more allocations, then fail one (-1 = off).
    #[cfg(feature = "fault-injection")]
    pub fn set_fail_alloc_after(&self, n: i64) {
        self.fail_alloc_after.store(n, AtomicOrdering::Relaxed);
    }

    pub fn num_pages(&self) -> u32 {
        *self.num_pages.lock().unwrap()
    }

    pub fn free_page_count(&self) -> usize {
        self.free_pages.lock().unwrap().len()
    }

    pub fn is_page_free(&self, page_id: PageId) -> bool {
        self.free_pages.lock().unwrap().contains(&page_id)
    }

    pub fn stage_free_page(&self, txn_id: TxnId, page_id: PageId) {
        if page_id == 0 {
            return;
        }
        let mut pending = self.pending_free_pages.lock().unwrap();
        let list = pending.entry(txn_id).or_default();
        if !list.contains(&page_id) {
            list.push(page_id);
        }
    }

    pub fn take_pending_free_pages(&self, txn_id: TxnId) -> Vec<PageId> {
        self.pending_free_pages
            .lock()
            .unwrap()
            .remove(&txn_id)
            .unwrap_or_default()
    }

    pub fn pending_free_txn_ids(&self) -> Vec<TxnId> {
        let pending = self.pending_free_pages.lock().unwrap();
        let mut tids: Vec<TxnId> = pending
            .iter()
            .filter(|(_, list)| !list.is_empty())
            .map(|(&tid, _)| tid)
            .collect();
        tids.sort_unstable();
        tids
    }

    pub fn unmark_page_free(&self, page_id: PageId) {
        let mut free_pages = self.free_pages.lock().unwrap();
        if let Some(pos) = free_pages.iter().position(|&pid| pid == page_id) {
            free_pages.remove(pos);
            let _ = self.save_freelist_locked(&free_pages);
        }
    }

    pub fn rebuild_freelist_from_disk(&self) -> Result<Vec<PageId>> {
        self.rebuild_freelist_from_disk_excluding(&std::collections::HashSet::new())
    }

    pub fn rebuild_freelist_from_disk_excluding(
        &self,
        pinned_pages: &std::collections::HashSet<PageId>,
    ) -> Result<Vec<PageId>> {
        let num_pages = *self.num_pages.lock().unwrap();
        let mut free_pages = self.free_pages.lock().unwrap();
        let mut pending = self.pending_free_pages.lock().unwrap();
        let mut file = self.file.lock().unwrap();
        let mut rebuilt = Vec::new();
        let mut normalized_any = false;
        for pid in 1..num_pages {
            let offset = (pid as u64) * (PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            let mut hdr = [0u8; 8];
            file.read_exact(&mut hdr)?;
            let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let ptype = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
            let flags = u16::from_le_bytes(hdr[6..8].try_into().unwrap());
            let valid_ptype = ptype == PAGE_TYPE_RAW
                || ptype == crate::page::PAGE_TYPE_SLOTTED
                || ptype == crate::page::PAGE_TYPE_BTREE_LEAF
                || ptype == crate::page::PAGE_TYPE_BTREE_INTERNAL;
            if magic == MAGIC_BYTES && valid_ptype && (flags & PAGE_FLAG_FREE) != 0 {
                let pending_for_active_txn = pending
                    .iter()
                    .any(|(&tid, list)| tid != 0 && list.contains(&pid));
                if pinned_pages.contains(&pid) || pending_for_active_txn {
                    let already_pending = pending.values().any(|list| list.contains(&pid));
                    if !already_pending {
                        pending.entry(0).or_default().push(pid);
                    }
                } else {
                    if ptype != PAGE_TYPE_RAW {
                        let mut blank_page = Page::new_empty(pid, PAGE_TYPE_RAW);
                        blank_page.set_flags(PAGE_FLAG_FREE);
                        blank_page.data[PAGE_HEADER_SIZE] = 0xFF;
                        blank_page.update_checksum();
                        file.seek(SeekFrom::Start(offset))?;
                        file.write_all(blank_page.as_bytes())?;
                        self.record_backup_dirty_page(pid);
                        normalized_any = true;
                    }
                    rebuilt.push(pid);
                }
            }
        }
        if normalized_any {
            file.sync_all()?;
        }
        drop(file);
        for list in pending.values_mut() {
            list.retain(|pid| !rebuilt.contains(pid));
        }
        pending.retain(|_, list| !list.is_empty());
        *free_pages = rebuilt.clone();
        let _ = self.save_freelist_locked(&free_pages);
        Ok(rebuilt)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let num_pages = self.num_pages.lock().unwrap();
        let free_pages = self.free_pages.lock().unwrap();
        if page_id >= *num_pages || free_pages.contains(&page_id) {
            return Err(Error::InvalidPageId(page_id));
        }
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock().unwrap();
        drop(free_pages);
        drop(num_pages);
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = [0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;
        GLOBAL_METRICS.record_read();
        let page = Page::from_bytes(buffer)?;
        if page.page_id() != page_id {
            return Err(Error::PageCorrupted(format!(
                "Page ID mismatch on disk: expected {}, got {}",
                page_id,
                page.page_id()
            )));
        }
        Ok(page)
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        let num_pages = self.num_pages.lock().unwrap();
        if page_id >= *num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        if page.page_id() != page_id {
            return Err(Error::PageCorrupted(format!(
                "Cannot write page with header page_id {} to slot {}",
                page.page_id(),
                page_id
            )));
        }
        page.validate_structure()?;
        let mut page_copy = page.clone();
        page_copy.update_checksum();
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock().unwrap();
        drop(num_pages);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page_copy.as_bytes())?;
        self.record_backup_dirty_page(page_id);
        GLOBAL_METRICS.record_write();
        Ok(())
    }

    pub fn write_pages_batch(&self, batch: &[(PageId, Page)]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let num_pages = self.num_pages.lock().unwrap();
        for (page_id, page) in batch {
            if *page_id >= *num_pages {
                return Err(Error::InvalidPageId(*page_id));
            }
            if page.page_id() != *page_id {
                return Err(Error::PageCorrupted(format!(
                    "Cannot write page with header page_id {} to slot {}",
                    page.page_id(),
                    page_id
                )));
            }
            page.validate_structure()?;
        }
        drop(num_pages);

        // Har page ki copy banake checksum calculate karo
        let mut prepared: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch.len());
        for (page_id, page) in batch {
            let mut page_copy = page.clone();
            page_copy.update_checksum();
            let offset = (*page_id as u64) * (PAGE_SIZE as u64);
            prepared.push((offset, page_copy.as_bytes().to_vec()));
        }

        let mut file_guard = self.file.lock().unwrap();

        let mut io_done = false;

        {
            let mut ring_guard = self.ring.lock().unwrap();
            let mut ring_failed = false;
            if let Some(ref mut ring) = *ring_guard {
                use io_uring::{opcode, types};
                let fd = types::Fd(file_guard.as_raw_fd());
                let mut all_ok = true;

                // Chunk batch if larger than queue capacity. Every chunk must fully
                // succeed (no error, no short write); otherwise the whole batch is
                // rewritten through the plain write_all fallback below.
                for chunk in prepared.chunks(64) {
                    let mut submitted = 0usize;
                    unsafe {
                        let mut sq = ring.submission();
                        for (offset, data) in chunk {
                            let write_op = opcode::Write::new(fd, data.as_ptr(), data.len() as u32)
                                .offset(*offset)
                                .build();
                            if sq.push(&write_op).is_err() {
                                break;
                            }
                            submitted += 1;
                        }
                    }
                    if submitted != chunk.len() {
                        all_ok = false;
                    }
                    if submitted > 0 {
                        if ring.submit_and_wait(submitted).is_err() {
                            all_ok = false;
                            ring_failed = true;
                            break;
                        }
                        for cqe in ring.completion() {
                            if cqe.result() != PAGE_SIZE as i32 {
                                all_ok = false;
                            }
                        }
                    }
                    if !all_ok {
                        break;
                    }
                }
                io_done = all_ok;
            }
            if ring_failed || !io_done {
                // Do not reuse a ring that may hold unreaped completions.
                *ring_guard = None;
            }
        }

        let needs_fallback = !io_done;

        if needs_fallback {
            for (offset, data) in &prepared {
                file_guard.seek(SeekFrom::Start(*offset))?;
                file_guard.write_all(data)?;
            }
        }

        for (page_id, _) in batch {
            self.record_backup_dirty_page(*page_id);
            GLOBAL_METRICS.record_write();
        }

        Ok(())
    }

    pub fn allocate_page(&self, page_type: u16) -> Result<(PageId, Page)> {
        self.allocate_page_with_filter(page_type, |_| true)
    }

    pub fn allocate_page_with_filter<F>(
        &self,
        page_type: u16,
        can_reuse: F,
    ) -> Result<(PageId, Page)>
    where
        F: Fn(PageId) -> bool,
    {
        #[cfg(feature = "fault-injection")]
        {
            let inj = self.fail_alloc_after.load(AtomicOrdering::Relaxed);
            if inj >= 0 {
                if inj == 0 {
                    self.fail_alloc_after.store(-1, AtomicOrdering::Relaxed);
                    return Err(Error::Io("injected allocation failure".to_string()));
                }
                self.fail_alloc_after
                    .store(inj - 1, AtomicOrdering::Relaxed);
            }
        }
        let mut num_pages = self.num_pages.lock().unwrap();
        let mut free_pages = self.free_pages.lock().unwrap();
        let pending = self.pending_free_pages.lock().unwrap();
        free_pages.retain(|&pid| pid > 0 && pid < *num_pages);
        if let Some(pos) = free_pages
            .iter()
            .rposition(|pid| !pending.values().any(|list| list.contains(pid)) && can_reuse(*pid))
        {
            let reused_id = free_pages.remove(pos);
            drop(pending);
            drop(num_pages);
            let page = Page::new_empty(reused_id, page_type);
            if let Err(e) = page.validate_structure() {
                free_pages.push(reused_id);
                return Err(e);
            }
            let offset = (reused_id as u64) * (PAGE_SIZE as u64);
            let mut file = self.file.lock().unwrap();
            if let Err(e) = file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| file.write_all(page.as_bytes()))
            {
                drop(file);
                free_pages.push(reused_id);
                return Err(e.into());
            }
            self.record_backup_dirty_page(reused_id);
            drop(file);
            let _ = self.save_freelist_locked(&free_pages);
            GLOBAL_METRICS.record_write();
            return Ok((reused_id, page));
        }
        drop(pending);
        drop(free_pages);
        let new_id = *num_pages;
        if !can_reuse(new_id) {
            return Err(Error::Io(format!(
                "Cannot allocate page {} while resident in buffer pool",
                new_id
            )));
        }
        let page = Page::new_empty(new_id, page_type);
        page.validate_structure()?;
        let offset = (new_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock().unwrap();
        if let Err(e) = file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| file.write_all(page.as_bytes()))
        {
            let _ = file.set_len(offset);
            return Err(e.into());
        }
        *num_pages = new_id + 1;
        self.record_backup_dirty_page(new_id);
        GLOBAL_METRICS.record_write();
        Ok((new_id, page))
    }

    pub fn free_page(&self, page_id: PageId) -> Result<()> {
        if page_id == 0 {
            return Err(Error::TransactionError(
                "Cannot free reserved page 0".to_string(),
            ));
        }
        let num_pages = self.num_pages.lock().unwrap();
        if page_id >= *num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        let mut free_pages = self.free_pages.lock().unwrap();
        drop(num_pages);
        if free_pages.contains(&page_id) {
            return Ok(());
        }
        let mut blank_page = Page::new_empty(page_id, PAGE_TYPE_RAW);
        blank_page.set_flags(PAGE_FLAG_FREE);
        blank_page.data[PAGE_HEADER_SIZE] = 0xFF;
        blank_page.update_checksum();
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(blank_page.as_bytes())?;
        file.sync_all()?;
        self.record_backup_dirty_page(page_id);
        drop(file);

        free_pages.push(page_id);
        let _ = self.save_freelist_locked(&free_pages);
        GLOBAL_METRICS.record_write();
        Ok(())
    }

    /// Free many pages with one data-file fsync and one freelist save.
    /// Same ordering as `free_page` (blank pages durable, then freelist), but per call
    /// instead of per page. On error nothing is added to the in-memory freelist.
    pub fn free_pages_batch(&self, page_ids: &[PageId]) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }
        let num_pages = self.num_pages.lock().unwrap();
        let mut free_pages = self.free_pages.lock().unwrap();
        let np = *num_pages;
        drop(num_pages);

        let mut seen = std::collections::HashSet::new();
        let mut todo: Vec<PageId> = Vec::with_capacity(page_ids.len());
        for &pid in page_ids {
            if pid == 0 {
                return Err(Error::TransactionError(
                    "Cannot free reserved page 0".to_string(),
                ));
            }
            if pid >= np {
                return Err(Error::InvalidPageId(pid));
            }
            if free_pages.contains(&pid) || !seen.insert(pid) {
                continue;
            }
            todo.push(pid);
        }
        if todo.is_empty() {
            return Ok(());
        }

        {
            let mut file = self.file.lock().unwrap();
            for &pid in &todo {
                let mut blank_page = Page::new_empty(pid, PAGE_TYPE_RAW);
                blank_page.set_flags(PAGE_FLAG_FREE);
                blank_page.data[PAGE_HEADER_SIZE] = 0xFF;
                blank_page.update_checksum();
                let offset = (pid as u64) * (PAGE_SIZE as u64);
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(blank_page.as_bytes())?;
            }
            file.sync_all()?;
            for &pid in &todo {
                self.record_backup_dirty_page(pid);
            }
        }

        free_pages.extend_from_slice(&todo);
        let _ = self.save_freelist_locked(&free_pages);
        for _ in &todo {
            GLOBAL_METRICS.record_write();
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_all()?;
        Ok(())
    }

    pub fn copy_data_file<P: AsRef<Path>>(&self, dest_path: P) -> Result<(u64, u32)> {
        let path_ref = dest_path.as_ref();
        if let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path_ref)?;

        let num_pages = *self.num_pages.lock().unwrap();
        {
            let mut file = self.file.lock().unwrap();
            file.flush()?;
            file.sync_all()?;
        }

        let mut hasher = Crc32cHasher::new();
        let mut page_buf = [0u8; PAGE_SIZE];
        let mut total_copied = 0u64;
        for pid in 0..num_pages {
            let offset = (pid as u64) * (PAGE_SIZE as u64);
            {
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut page_buf)?;
            }
            dest_file.write_all(&page_buf)?;
            hasher.update(&page_buf);
            total_copied += PAGE_SIZE as u64;
        }
        dest_file.sync_all()?;
        {
            let _free_guard = self.free_pages.lock().unwrap();
            let src_freelist = Self::freelist_path(&self.path);
            if src_freelist.exists() {
                let dest_freelist = Self::freelist_path(path_ref);
                let _ = std::fs::copy(&src_freelist, &dest_freelist);
            }
        }
        sync_parent_dir(path_ref)?;
        Ok((total_copied, hasher.finalize()))
    }

    pub fn append_pages_from<P: AsRef<Path>>(&self, start_page: u32, dest_path: P) -> Result<u32> {
        let num_pages = *self.num_pages.lock().unwrap();
        if start_page >= num_pages {
            return Ok(0);
        }
        let pages_to_copy = num_pages - start_page;

        let mut dest_file = OpenOptions::new().append(true).open(dest_path.as_ref())?;
        {
            let mut file = self.file.lock().unwrap();
            file.flush()?;
            file.sync_all()?;
        }

        let mut page_buf = [0u8; PAGE_SIZE];
        for pid in start_page..num_pages {
            let offset = (pid as u64) * (PAGE_SIZE as u64);
            {
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut page_buf)?;
            }
            dest_file.write_all(&page_buf)?;
        }
        dest_file.sync_all()?;
        sync_parent_dir(dest_path.as_ref())?;
        Ok(pages_to_copy)
    }

    pub fn sync_backup_delta_pages<P: AsRef<Path>>(
        &self,
        initial_pages: u32,
        dest_path: P,
        max_allowed_lsn: Lsn,
    ) -> Result<(u64, u32)> {
        let path_ref = dest_path.as_ref();
        let num_pages_guard = self.num_pages.lock().unwrap();
        let _free_guard = self.free_pages.lock().unwrap();
        let num_pages = *num_pages_guard;
        let mut file = self.file.lock().unwrap();
        let dirty_pages = self
            .backup_dirty_pages
            .lock()
            .unwrap()
            .take()
            .unwrap_or_default();

        file.flush()?;
        file.sync_all()?;

        let mut dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_ref)?;

        let mut page_buf = [0u8; PAGE_SIZE];
        for &pid in &dirty_pages {
            if pid < initial_pages && pid < num_pages {
                let offset = (pid as u64) * (PAGE_SIZE as u64);
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut page_buf)?;
                dest_file.seek(SeekFrom::Start(offset))?;
                dest_file.write_all(&page_buf)?;
            }
        }

        if num_pages > initial_pages {
            for pid in initial_pages..num_pages {
                let offset = (pid as u64) * (PAGE_SIZE as u64);
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut page_buf)?;
                dest_file.seek(SeekFrom::Start(offset))?;
                dest_file.write_all(&page_buf)?;
            }
        }

        let total_size = (num_pages as u64) * (PAGE_SIZE as u64);
        dest_file.set_len(total_size)?;
        dest_file.flush()?;
        dest_file.sync_all()?;

        let src_freelist = Self::freelist_path(&self.path);
        let dest_freelist = Self::freelist_path(path_ref);
        if src_freelist.exists() {
            let _ = std::fs::copy(&src_freelist, &dest_freelist);
        } else if dest_freelist.exists() {
            let _ = std::fs::remove_file(&dest_freelist);
        }

        dest_file.seek(SeekFrom::Start(0))?;
        let mut hasher = Crc32cHasher::new();
        for pid in 0..num_pages {
            dest_file.read_exact(&mut page_buf)?;
            let page = Page::from_bytes(page_buf).map_err(|e| {
                Error::BackupError(format!(
                    "Copied backup page {} failed structural/checksum validation: {}",
                    pid, e
                ))
            })?;
            if page.page_id() != pid {
                return Err(Error::BackupError(format!(
                    "Copied backup page ID mismatch: expected {}, got {}",
                    pid,
                    page.page_id()
                )));
            }
            if page.page_lsn() > max_allowed_lsn {
                return Err(Error::BackupError(format!(
                    "Copied backup page {} has page_lsn {} exceeding backup_end_lsn {}",
                    pid,
                    page.page_lsn(),
                    max_allowed_lsn
                )));
            }
            hasher.update(&page_buf);
        }

        sync_parent_dir(path_ref)?;
        Ok((total_size, hasher.finalize()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_TYPE_SLOTTED;

    #[test]
    fn test_disk_manager_create_and_read_write() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_test_disk_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        assert_eq!(disk.num_pages(), 0);

        let (page_id, mut page) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(disk.num_pages(), 1);

        let slot = page.insert_record(b"persistent record").unwrap();
        disk.write_page(page_id, &page).unwrap();
        disk.sync().unwrap();

        let read_page = disk.read_page(0).unwrap();
        assert_eq!(read_page.get_record(slot).unwrap(), b"persistent record");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_manager_invalid_page_id() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_test_invalid_page_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let err = disk.read_page(99).unwrap_err();
        assert_eq!(err, Error::InvalidPageId(99));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_manager_corrupted_file_rejection() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_test_corrupt_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(&db_path, [0xAA; 100]).unwrap();

        let res = DiskManager::open_or_create(db_path.as_path());
        assert!(res.is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_manager_reopen_and_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_test_reopen_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            let (pid, mut p) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
            p.insert_record(b"reopen check").unwrap();
            disk.write_page(pid, &p).unwrap();
            disk.sync().unwrap();
        }

        {
            let disk2 = DiskManager::open_or_create(db_path.as_path()).unwrap();
            assert_eq!(disk2.num_pages(), 1);
            let p = disk2.read_page(0).unwrap();
            assert_eq!(p.get_record(0).unwrap(), b"reopen check");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_manager_rebuild_freelist_preserves_unflushed_pending_and_excludes_pinned() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_test_disk_rebuild_pending_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
        let (p0, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p0, 0);
        let (p1, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p1, 1);
        let (p2, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
        assert_eq!(p2, 2);

        disk.free_page(p1).unwrap();
        disk.stage_free_page(55, p2);

        let mut pinned = std::collections::HashSet::new();
        pinned.insert(p1);
        let rebuilt = disk.rebuild_freelist_from_disk_excluding(&pinned).unwrap();
        assert!(rebuilt.is_empty());
        assert!(!disk.is_page_free(p1));
        assert_eq!(disk.pending_free_txn_ids(), vec![0, 55]);
        assert_eq!(disk.take_pending_free_pages(55), vec![p2]);

        let rebuilt_after_unpin = disk
            .rebuild_freelist_from_disk_excluding(&std::collections::HashSet::new())
            .unwrap();
        assert_eq!(rebuilt_after_unpin, vec![p1]);
        assert!(disk.is_page_free(p1));
        assert!(disk.pending_free_txn_ids().is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn free_pages_batch_persists_and_reopens() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_test_disk_free_batch_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            let mut ids = Vec::new();
            for _ in 0..8 {
                let (p, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
                ids.push(p);
            }
            // duplicate (ids[5]) ko dedupe hona chahiye
            disk.free_pages_batch(&[ids[2], ids[3], ids[5], ids[5]])
                .unwrap();
            assert_eq!(disk.free_page_count(), 3);
            assert!(disk.is_page_free(ids[2]));
            assert!(disk.is_page_free(ids[3]));
            assert!(disk.is_page_free(ids[5]));
            assert!(!disk.is_page_free(ids[4]));

            // idempotent
            disk.free_pages_batch(&[ids[2]]).unwrap();
            assert_eq!(disk.free_page_count(), 3);

            // invalid input: kuch add nahi hona chahiye
            assert!(disk.free_pages_batch(&[ids[6], 0]).is_err());
            assert!(disk.free_pages_batch(&[ids[6], 9999]).is_err());
            assert_eq!(disk.free_page_count(), 3);
            assert!(!disk.is_page_free(ids[6]));
        }

        // reopen: freelist file se
        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            assert_eq!(disk.free_page_count(), 3);
            assert!(disk.is_page_free(2) && disk.is_page_free(3) && disk.is_page_free(5));
        }

        // freelist file hata do: page flags se rebuild hona chahiye
        let fl = DiskManager::freelist_path(&db_path);
        std::fs::remove_file(&fl).unwrap();
        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            assert_eq!(disk.free_page_count(), 3);
            assert!(disk.is_page_free(2) && disk.is_page_free(3) && disk.is_page_free(5));
            // reuse kaam karta hai
            let (p, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
            assert!([2u32, 3, 5].contains(&p));
            assert_eq!(disk.free_page_count(), 2);
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_free_pages_batch_crash_before_freelist_persist() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_test_disk_free_batch_crash_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("crash_test.db");
        let fl_path = DiskManager::freelist_path(&db_path);
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let allocated_pids;
        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            let mut ids = Vec::new();
            for _ in 0..6 {
                let (p, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
                ids.push(p);
            }
            allocated_pids = ids;
            disk.sync().unwrap();
        }

        // Freelist state snapshot before batch free (empty freelist)
        let stale_freelist_bytes = std::fs::read(&fl_path).unwrap_or_default();

        // Perform batch free
        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            disk.free_pages_batch(&[allocated_pids[2], allocated_pids[4]])
                .unwrap();
            // Drop disk manager immediately
        }

        // SIMULATE CRASH: disk had written blank pages + sync_all,
        // but .freelist sidecar was reverted or torn to stale state
        if stale_freelist_bytes.is_empty() {
            let _ = std::fs::remove_file(&fl_path);
        } else {
            std::fs::write(&fl_path, &stale_freelist_bytes).unwrap();
        }

        // Reopen with stale/missing freelist and verify disk-level rebuild recovery
        {
            let disk = DiskManager::open_or_create(db_path.as_path()).unwrap();
            let rebuilt = disk
                .rebuild_freelist_from_disk_excluding(&std::collections::HashSet::new())
                .unwrap();

            assert!(rebuilt.contains(&allocated_pids[2]));
            assert!(rebuilt.contains(&allocated_pids[4]));
            assert!(disk.is_page_free(allocated_pids[2]));
            assert!(disk.is_page_free(allocated_pids[4]));

            // New page allocation must reuse one of the recovered free pages
            let (reused_pid, _) = disk.allocate_page(PAGE_TYPE_SLOTTED).unwrap();
            assert!(reused_pid == allocated_pids[2] || reused_pid == allocated_pids[4]);
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
