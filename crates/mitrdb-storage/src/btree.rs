
thread_local! {
    static THREAD_LEAF_HINTS: std::cell::RefCell<std::collections::HashMap<u64, (PageId, PageId, Option<Vec<u8>>)>> = std::cell::RefCell::new(std::collections::HashMap::new());
}
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::cell::RefCell;
use std::sync::Arc;

#[derive(Default)]
struct LeafWalScratch {
    b0: Vec<u8>,
    b1: Vec<u8>,
    b2: Vec<u8>,
    b3: Vec<u8>,
}

thread_local! {
    static LEAF_WAL_SCRATCH: RefCell<LeafWalScratch> = RefCell::new(LeafWalScratch::default());
}

use mitrdb_common::{
    Error, INVALID_LSN, INVALID_PAGE_ID, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result, TxnId,
};

use crate::buffer_pool::BufferPoolManager;
use crate::page::{
    PAGE_FLAG_FREE, PAGE_TYPE_BTREE_INTERNAL, PAGE_TYPE_BTREE_LEAF, PAGE_TYPE_RAW, Page,
};
use crate::wal::{LogRecord, WalManager};
use mitrdb_observe::GLOBAL_METRICS;

static NEXT_TREE_ID: AtomicU64 = AtomicU64::new(1);

/// Plan for one leaf split, computed before the first write: exact split point,
/// the already-read old next leaf, and how many new pages the whole chain needs.
struct SplitPlan {
    mid: usize,
    old_next: Option<BTreeNode>,
    pages_needed: usize,
}

pub(crate) const NODE_KIND_LEAF: u8 = 1;
pub(crate) const NODE_KIND_INTERNAL: u8 = 2;
pub(crate) const NODE_KIND_FREE: u8 = 0xFF;
const BTREE_NODE_HEADER_OFFSET: usize = PAGE_HEADER_SIZE;
const BTREE_NODE_BODY_OFFSET: usize = PAGE_HEADER_SIZE + 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BTreeNode {
    pub page_id: PageId,
    pub is_leaf: bool,
    pub parent_page_id: PageId,
    pub prev_leaf_page_id: PageId,
    pub next_leaf_page_id: PageId,
    pub max_keys: u16,
    pub keys: Vec<Vec<u8>>,
    pub values: Vec<Vec<u8>>,
    pub children: Vec<PageId>,
}

impl BTreeNode {
    pub fn new_leaf(page_id: PageId, max_keys: u16) -> Self {
        BTreeNode {
            page_id,
            is_leaf: true,
            parent_page_id: INVALID_PAGE_ID,
            prev_leaf_page_id: INVALID_PAGE_ID,
            next_leaf_page_id: INVALID_PAGE_ID,
            max_keys,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn new_internal(page_id: PageId, max_keys: u16) -> Self {
        BTreeNode {
            page_id,
            is_leaf: false,
            parent_page_id: INVALID_PAGE_ID,
            prev_leaf_page_id: INVALID_PAGE_ID,
            next_leaf_page_id: INVALID_PAGE_ID,
            max_keys,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn serialized_body_size(&self) -> usize {
        if self.is_leaf {
            2 + self
                .keys
                .iter()
                .zip(self.values.iter())
                .map(|(k, v)| 4 + k.len() + v.len())
                .sum::<usize>()
        } else {
            2 + 4 + self.keys.iter().map(|k| 2 + k.len() + 4).sum::<usize>()
        }
    }

    pub fn fits_in_page(&self) -> bool {
        BTREE_NODE_BODY_OFFSET + self.serialized_body_size() <= PAGE_SIZE
    }
    pub fn encoded_size(&self) -> usize {
        16 + self.serialized_body_size()
    }

    pub fn encode_payload_into(&self, buf: &mut [u8]) -> Result<()> {
        if !self.fits_in_page() {
            return Err(Error::OutOfSpace);
        }
        let expected_len = PAGE_SIZE - PAGE_HEADER_SIZE;
        if buf.len() < expected_len {
            return Err(Error::OutOfSpace);
        }
        buf[..expected_len].fill(0);

        buf[0] = if self.is_leaf {
            NODE_KIND_LEAF
        } else {
            NODE_KIND_INTERNAL
        };
        buf[1] = 0;
        buf[2..4].copy_from_slice(&self.max_keys.to_le_bytes());
        buf[4..8].copy_from_slice(&self.parent_page_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.prev_leaf_page_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.next_leaf_page_id.to_le_bytes());

        let mut pos = 16;
        let key_cnt = self.keys.len() as u16;
        if pos + 2 > expected_len {
            return Err(Error::OutOfSpace);
        }
        if self.is_leaf {
            if self.keys.len() != self.values.len() {
                return Err(Error::PageCorrupted(
                    "Leaf key/value count mismatch".to_string(),
                ));
            }
            buf[pos..pos + 2].copy_from_slice(&key_cnt.to_le_bytes());
            pos += 2;
            for (k, v) in self.keys.iter().zip(self.values.iter()) {
                let needed = 4 + k.len() + v.len();
                if pos + needed > expected_len {
                    return Err(Error::OutOfSpace);
                }
                buf[pos..pos + 2].copy_from_slice(&(k.len() as u16).to_le_bytes());
                buf[pos + 2..pos + 4].copy_from_slice(&(v.len() as u16).to_le_bytes());
                pos += 4;
                buf[pos..pos + k.len()].copy_from_slice(k);
                pos += k.len();
                buf[pos..pos + v.len()].copy_from_slice(v);
                pos += v.len();
            }
        } else {
            if self.children.len() != self.keys.len() + 1 {
                return Err(Error::PageCorrupted(
                    "Internal node child count mismatch".to_string(),
                ));
            }
            buf[pos..pos + 2].copy_from_slice(&key_cnt.to_le_bytes());
            pos += 2;
            buf[pos..pos + 4].copy_from_slice(&self.children[0].to_le_bytes());
            pos += 4;
            for (i, k) in self.keys.iter().enumerate() {
                let needed = 2 + k.len() + 4;
                if pos + needed > expected_len {
                    return Err(Error::OutOfSpace);
                }
                buf[pos..pos + 2].copy_from_slice(&(k.len() as u16).to_le_bytes());
                pos += 2;
                buf[pos..pos + k.len()].copy_from_slice(k);
                pos += k.len();
                buf[pos..pos + 4].copy_from_slice(&self.children[i + 1].to_le_bytes());
                pos += 4;
            }
        }
        Ok(())
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; PAGE_SIZE - PAGE_HEADER_SIZE];
        self.encode_payload_into(&mut buf)?;
        Ok(buf)
    }

    pub fn from_page(page: &Page) -> Result<Self> {
        let ptype = page.page_type();
        if ptype != PAGE_TYPE_BTREE_LEAF && ptype != PAGE_TYPE_BTREE_INTERNAL {
            return Err(Error::PageCorrupted(format!(
                "Expected B+Tree page type, got {}",
                ptype
            )));
        }
        let buf = &page.data[BTREE_NODE_HEADER_OFFSET..PAGE_SIZE];
        let kind = buf[0];
        let is_leaf = match kind {
            NODE_KIND_LEAF => true,
            NODE_KIND_INTERNAL => false,
            _ => {
                return Err(Error::PageCorrupted(format!(
                    "Invalid B+Tree node kind byte: {}",
                    kind
                )));
            }
        };
        let max_keys = u16::from_le_bytes(buf[2..4].try_into().unwrap());
        let parent_page_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let prev_leaf_page_id = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let next_leaf_page_id = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        let mut pos = 16;
        let key_cnt = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut keys = Vec::with_capacity(key_cnt);
        let mut values = Vec::new();
        let mut children = Vec::new();

        if is_leaf {
            values.reserve(key_cnt);
            for _ in 0..key_cnt {
                if pos + 4 > buf.len() {
                    return Err(Error::PageCorrupted(
                        "Truncated B+Tree leaf entry header".to_string(),
                    ));
                }
                let klen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
                let vlen = u16::from_le_bytes(buf[pos + 2..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + klen + vlen > buf.len() {
                    return Err(Error::PageCorrupted(
                        "Truncated B+Tree leaf entry payload".to_string(),
                    ));
                }
                keys.push(buf[pos..pos + klen].to_vec());
                pos += klen;
                values.push(buf[pos..pos + vlen].to_vec());
                pos += vlen;
            }
        } else {
            if pos + 4 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated B+Tree internal first child".to_string(),
                ));
            }
            children.reserve(key_cnt + 1);
            children.push(u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()));
            pos += 4;
            for _ in 0..key_cnt {
                if pos + 2 > buf.len() {
                    return Err(Error::PageCorrupted(
                        "Truncated B+Tree internal key length".to_string(),
                    ));
                }
                let klen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + klen + 4 > buf.len() {
                    return Err(Error::PageCorrupted(
                        "Truncated B+Tree internal key/child".to_string(),
                    ));
                }
                keys.push(buf[pos..pos + klen].to_vec());
                pos += klen;
                children.push(u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()));
                pos += 4;
            }
        }

        for w in keys.windows(2) {
            if w[0] >= w[1] {
                return Err(Error::PageCorrupted(
                    "B+Tree keys are not strictly sorted".to_string(),
                ));
            }
        }

        Ok(BTreeNode {
            page_id: page.page_id(),
            is_leaf,
            parent_page_id,
            prev_leaf_page_id,
            next_leaf_page_id,
            max_keys,
            keys,
            values,
            children,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyBound {
    Unbounded,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyRange {
    pub start: KeyBound,
    pub end: KeyBound,
}

impl KeyRange {
    pub fn new(start: KeyBound, end: KeyBound) -> Self {
        KeyRange { start, end }
    }

    pub fn full() -> Self {
        KeyRange {
            start: KeyBound::Unbounded,
            end: KeyBound::Unbounded,
        }
    }

    pub fn point(key: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Included(key.clone()),
            end: KeyBound::Included(key),
        }
    }

    pub fn inclusive(start: Vec<u8>, end: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Included(start),
            end: KeyBound::Included(end),
        }
    }

    pub fn exclusive(start: Vec<u8>, end: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Excluded(start),
            end: KeyBound::Excluded(end),
        }
    }

    pub fn from_inclusive(start: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Included(start),
            end: KeyBound::Unbounded,
        }
    }

    pub fn from_exclusive(start: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Excluded(start),
            end: KeyBound::Unbounded,
        }
    }

    pub fn to_inclusive(end: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Unbounded,
            end: KeyBound::Included(end),
        }
    }

    pub fn to_exclusive(end: Vec<u8>) -> Self {
        KeyRange {
            start: KeyBound::Unbounded,
            end: KeyBound::Excluded(end),
        }
    }

    fn is_immediate_byte_successor(lo: &[u8], hi: &[u8]) -> bool {
        hi.len() == lo.len() + 1 && hi.starts_with(lo) && hi[lo.len()] == 0
    }

    pub fn is_empty(&self) -> bool {
        if let KeyBound::Excluded(hi) = &self.end
            && hi.is_empty()
        {
            return true;
        }
        match (&self.start, &self.end) {
            (KeyBound::Included(lo), KeyBound::Included(hi)) => lo > hi,
            (KeyBound::Included(lo), KeyBound::Excluded(hi))
            | (KeyBound::Excluded(lo), KeyBound::Included(hi)) => lo >= hi,
            (KeyBound::Excluded(lo), KeyBound::Excluded(hi)) => {
                lo >= hi || Self::is_immediate_byte_successor(lo, hi)
            }
            _ => false,
        }
    }

    pub fn is_single_key(&self) -> bool {
        match (&self.start, &self.end) {
            (KeyBound::Included(lo), KeyBound::Included(hi)) => lo == hi,
            _ => false,
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        let lower_ok = match &self.start {
            KeyBound::Unbounded => true,
            KeyBound::Included(lo) => key >= lo.as_slice(),
            KeyBound::Excluded(lo) => key > lo.as_slice(),
        };
        if !lower_ok {
            return false;
        }
        match &self.end {
            KeyBound::Unbounded => true,
            KeyBound::Included(hi) => key <= hi.as_slice(),
            KeyBound::Excluded(hi) => key < hi.as_slice(),
        }
    }

    pub fn overlaps(&self, other: &KeyRange) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }
        let max_start = match (&self.start, &other.start) {
            (KeyBound::Unbounded, b) => b.clone(),
            (a, KeyBound::Unbounded) => a.clone(),
            (KeyBound::Included(a), KeyBound::Included(b)) => {
                KeyBound::Included(std::cmp::max(a, b).clone())
            }
            (KeyBound::Excluded(a), KeyBound::Excluded(b)) => {
                KeyBound::Excluded(std::cmp::max(a, b).clone())
            }
            (KeyBound::Included(a), KeyBound::Excluded(b))
            | (KeyBound::Excluded(b), KeyBound::Included(a)) => {
                if a > b {
                    KeyBound::Included(a.clone())
                } else {
                    KeyBound::Excluded(b.clone())
                }
            }
        };
        let min_end = match (&self.end, &other.end) {
            (KeyBound::Unbounded, b) => b.clone(),
            (a, KeyBound::Unbounded) => a.clone(),
            (KeyBound::Included(a), KeyBound::Included(b)) => {
                KeyBound::Included(std::cmp::min(a, b).clone())
            }
            (KeyBound::Excluded(a), KeyBound::Excluded(b)) => {
                KeyBound::Excluded(std::cmp::min(a, b).clone())
            }
            (KeyBound::Included(a), KeyBound::Excluded(b))
            | (KeyBound::Excluded(b), KeyBound::Included(a)) => {
                if a < b {
                    KeyBound::Included(a.clone())
                } else {
                    KeyBound::Excluded(b.clone())
                }
            }
        };
        !KeyRange::new(max_start, min_end).is_empty()
    }
}

pub struct BPlusTree {
    tree_id: u64,
    bpm: Arc<BufferPoolManager>,
    wal: Option<Arc<WalManager>>,
    root_page_id: parking_lot::RwLock<PageId>,
    split_mutex: parking_lot::Mutex<()>,
    max_keys: u16,
    last_leaf_hint: AtomicU32,
    leaf_latches: Box<[parking_lot::Mutex<()>; 64]>,
}

impl BPlusTree {
    pub fn create(
        bpm: Arc<BufferPoolManager>,
        wal: Option<Arc<WalManager>>,
        max_keys: u16,
    ) -> Result<Self> {
        let mut prev_lsn = INVALID_LSN;
        Self::create_logged(bpm, wal, max_keys, 0, &mut prev_lsn)
    }

    pub fn create_logged(
        bpm: Arc<BufferPoolManager>,
        wal: Option<Arc<WalManager>>,
        max_keys: u16,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<Self> {
        assert!(max_keys >= 2);
        let _op_guard = bpm.acquire_op_shared();
        if let Some(ref w) = wal {
            w.register_bpm(&bpm);
        }
        let (root_id, _) = bpm.new_page(PAGE_TYPE_BTREE_LEAF)?;
        bpm.unpin_page(root_id, false)?;
        let tree = BPlusTree {
            tree_id: NEXT_TREE_ID.fetch_add(1, Ordering::SeqCst),
            bpm: Arc::clone(&bpm),
            wal,
            root_page_id: parking_lot::RwLock::new(root_id),
            split_mutex: parking_lot::Mutex::new(()),
            max_keys,
            last_leaf_hint: AtomicU32::new(INVALID_PAGE_ID),
            leaf_latches: Box::new(std::array::from_fn(|_| parking_lot::Mutex::new(()))),
        };
        let root_node = BTreeNode::new_leaf(root_id, max_keys);
        tree.write_node(&root_node, txn_id, prev_lsn)?;
        tree.last_leaf_hint.store(root_id, Ordering::Relaxed);
        Ok(tree)
    }

    pub fn open(
        bpm: Arc<BufferPoolManager>,
        wal: Option<Arc<WalManager>>,
        root_page_id: PageId,
        max_keys: u16,
    ) -> Result<Self> {
        assert!(max_keys >= 2);
        if let Some(ref w) = wal {
            w.register_bpm(&bpm);
        }
        let page = bpm.fetch_page(root_page_id)?;
        let _ = BTreeNode::from_page(&page)?;
        bpm.unpin_page(root_page_id, false)?;
        Ok(BPlusTree {
            tree_id: NEXT_TREE_ID.fetch_add(1, Ordering::SeqCst),
            bpm,
            wal,
            root_page_id: parking_lot::RwLock::new(root_page_id),
            split_mutex: parking_lot::Mutex::new(()),
            max_keys,
            last_leaf_hint: AtomicU32::new(INVALID_PAGE_ID),
            leaf_latches: Box::new(std::array::from_fn(|_| parking_lot::Mutex::new(()))),
        })
    }

    pub fn tree_id(&self) -> u64 {
        self.tree_id
    }

    pub fn root_page_id(&self) -> PageId {
        *self.root_page_id.read()
    }

    pub fn set_root_page_id(&self, root_page_id: PageId) {
        *self.root_page_id.write() = root_page_id;
        // Root changed from outside: any cached leaf may no longer belong here.
        THREAD_LEAF_HINTS.with(|hints| {
            hints.borrow_mut().remove(&self.tree_id);
        });
    }

    /// Drops the cached rightmost-leaf hint. Safe to call at any time; the next
    /// insert simply does a normal root-to-leaf descent. Intended for callers
    /// that roll back structural changes (e.g. after an aborted split).
    pub fn invalidate_leaf_hint(&self) {
        THREAD_LEAF_HINTS.with(|hints| {
            hints.borrow_mut().remove(&self.tree_id);
        });
    }

    fn read_first_key_zero_copy(&self, page_id: PageId) -> Option<Vec<u8>> {
        self.bpm
            .with_page(page_id, |page| {
                let buf = &page.data[PAGE_HEADER_SIZE..PAGE_SIZE];
                if buf.len() < 22 || buf[0] != NODE_KIND_LEAF {
                    return Ok(None);
                }
                let key_cnt = u16::from_le_bytes([buf[16], buf[17]]) as usize;
                if key_cnt == 0 {
                    return Ok(None);
                }
                let klen = u16::from_le_bytes([buf[18], buf[19]]) as usize;
                if 22 + klen > buf.len() {
                    return Ok(None);
                }
                Ok(Some(buf[22..22 + klen].to_vec()))
            })
            .ok()
            .flatten()
    }

    /// Zero-allocation in-place leaf insert/replace directly inside the buffer pool frame.
    /// Returns:
    /// - `Ok(Some((old_val, next_pid)))` if mutated in-place (or `Err` on validation/WAL failure)
    /// - `Ok(None)` if hint check failed or leaf is full and needs a split.
    fn try_mutate_leaf_in_place(
        &self,
        pid: PageId,
        hint_check: Option<(PageId, &Option<Vec<u8>>)>,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Option<Vec<u8>>, PageId)>> {
        let max_body = PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut outcome: Option<(Option<Vec<u8>>, PageId)> = None;
        let mut wal_err: Option<Error> = None;

        self.bpm.mutate_page(pid, |page| {
            let cur_ptype = page.page_type();
            let cur_lsn = page.page_lsn();
            let mut new_lsn: Option<Lsn> = None;
            let buf = &mut page.data[PAGE_HEADER_SIZE..PAGE_SIZE];
            if buf.len() < 18 || buf[0] != NODE_KIND_LEAF {
                return Ok(());
            }
            let next_pid = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let key_cnt = u16::from_le_bytes([buf[16], buf[17]]) as usize;

            if let Some((exp_next, high_key)) = hint_check {
                if next_pid != exp_next {
                    return Ok(());
                }
                if let Some(hk) = high_key {
                    if key >= hk.as_slice() {
                        return Ok(());
                    }
                }
                if key_cnt > 0 {
                    let first_klen = u16::from_le_bytes([buf[18], buf[19]]) as usize;
                    if 22 + first_klen > buf.len() || key < &buf[22..22 + first_klen] {
                        return Ok(());
                    }
                }
            }

            // Scan packed entries directly on page slice without heap allocations
            let mut pos = 18usize;
            let mut insert_pos = None;
            let mut replace_info: Option<(usize, usize, usize)> = None; // (entry_start, entry_end, val_start)

            for _ in 0..key_cnt {
                if pos + 4 > buf.len() {
                    return Err(Error::PageCorrupted("Truncated B+Tree leaf entry".to_string()));
                }
                let klen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                let vlen = u16::from_le_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
                let entry_end = pos + 4 + klen + vlen;
                if entry_end > buf.len() {
                    return Err(Error::PageCorrupted("Truncated B+Tree leaf key/value".to_string()));
                }
                if insert_pos.is_none() && replace_info.is_none() {
                    let cur_key = &buf[pos + 4..pos + 4 + klen];
                    match cur_key.cmp(key) {
                        std::cmp::Ordering::Equal => {
                            replace_info = Some((pos, entry_end, pos + 4 + klen));
                        }
                        std::cmp::Ordering::Greater => {
                            insert_pos = Some(pos);
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                pos = entry_end;
            }
            let old_payload_len = pos;
            let new_entry_len = 4 + key.len() + value.len();

            if let Some((estart, eend, vstart)) = replace_info {
                let old_entry_len = eend - estart;
                let new_payload_len = old_payload_len - old_entry_len + new_entry_len;
                if new_payload_len > max_body {
                    return Ok(()); // needs split
                }
                let old_val = buf[vstart..eend].to_vec();

                if let Some(ref wal) = self.wal
                    && txn_id != 0
                {
                    let needs_fpi = cur_ptype != PAGE_TYPE_BTREE_LEAF
                        || wal.needs_full_page_image(cur_lsn);
                    let (wal_start, wal_end) = if needs_fpi {
                        (0usize, max_body)
                    } else {
                        (estart, old_payload_len.max(new_payload_len))
                    };
                    let undo = buf[wal_start..wal_end].to_vec();
                    let mut redo = undo.clone();
                    let rel_estart = estart - wal_start;
                    let rel_eend = eend - wal_start;
                    let rel_old_end = old_payload_len - wal_start;
                    redo.copy_within(rel_eend..rel_old_end, rel_estart + new_entry_len);
                    redo[rel_estart..rel_estart + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    redo[rel_estart + 2..rel_estart + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
                    redo[rel_estart + 4..rel_estart + 4 + key.len()].copy_from_slice(key);
                    redo[rel_estart + 4 + key.len()..rel_estart + new_entry_len].copy_from_slice(value);
                    if new_payload_len < old_payload_len {
                        redo[(new_payload_len - wal_start)..rel_old_end].fill(0);
                    }

                    let offset = (PAGE_HEADER_SIZE + wal_start) as u16;
                    match wal.append_record(LogRecord::new_update(
                        0,
                        txn_id,
                        *prev_lsn,
                        pid,
                        offset,
                        redo,
                        undo,
                    )) {
                        Ok(lsn) => {
                            *prev_lsn = lsn;
                            new_lsn = Some(lsn);
                        }
                        Err(e) => {
                            wal_err = Some(e);
                            return Ok(());
                        }
                    }
                }

                buf.copy_within(eend..old_payload_len, estart + new_entry_len);
                buf[estart..estart + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                buf[estart + 2..estart + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
                buf[estart + 4..estart + 4 + key.len()].copy_from_slice(key);
                buf[estart + 4 + key.len()..estart + new_entry_len].copy_from_slice(value);
                if new_payload_len < old_payload_len {
                    buf[new_payload_len..old_payload_len].fill(0);
                }
                if let Some(lsn) = new_lsn { page.set_page_lsn(lsn); }
                page.set_page_type(PAGE_TYPE_BTREE_LEAF);
                page.set_flags(0);
                page.update_checksum();
                outcome = Some((Some(old_val), next_pid));
                return Ok(());
            }

            // Pure insert
            let new_key_cnt = key_cnt + 1;
            let new_payload_len = old_payload_len + new_entry_len;
            if new_key_cnt > (self.max_keys as usize) || new_payload_len > max_body {
                return Ok(()); // needs split
            }
            let ipos = insert_pos.unwrap_or(old_payload_len);

            if let Some(ref wal) = self.wal
                && txn_id != 0
            {
                let needs_fpi = cur_ptype != PAGE_TYPE_BTREE_LEAF
                    || wal.needs_full_page_image(cur_lsn);
                if needs_fpi || new_payload_len <= 1024 {
                    let (wal_start, wal_end) = if needs_fpi {
                        (0usize, max_body)
                    } else {
                        (16usize, new_payload_len)
                    };
                    let (mut undo, mut redo) = LEAF_WAL_SCRATCH.with(|s| {
                        let mut s = s.borrow_mut();
                        (std::mem::take(&mut s.b0), std::mem::take(&mut s.b1))
                    });
                    undo.clear();
                    undo.extend_from_slice(&buf[wal_start..wal_end]);
                    redo.clear();
                    redo.extend_from_slice(&undo);
                    let rel_cnt = 16 - wal_start;
                    redo[rel_cnt..rel_cnt + 2].copy_from_slice(&(new_key_cnt as u16).to_le_bytes());
                    let rel_ipos = ipos - wal_start;
                    let rel_old_end = old_payload_len - wal_start;
                    redo.copy_within(rel_ipos..rel_old_end, rel_ipos + new_entry_len);
                    redo[rel_ipos..rel_ipos + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    redo[rel_ipos + 2..rel_ipos + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
                    redo[rel_ipos + 4..rel_ipos + 4 + key.len()].copy_from_slice(key);
                    redo[rel_ipos + 4 + key.len()..rel_ipos + new_entry_len].copy_from_slice(value);

                    let offset = (PAGE_HEADER_SIZE + wal_start) as u16;
                    let mut rec = LogRecord::new_update(
                        0,
                        txn_id,
                        *prev_lsn,
                        pid,
                        offset,
                        redo,
                        undo,
                    );
                    let app_res = wal.append_record_ref(&mut rec);
                    LEAF_WAL_SCRATCH.with(|s| {
                        let mut s = s.borrow_mut();
                        s.b0 = std::mem::take(&mut rec.undo_data);
                        s.b1 = std::mem::take(&mut rec.redo_data);
                    });
                    match app_res {
                        Ok(lsn) => {
                            *prev_lsn = lsn;
                            new_lsn = Some(lsn);
                        }
                        Err(e) => {
                            wal_err = Some(e);
                            return Ok(());
                        }
                    }
                } else {
                    // Log only the 2-byte key_cnt (16..18) and the newly shifted/inserted tail (ipos..new_payload_len)
                    let (mut undo_cnt, mut redo_cnt, mut undo_tail, mut redo_tail) = LEAF_WAL_SCRATCH.with(|s| {
                        let mut s = s.borrow_mut();
                        (
                            std::mem::take(&mut s.b0),
                            std::mem::take(&mut s.b1),
                            std::mem::take(&mut s.b2),
                            std::mem::take(&mut s.b3),
                        )
                    });
                    undo_cnt.clear();
                    undo_cnt.extend_from_slice(&buf[16..18]);
                    redo_cnt.clear();
                    redo_cnt.extend_from_slice(&(new_key_cnt as u16).to_le_bytes());
                    let tail_len = new_payload_len - ipos;
                    undo_tail.clear();
                    undo_tail.resize(tail_len, 0);
                    let existing_tail = old_payload_len - ipos;
                    undo_tail[..existing_tail].copy_from_slice(&buf[ipos..old_payload_len]);
                    redo_tail.clear();
                    redo_tail.resize(tail_len, 0);
                    redo_tail[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    redo_tail[2..4].copy_from_slice(&(value.len() as u16).to_le_bytes());
                    redo_tail[4..4 + key.len()].copy_from_slice(key);
                    redo_tail[4 + key.len()..new_entry_len].copy_from_slice(value);
                    redo_tail[new_entry_len..].copy_from_slice(&buf[ipos..old_payload_len]);

                    let mut recs = [
                        LogRecord::new_update(
                            0,
                            txn_id,
                            *prev_lsn,
                            pid,
                            (PAGE_HEADER_SIZE + 16) as u16,
                            redo_cnt,
                            undo_cnt,
                        ),
                        LogRecord::new_update(
                            0,
                            txn_id,
                            INVALID_LSN,
                            pid,
                            (PAGE_HEADER_SIZE + ipos) as u16,
                            redo_tail,
                            undo_tail,
                        ),
                    ];
                    let batch_res = wal.append_records_batch(&mut recs);
                    LEAF_WAL_SCRATCH.with(|s| {
                        let mut s = s.borrow_mut();
                        s.b0 = std::mem::take(&mut recs[0].undo_data);
                        s.b1 = std::mem::take(&mut recs[0].redo_data);
                        s.b2 = std::mem::take(&mut recs[1].undo_data);
                        s.b3 = std::mem::take(&mut recs[1].redo_data);
                    });
                    match batch_res {
                        Ok(lsn2) => {
                            *prev_lsn = lsn2;
                            new_lsn = Some(lsn2);
                        }
                        Err(e) => {
                            wal_err = Some(e);
                            return Ok(());
                        }
                    }
                }
            }

            buf[16..18].copy_from_slice(&(new_key_cnt as u16).to_le_bytes());
            buf.copy_within(ipos..old_payload_len, ipos + new_entry_len);
            buf[ipos..ipos + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            buf[ipos + 2..ipos + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
            buf[ipos + 4..ipos + 4 + key.len()].copy_from_slice(key);
            buf[ipos + 4 + key.len()..ipos + new_entry_len].copy_from_slice(value);
            if let Some(lsn) = new_lsn { page.set_page_lsn(lsn); }
                page.set_page_type(PAGE_TYPE_BTREE_LEAF);
            page.set_flags(0);
            page.update_checksum();
            outcome = Some((None, next_pid));
            Ok(())
        })?;

        if let Some(e) = wal_err {
            return Err(e);
        }
        Ok(outcome)
    }

    pub fn read_node(&self, page_id: PageId) -> Result<BTreeNode> {
        let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let page = self.bpm.fetch_page(page_id)?;
        let res = BTreeNode::from_page(&page);
        self.bpm.unpin_page(page_id, false)?;
        if let Some(started) = profile_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::BtreeTraversal,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        res
    }

    fn get_page_payload_len(data: &[u8]) -> usize {
        if data.len() < 18 {
            return data.len();
        }
        let kind = data[0];
        let is_leaf = kind == NODE_KIND_LEAF;
        let mut pos = 16;
        let key_cnt = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if is_leaf {
            for _ in 0..key_cnt {
                if pos + 4 > data.len() {
                    return data.len();
                }
                let klen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                let vlen = u16::from_le_bytes([data[pos + 2], data[pos + 3]]) as usize;
                pos += 4 + klen + vlen;
            }
        } else {
            pos += 4; // first child
            for _ in 0..key_cnt {
                if pos + 2 > data.len() {
                    return data.len();
                }
                let klen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2 + klen + 4;
            }
        }
        pos.min(data.len())
    }

    /// Byte ranges (relative to the node payload) where `old` and `new` differ.
    /// Both slices are treated as zero-padded; nearby differences are merged.
    fn changed_ranges(old: &[u8], new: &[u8]) -> Vec<(usize, usize)> {
        const MERGE_GAP: usize = 32;
        const CHUNK: usize = 64;
        let len = old.len().max(new.len());
        let common = old.len().min(new.len());
        let at = |s: &[u8], i: usize| if i < s.len() { s[i] } else { 0u8 };
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut i = 0;
        while i < len {
            // Skip identical 64-byte chunks quickly (memcmp) before falling back to bytes.
            if i + CHUNK <= common && old[i..i + CHUNK] == new[i..i + CHUNK] {
                i += CHUNK;
                continue;
            }
            if at(old, i) == at(new, i) {
                i += 1;
                continue;
            }
            let start = i;
            let mut end = i + 1;
            let mut j = i + 1;
            while j < len && j - end <= MERGE_GAP {
                if at(old, j) != at(new, j) {
                    end = j + 1;
                }
                j += 1;
            }
            ranges.push((start, end));
            i = j;
        }
        if ranges.is_empty() {
            ranges.push((0, len.min(1)));
        }
        ranges
    }

    fn padded_slice(src: &[u8], start: usize, end: usize) -> Vec<u8> {
        let mut out = vec![0u8; end - start];
        if start < src.len() {
            let n = src.len().min(end) - start;
            out[..n].copy_from_slice(&src[start..start + n]);
        }
        out
    }

    fn write_node(&self, node: &BTreeNode, txn_id: TxnId, prev_lsn: &mut Lsn) -> Result<()> {
        let target_ptype = if node.is_leaf {
            PAGE_TYPE_BTREE_LEAF
        } else {
            PAGE_TYPE_BTREE_INTERNAL
        };

        if let Some(ref wal) = self.wal
            && txn_id != 0
        {
            let mut new_payload = vec![0u8; PAGE_SIZE - PAGE_HEADER_SIZE];
            node.encode_payload_into(&mut new_payload)?;
            let new_len = node.encoded_size().min(PAGE_SIZE - PAGE_HEADER_SIZE);
            new_payload.truncate(new_len);

            // Step 1: Inspect page in-place (read-only, no dirty bit, zero page clone)
            let (old_payload, type_changed) = self.bpm.with_page(node.page_id, |page| {
                let page_body = &page.data[PAGE_HEADER_SIZE..PAGE_SIZE];
                let old_len = Self::get_page_payload_len(page_body);
                let type_changed =
                    page.page_type() != target_ptype || wal.needs_full_page_image(page.page_lsn());
                let old_data = if type_changed {
                    page_body.to_vec()
                } else {
                    page_body[..old_len].to_vec()
                };
                Ok((old_data, type_changed))
            })?;

            if type_changed {
                new_payload.resize(PAGE_SIZE - PAGE_HEADER_SIZE, 0);
            }

            // Step 2: Calculate byte delta ranges and append WAL records
            let ranges = if type_changed {
                vec![(0usize, PAGE_SIZE - PAGE_HEADER_SIZE)]
            } else {
                Self::changed_ranges(&old_payload, &new_payload)
            };

            #[cfg(debug_assertions)]
            {
                let n = old_payload.len().max(new_payload.len());
                let mut redo_check = Self::padded_slice(&old_payload, 0, n);
                let mut undo_check = Self::padded_slice(&new_payload, 0, n);
                for &(start, end) in &ranges {
                    redo_check[start..end].copy_from_slice(&Self::padded_slice(
                        &new_payload,
                        start,
                        end,
                    ));
                    undo_check[start..end].copy_from_slice(&Self::padded_slice(
                        &old_payload,
                        start,
                        end,
                    ));
                }
                assert_eq!(redo_check, Self::padded_slice(&new_payload, 0, n));
                assert_eq!(undo_check, Self::padded_slice(&old_payload, 0, n));
            }

            let mut lsn = *prev_lsn;
            for (start, end) in ranges {
                let redo = Self::padded_slice(&new_payload, start, end);
                let undo = Self::padded_slice(&old_payload, start, end);
                let offset = u16::try_from(PAGE_HEADER_SIZE + start).map_err(|_| {
                    Error::PageCorrupted("WAL record offset out of range".to_string())
                })?;
                lsn = wal.append_record(LogRecord::new_update(
                    0,
                    txn_id,
                    *prev_lsn,
                    node.page_id,
                    offset,
                    redo,
                    undo,
                ))?;
                *prev_lsn = lsn;
            }

            // Step 3: Single atomic mutation applying payload + LSN stamp + checksum
            let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
            let res = self.bpm.mutate_page(node.page_id, |page| {
                page.set_page_type(target_ptype);
                page.set_flags(0);
                page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + new_len]
                    .copy_from_slice(&new_payload[..new_len]);
                page.data[PAGE_HEADER_SIZE + new_len..PAGE_SIZE].fill(0);
                page.set_page_lsn(lsn);
                page.update_checksum();
                Ok(())
            });

            if let Some(started) = profile_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::BtreePageWrite,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            res
        } else {
            let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
            let result = self.bpm.mutate_page(node.page_id, |page| {
                page.set_page_type(target_ptype);
                page.set_flags(0);
                node.encode_payload_into(&mut page.data[PAGE_HEADER_SIZE..PAGE_SIZE])?;
                page.update_checksum();
                Ok(())
            });

            if let Some(started) = profile_started {
                mitrdb_observe::record_profile_duration(
                    mitrdb_observe::ProfilePhase::BtreePageWrite,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            result
        }
    }

    fn route_internal_page(page: &Page, key: &[u8]) -> Result<Option<PageId>> {
        let ptype = page.page_type();
        if ptype != PAGE_TYPE_BTREE_LEAF && ptype != PAGE_TYPE_BTREE_INTERNAL {
            return Err(Error::PageCorrupted(
                "Expected B+Tree page type".to_string(),
            ));
        }
        let buf = &page.data[BTREE_NODE_HEADER_OFFSET..PAGE_SIZE];
        if buf[0] == NODE_KIND_LEAF {
            return Ok(None);
        }
        if buf[0] != NODE_KIND_INTERNAL {
            return Err(Error::PageCorrupted(
                "Invalid B+Tree node kind byte".to_string(),
            ));
        }

        let mut pos = 16;
        if pos + 6 > buf.len() {
            return Err(Error::PageCorrupted(
                "Truncated internal node header".to_string(),
            ));
        }
        let key_cnt = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        let mut target_child = u32::from_le_bytes(buf[pos + 2..pos + 6].try_into().unwrap());
        pos += 6;

        for _ in 0..key_cnt {
            if pos + 2 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated internal key len".to_string(),
                ));
            }
            let klen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + klen + 4 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated internal entry payload".to_string(),
                ));
            }
            let node_key = &buf[pos..pos + klen];
            pos += klen;
            let child_id = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
            pos += 4;

            if node_key <= key {
                target_child = child_id;
            } else {
                break;
            }
        }

        Ok(Some(target_child))
    }

    fn search_leaf_page(page: &Page, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ptype = page.page_type();
        if ptype != PAGE_TYPE_BTREE_LEAF {
            return Err(Error::PageCorrupted(
                "Expected B+Tree leaf page type".to_string(),
            ));
        }
        let buf = &page.data[BTREE_NODE_HEADER_OFFSET..PAGE_SIZE];
        if buf[0] != NODE_KIND_LEAF {
            return Err(Error::PageCorrupted(
                "Invalid B+Tree leaf node kind byte".to_string(),
            ));
        }

        let mut pos = 16;
        if pos + 2 > buf.len() {
            return Err(Error::PageCorrupted("Truncated leaf header".to_string()));
        }
        let key_cnt = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        for _ in 0..key_cnt {
            if pos + 4 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated leaf entry header".to_string(),
                ));
            }
            let klen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            let vlen = u16::from_le_bytes(buf[pos + 2..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + klen + vlen > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated leaf entry payload".to_string(),
                ));
            }
            let entry_key = &buf[pos..pos + klen];
            pos += klen;
            let entry_val = &buf[pos..pos + vlen];
            pos += vlen;

            if entry_key == key {
                return Ok(Some(entry_val.to_vec()));
            } else if entry_key > key {
                // Keys sorted hain, aage dhoondhne ki zaroorat nahi
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn find_leaf_page_id(&self, root_id: PageId, key: &[u8]) -> Result<PageId> {
        let mut curr_id = root_id;
        loop {
            let route_res = self.bpm.with_page(curr_id, |page| Self::route_internal_page(page, key))?;
            match route_res {
                Some(child_id) => {
                    curr_id = child_id;
                }
                None => {
                    return Ok(curr_id);
                }
            }
        }
    }

    fn find_leaf_page(&self, root_id: PageId, key: &[u8]) -> Result<BTreeNode> {
        let leaf_id = self.find_leaf_page_id(root_id, key)?;
        self.read_node(leaf_id)
    }

    fn find_leftmost_leaf(&self, root_id: PageId) -> Result<BTreeNode> {
        let mut curr = self.read_node(root_id)?;
        while !curr.is_leaf {
            let child_id = curr.children[0];
            curr = self.read_node(child_id)?;
        }
        Ok(curr)
    }

    fn find_rightmost_leaf(&self, root_id: PageId) -> Result<BTreeNode> {
        let mut curr = self.read_node(root_id)?;
        while !curr.is_leaf {
            let child_id = *curr.children.last().unwrap();
            curr = self.read_node(child_id)?;
        }
        Ok(curr)
    }

    /// Point lookup. Always descends from the root (no cached-leaf shortcut):
    /// a stale hint could point at a page that no longer belongs to this tree,
    /// and for random keys the hint only added a wasted page fetch.
    /// The root read guard is held until the leaf has been searched so the leaf
    /// cannot be merged/freed by a writer in between.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root_guard = self.root_page_id.read();
        let leaf_pid = self.find_leaf_page_id(*root_guard, key)?;
        let page = self.bpm.fetch_page(leaf_pid)?;
        let res = Self::search_leaf_page(&page, key);
        self.bpm.unpin_page(leaf_pid, false)?;
        res
    }

    pub fn range_scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_bound = match start_key {
            Some(sk) => KeyBound::Included(sk.to_vec()),
            None => KeyBound::Unbounded,
        };
        let end_bound = match end_key {
            Some(ek) => KeyBound::Included(ek.to_vec()),
            None => KeyBound::Unbounded,
        };
        self.range_scan_bounds(&KeyRange::new(start_bound, end_bound))
    }

    pub fn range_scan_bounds(&self, range: &KeyRange) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let root_wait_started = std::time::Instant::now();
        let root_guard = self.root_page_id.read();
        GLOBAL_METRICS.record_btree_root_wait_ns(
            root_wait_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let mut leaf = match &range.start {
            KeyBound::Included(sk) | KeyBound::Excluded(sk) => {
                self.find_leaf_page(*root_guard, sk)?
            }
            KeyBound::Unbounded => self.find_leftmost_leaf(*root_guard)?,
        };

        let mut out = Vec::new();
        loop {
            let next_id = leaf.next_leaf_page_id;
            // Move keys/values out of the (already owned) leaf instead of cloning.
            for (k, v) in leaf.keys.into_iter().zip(leaf.values) {
                match &range.start {
                    KeyBound::Included(sk) if k.as_slice() < sk.as_slice() => continue,
                    KeyBound::Excluded(sk) if k.as_slice() <= sk.as_slice() => continue,
                    _ => {}
                }
                match &range.end {
                    KeyBound::Included(ek) if k.as_slice() > ek.as_slice() => return Ok(out),
                    KeyBound::Excluded(ek) if k.as_slice() >= ek.as_slice() => return Ok(out),
                    _ => {}
                }
                out.push((k, v));
            }
            if next_id == INVALID_PAGE_ID {
                break;
            }
            leaf = self.read_node(next_id)?;
        }
        Ok(out)
    }

    pub fn range_scan_bounds_rev(&self, range: &KeyRange) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let root_wait_started = std::time::Instant::now();
        let root_guard = self.root_page_id.read();
        GLOBAL_METRICS.record_btree_root_wait_ns(
            root_wait_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let mut leaf = match &range.end {
            KeyBound::Included(ek) | KeyBound::Excluded(ek) => {
                self.find_leaf_page(*root_guard, ek)?
            }
            KeyBound::Unbounded => self.find_rightmost_leaf(*root_guard)?,
        };

        let mut out = Vec::new();
        loop {
            let prev_id = leaf.prev_leaf_page_id;
            for (k, v) in leaf.keys.into_iter().zip(leaf.values).rev() {
                match &range.end {
                    KeyBound::Included(ek) if k.as_slice() > ek.as_slice() => continue,
                    KeyBound::Excluded(ek) if k.as_slice() >= ek.as_slice() => continue,
                    _ => {}
                }
                match &range.start {
                    KeyBound::Included(sk) if k.as_slice() < sk.as_slice() => return Ok(out),
                    KeyBound::Excluded(sk) if k.as_slice() <= sk.as_slice() => return Ok(out),
                    _ => {}
                }
                out.push((k, v));
            }
            if prev_id == INVALID_PAGE_ID {
                break;
            }
            leaf = self.read_node(prev_id)?;
        }
        Ok(out)
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut prev_lsn = INVALID_LSN;
        self.insert_logged(0, &mut prev_lsn, key, value)
    }

    pub fn insert_or_replace_logged(
        &self,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if BTREE_NODE_BODY_OFFSET + 2 + 4 + key.len() + value.len() > PAGE_SIZE
            || BTREE_NODE_BODY_OFFSET + 2 + 4 + 2 + key.len() + 4 > PAGE_SIZE
        {
            return Err(Error::OutOfSpace);
        }
        let _op_guard = self.bpm.acquire_op_shared();

        // FAST PATH: Zero-Allocation In-Place Leaf Mutation with Latched Range Hints
        {
            let root_guard = self.root_page_id.read();
            let root_id = *root_guard;
            let hint = THREAD_LEAF_HINTS.with(|hints| {
                hints.borrow().get(&self.tree_id).cloned()
            });

            let mut tried_hint = false;
            if let Some((hint_pid, exp_next, ref high_key)) = hint {
                if high_key.is_none() || key < high_key.as_ref().unwrap().as_slice() {
                    tried_hint = true;
                    let _leaf_latch = self.leaf_latches[(hint_pid as usize) & 63].lock();
                    if let Some((old_value, _next_pid)) = self.try_mutate_leaf_in_place(
                        hint_pid,
                        Some((exp_next, high_key)),
                        txn_id,
                        prev_lsn,
                        key,
                        value,
                    )? {
                        return Ok(old_value);
                    }
                }
            }

            if !tried_hint || hint.is_some() {
                let real_pid = self.find_leaf_page_id(root_id, key)?;
                let _leaf_latch = self.leaf_latches[(real_pid as usize) & 63].lock();
                if let Some((old_value, next_pid)) = self.try_mutate_leaf_in_place(
                    real_pid,
                    None,
                    txn_id,
                    prev_lsn,
                    key,
                    value,
                )? {
                    let high_key = if next_pid == INVALID_PAGE_ID {
                        None
                    } else {
                        self.read_first_key_zero_copy(next_pid)
                    };
                    if next_pid == INVALID_PAGE_ID || high_key.is_some() {
                        THREAD_LEAF_HINTS.with(|hints| {
                            hints.borrow_mut().insert(self.tree_id, (real_pid, next_pid, high_key));
                        });
                    }
                    return Ok(old_value);
                }
            }
        }

        // SLOW PATH A: Non-root leaf split under root read lock + split_mutex
        // (avoids blocking fast-path readers/writers when root_page_id does not change).
        {
            let _split_guard = self.split_mutex.lock();
            let root_read = self.root_page_id.read();
            let mut root_copy = *root_read;
            let mut leaf = self.find_leaf_page(root_copy, key)?;
            let _leaf_latch = self.leaf_latches[(leaf.page_id as usize) & 63].lock();
            let old_value = match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => Some(std::mem::replace(&mut leaf.values[idx], value.to_vec())),
                Err(idx) => {
                    leaf.keys.insert(idx, key.to_vec());
                    leaf.values.insert(idx, value.to_vec());
                    None
                }
            };

            if leaf.keys.len() <= (self.max_keys as usize) && leaf.fits_in_page() {
                let pid = leaf.page_id;
                let next_pid = leaf.next_leaf_page_id;
                self.write_node(&leaf, txn_id, prev_lsn)?;
                let high_key = if next_pid == INVALID_PAGE_ID {
                    None
                } else {
                    self.read_node(next_pid).ok().and_then(|n| n.keys.first().cloned())
                };
                if next_pid == INVALID_PAGE_ID || high_key.is_some() {
                    THREAD_LEAF_HINTS.with(|hints| {
                        hints.borrow_mut().insert(self.tree_id, (pid, next_pid, high_key));
                    });
                }
                return Ok(old_value);
            }

            let plan = self.plan_split(&leaf)?;
            if plan.pages_needed == 1 {
                let mut pages = self.alloc_split_pages(1)?;
                let res = self.split_leaf(
                    &mut root_copy,
                    leaf,
                    plan.mid,
                    plan.old_next,
                    &mut pages,
                    txn_id,
                    prev_lsn,
                );
                debug_assert_eq!(root_copy, *root_read);
                self.release_split_pages(&mut pages);
                res?;
                return Ok(old_value);
            }
        }

        // SLOW PATH B: Ancestor/root split (pages_needed > 1) requires exclusive root write lock
        let mut root_guard = self.root_page_id.write();
        let mut leaf = self.find_leaf_page(*root_guard, key)?;
        let _leaf_latch = self.leaf_latches[(leaf.page_id as usize) & 63].lock();
        let old_value = match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(idx) => Some(std::mem::replace(&mut leaf.values[idx], value.to_vec())),
            Err(idx) => {
                leaf.keys.insert(idx, key.to_vec());
                leaf.values.insert(idx, value.to_vec());
                None
            }
        };

        if leaf.keys.len() <= (self.max_keys as usize) && leaf.fits_in_page() {
            let pid = leaf.page_id;
            let next_pid = leaf.next_leaf_page_id;
            self.write_node(&leaf, txn_id, prev_lsn)?;
            let high_key = if next_pid == INVALID_PAGE_ID {
                None
            } else {
                self.read_node(next_pid).ok().and_then(|n| n.keys.first().cloned())
            };
            if next_pid == INVALID_PAGE_ID || high_key.is_some() {
                THREAD_LEAF_HINTS.with(|hints| {
                    hints.borrow_mut().insert(self.tree_id, (pid, next_pid, high_key));
                });
            }
            return Ok(old_value);
        }
        drop(_leaf_latch);

        let plan = self.plan_split(&leaf)?;
        let mut pages = self.alloc_split_pages(plan.pages_needed)?;
        let res = self.split_leaf(
            &mut root_guard,
            leaf,
            plan.mid,
            plan.old_next,
            &mut pages,
            txn_id,
            prev_lsn,
        );
        debug_assert!(
            res.is_err() || pages.is_empty(),
            "split plan allocated more pages than the split used"
        );
        self.release_split_pages(&mut pages);
        res?;
        Ok(old_value)
    }

    pub fn insert_logged(
        &self,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.insert_or_replace_logged(txn_id, prev_lsn, key, value)
            .map(|_| ())
    }

    /// Split point of an (already overfull) leaf. Pure: used by both the plan and the real split.
    fn choose_leaf_mid(leaf: &BTreeNode) -> usize {
        let total_len = leaf.keys.len();
        let mut prefix_bytes = Vec::with_capacity(total_len + 1);
        prefix_bytes.push(0usize);
        for (k, v) in leaf.keys.iter().zip(leaf.values.iter()) {
            prefix_bytes.push(prefix_bytes.last().copied().unwrap_or(0) + 4 + k.len() + v.len());
        }
        let total_bytes = *prefix_bytes.last().unwrap_or(&0);
        let mut mid = total_len / 2;
        // Rightmost leaf: split near the end so sequential appends leave full leaves.
        let first_try = if leaf.next_leaf_page_id == INVALID_PAGE_ID {
            total_len.saturating_sub(1)
        } else {
            total_len / 2
        };
        for cand_mid in std::iter::once(first_try).chain(1..total_len) {
            if cand_mid == 0 || cand_mid >= total_len {
                continue;
            }
            let left_body = 2 + prefix_bytes[cand_mid];
            let right_body = 2 + total_bytes - prefix_bytes[cand_mid];
            if BTREE_NODE_BODY_OFFSET + left_body <= PAGE_SIZE
                && BTREE_NODE_BODY_OFFSET + right_body <= PAGE_SIZE
            {
                mid = cand_mid;
                break;
            }
        }
        mid
    }

    /// Split point of an (already overfull) internal node. Pure.
    fn choose_internal_mid(node: &BTreeNode) -> usize {
        let total_len = node.keys.len();
        let mut prefix_bytes = Vec::with_capacity(total_len + 1);
        prefix_bytes.push(0usize);
        for k in &node.keys {
            prefix_bytes.push(prefix_bytes.last().copied().unwrap_or(0) + 2 + k.len() + 4);
        }
        let mut mid = total_len / 2;
        for cand_mid in std::iter::once(total_len / 2).chain(1..total_len.saturating_sub(1)) {
            if cand_mid >= total_len {
                continue;
            }
            let left_body = 2 + 4 + prefix_bytes[cand_mid];
            let right_body = 2 + 4 + prefix_bytes[total_len] - prefix_bytes[cand_mid + 1];
            if BTREE_NODE_BODY_OFFSET + left_body <= PAGE_SIZE
                && BTREE_NODE_BODY_OFFSET + right_body <= PAGE_SIZE
            {
                mid = cand_mid;
                break;
            }
        }
        mid
    }

    /// Read everything the split chain will read and count the exact number of new pages
    /// (1 leaf + 1 per splitting ancestor + 1 if the root splits), before any write.
    fn plan_split(&self, leaf: &BTreeNode) -> Result<SplitPlan> {
        let old_next = if leaf.next_leaf_page_id != INVALID_PAGE_ID {
            Some(self.read_node(leaf.next_leaf_page_id)?)
        } else {
            None
        };
        let mid_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let mid = Self::choose_leaf_mid(leaf);
        if let Some(started) = mid_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitMidSelection,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        let mut sep = leaf.keys[mid].clone();
        let mut pages_needed = 1usize;
        let mut left_id = leaf.page_id;
        let mut pid = leaf.parent_page_id;
        loop {
            if pid == INVALID_PAGE_ID {
                pages_needed += 1; // new root
                break;
            }
            let node = self.read_node(pid)?;
            let pos = node
                .children
                .iter()
                .position(|&c| c == left_id)
                .ok_or_else(|| {
                    Error::PageCorrupted("Left child not found in B+Tree parent".to_string())
                })?;
            // Common case (parent has room): pure arithmetic, no node clone.
            if node.keys.len() < (self.max_keys as usize)
                && BTREE_NODE_BODY_OFFSET + node.serialized_body_size() + 2 + sep.len() + 4
                    <= PAGE_SIZE
            {
                break;
            }
            let mut sim = node.clone();
            sim.keys.insert(pos, sep.clone());
            sim.children.insert(pos + 1, INVALID_PAGE_ID);
            for &c in &node.children {
                self.read_node(c)?;
            }
            pages_needed += 1;
            let m = Self::choose_internal_mid(&sim);
            sep = sim.keys[m].clone();
            left_id = pid;
            pid = node.parent_page_id;
        }
        Ok(SplitPlan {
            mid,
            old_next,
            pages_needed,
        })
    }

    /// Allocate all pages of a split up front. On failure everything allocated so far is freed.
    /// Returned order: pop() gives the leaf page first, then internal pages.
    fn alloc_split_pages(&self, n: usize) -> Result<Vec<PageId>> {
        let alloc_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let mut pages: Vec<PageId> = Vec::with_capacity(n);
        for i in 0..n {
            let ptype = if i == 0 {
                PAGE_TYPE_BTREE_LEAF
            } else {
                PAGE_TYPE_BTREE_INTERNAL
            };
            match self.bpm.new_page(ptype) {
                Ok((id, _)) => {
                    pages.push(id);
                    if let Err(e) = self.bpm.unpin_page(id, false) {
                        self.release_split_pages(&mut pages);
                        return Err(e);
                    }
                }
                Err(e) => {
                    self.release_split_pages(&mut pages);
                    return Err(e);
                }
            }
        }
        pages.reverse();
        if let Some(started) = alloc_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitPageAllocation,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        Ok(pages)
    }

    /// Give back pages that were allocated for a split but never used (error path only).
    fn release_split_pages(&self, pages: &mut Vec<PageId>) {
        for id in pages.drain(..) {
            let _ = self.bpm.free_page_in_op(id);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn split_leaf(
        &self,
        root_guard: &mut PageId,
        mut left_leaf: BTreeNode,
        mid: usize,
        old_next_leaf: Option<BTreeNode>,
        pages: &mut Vec<PageId>,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<()> {
        let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let right_id = pages
            .pop()
            .ok_or_else(|| Error::PageCorrupted("split page pool exhausted".to_string()))?;

        let redistribution_started =
            mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let right_keys = left_leaf.keys.split_off(mid);
        let right_values = left_leaf.values.split_off(mid);
        let separator_key = right_keys[0].clone();

        let old_next_id = left_leaf.next_leaf_page_id;
        left_leaf.next_leaf_page_id = right_id;

        let mut right_leaf = BTreeNode::new_leaf(right_id, self.max_keys);
        right_leaf.parent_page_id = left_leaf.parent_page_id;
        right_leaf.prev_leaf_page_id = left_leaf.page_id;
        right_leaf.next_leaf_page_id = old_next_id;
        right_leaf.keys = right_keys;
        right_leaf.values = right_values;
        if let Some(started) = redistribution_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitRedistribution,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }

        let parent_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        self.write_node(&left_leaf, txn_id, prev_lsn)?;
        self.write_node(&right_leaf, txn_id, prev_lsn)?;

        if old_next_id == INVALID_PAGE_ID {
            self.last_leaf_hint.store(right_id, Ordering::Relaxed);
        }

        if let Some(mut next_leaf) = old_next_leaf {
            next_leaf.prev_leaf_page_id = right_id;
            self.write_node(&next_leaf, txn_id, prev_lsn)?;
        }

        let left_pid = left_leaf.page_id;
        let right_pid = right_leaf.page_id;
        let next_after_right = right_leaf.next_leaf_page_id;
        let result = self.insert_into_parent(
            root_guard,
            pages,
            left_leaf.parent_page_id,
            left_pid,
            separator_key,
            right_pid,
            txn_id,
            prev_lsn,
        );
        if result.is_ok() {
            let right_high = if next_after_right == INVALID_PAGE_ID {
                None
            } else {
                self.read_first_key_zero_copy(next_after_right)
            };
            THREAD_LEAF_HINTS.with(|hints| {
                hints.borrow_mut().insert(self.tree_id, (right_pid, next_after_right, right_high));
            });
        }
        if let Some(started) = parent_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitParentUpdate,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        if let Some(started) = profile_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::BtreeLeafSplit,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_into_parent(
        &self,
        root_guard: &mut PageId,
        pages: &mut Vec<PageId>,
        parent_id: PageId,
        left_child_id: PageId,
        separator_key: Vec<u8>,
        right_child_id: PageId,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<()> {
        if parent_id == INVALID_PAGE_ID {
            let new_root_id = pages
                .pop()
                .ok_or_else(|| Error::PageCorrupted("split page pool exhausted".to_string()))?;

            let mut new_root = BTreeNode::new_internal(new_root_id, self.max_keys);
            new_root.keys.push(separator_key);
            new_root.children.push(left_child_id);
            new_root.children.push(right_child_id);
            self.write_node(&new_root, txn_id, prev_lsn)?;

            let mut left_node = self.read_node(left_child_id)?;
            left_node.parent_page_id = new_root_id;
            self.write_node(&left_node, txn_id, prev_lsn)?;

            let mut right_node = self.read_node(right_child_id)?;
            right_node.parent_page_id = new_root_id;
            self.write_node(&right_node, txn_id, prev_lsn)?;

            *root_guard = new_root_id;
            return Ok(());
        }

        let mut parent = self.read_node(parent_id)?;
        let pos = parent
            .children
            .iter()
            .position(|&c| c == left_child_id)
            .ok_or_else(|| {
                Error::PageCorrupted("Left child not found in B+Tree parent".to_string())
            })?;

        parent.keys.insert(pos, separator_key);
        parent.children.insert(pos + 1, right_child_id);

        if parent.keys.len() <= (self.max_keys as usize) && parent.fits_in_page() {
            self.write_node(&parent, txn_id, prev_lsn)?;
            return Ok(());
        }

        self.split_internal(root_guard, parent, pages, txn_id, prev_lsn)
    }

    fn split_internal(
        &self,
        root_guard: &mut PageId,
        mut left_internal: BTreeNode,
        pages: &mut Vec<PageId>,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<()> {
        let profile_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        let mid = Self::choose_internal_mid(&left_internal);
        let right_id = pages
            .pop()
            .ok_or_else(|| Error::PageCorrupted("split page pool exhausted".to_string()))?;

        let redistribution_started =
            mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        // Split after the promoted key, then pop it from the left node. This avoids
        // remove(0) shifting the entire tail and avoids cloning all right children.
        let right_keys = left_internal.keys.split_off(mid + 1);
        let promoted_key = left_internal.keys.pop().ok_or_else(|| {
            Error::PageCorrupted("Cannot promote key from empty internal split".to_string())
        })?;
        let right_children = left_internal.children.split_off(mid + 1);

        let mut right_internal = BTreeNode::new_internal(right_id, self.max_keys);
        right_internal.parent_page_id = left_internal.parent_page_id;
        right_internal.keys = right_keys;
        right_internal.children = right_children;
        if let Some(started) = redistribution_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitRedistribution,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }

        let parent_started = mitrdb_observe::profiling_enabled().then(std::time::Instant::now);
        self.write_node(&left_internal, txn_id, prev_lsn)?;
        self.write_node(&right_internal, txn_id, prev_lsn)?;

        for child_id in right_internal.children.iter().copied() {
            let mut child = self.read_node(child_id)?;
            child.parent_page_id = right_id;
            self.write_node(&child, txn_id, prev_lsn)?;
        }

        let result = self.insert_into_parent(
            root_guard,
            pages,
            left_internal.parent_page_id,
            left_internal.page_id,
            promoted_key,
            right_internal.page_id,
            txn_id,
            prev_lsn,
        );
        if let Some(started) = parent_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::SplitParentUpdate,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        if let Some(started) = profile_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::BtreeInternalSplit,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        result
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut prev_lsn = INVALID_LSN;
        self.delete_logged(0, &mut prev_lsn, key)
    }

    pub fn delete_logged(&self, txn_id: TxnId, prev_lsn: &mut Lsn, key: &[u8]) -> Result<bool> {
        let _op_guard = self.bpm.acquire_op_shared();
        let mut root_guard = self.root_page_id.write();
        THREAD_LEAF_HINTS.with(|hints| {
            hints.borrow_mut().remove(&self.tree_id);
        });
        let mut leaf = self.find_leaf_page(*root_guard, key)?;

        let idx = match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(_) => return Ok(false),
        };

        leaf.keys.remove(idx);
        leaf.values.remove(idx);

        self.rebalance_leaf(&mut root_guard, leaf, txn_id, prev_lsn)?;
        Ok(true)
    }

    pub fn delete_batch_logged(
        &self,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
        sorted_keys: &[&[u8]],
    ) -> Result<usize> {
        if sorted_keys.is_empty() {
            return Ok(0);
        }
        let _op_guard = self.bpm.acquire_op_shared();
        let mut root_guard = self.root_page_id.write();
        THREAD_LEAF_HINTS.with(|hints| {
            hints.borrow_mut().remove(&self.tree_id);
        });

        let mut deleted_count = 0;
        let mut key_idx = 0;

        while key_idx < sorted_keys.len() {
            let target_key = sorted_keys[key_idx];
            let mut leaf = self.find_leaf_page(*root_guard, target_key)?;
            let mut deleted_in_leaf = 0;

            // Drain all keys from sorted_keys present in this leaf
            while key_idx < sorted_keys.len() {
                let current_k = sorted_keys[key_idx];
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(current_k)) {
                    Ok(pos) => {
                        leaf.keys.remove(pos);
                        leaf.values.remove(pos);
                        deleted_in_leaf += 1;
                        deleted_count += 1;
                        key_idx += 1;
                    }
                    Err(_) => {
                        // Check if current_k is strictly greater than all keys currently in leaf
                        if let Some(last_k) = leaf.keys.last() {
                            if current_k > last_k.as_slice() {
                                break;
                            }
                        } else {
                            // Leaf has become empty
                            break;
                        }
                        // Non-matching key within range
                        key_idx += 1;
                    }
                }
            }

            if deleted_in_leaf > 0 {
                self.rebalance_leaf(&mut root_guard, leaf, txn_id, prev_lsn)?;
            } else {
                // If nothing was deleted from this leaf, advance to avoid re-evaluating the same key
                key_idx += 1;
            }
        }

        Ok(deleted_count)
    }

    fn rebalance_leaf(
        &self,
        root_guard: &mut PageId,
        mut leaf: BTreeNode,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<()> {
        if leaf.page_id == *root_guard {
            self.write_node(&leaf, txn_id, prev_lsn)?;
            return Ok(());
        }

        let min_keys = (self.max_keys as usize / 2).max(1);
        if leaf.keys.len() >= min_keys {
            self.write_node(&leaf, txn_id, prev_lsn)?;
            return Ok(());
        }

        let parent = self.read_node(leaf.parent_page_id)?;
        let leaf_idx = parent
            .children
            .iter()
            .position(|&c| c == leaf.page_id)
            .ok_or_else(|| {
                Error::PageCorrupted("Leaf child not found in B+Tree parent".to_string())
            })?;

        if leaf_idx > 0 {
            let left_sibling_id = parent.children[leaf_idx - 1];
            let left_sibling = self.read_node(left_sibling_id)?;
            if left_sibling.keys.len() > min_keys {
                let mut cand_left = left_sibling;
                let mut cand_leaf = leaf.clone();
                let mut cand_parent = parent.clone();

                let borrowed_k = cand_left.keys.pop().unwrap();
                let borrowed_v = cand_left.values.pop().unwrap();
                cand_leaf.keys.insert(0, borrowed_k);
                cand_leaf.values.insert(0, borrowed_v);
                cand_parent.keys[leaf_idx - 1] = cand_leaf.keys[0].clone();

                if cand_left.fits_in_page()
                    && cand_leaf.fits_in_page()
                    && cand_parent.fits_in_page()
                {
                    self.write_node(&cand_left, txn_id, prev_lsn)?;
                    self.write_node(&cand_leaf, txn_id, prev_lsn)?;
                    self.write_node(&cand_parent, txn_id, prev_lsn)?;
                    return Ok(());
                }
            }
        }

        if leaf_idx + 1 < parent.children.len() {
            let right_sibling_id = parent.children[leaf_idx + 1];
            let right_sibling = self.read_node(right_sibling_id)?;
            if right_sibling.keys.len() > min_keys {
                let mut cand_right = right_sibling;
                let mut cand_leaf = leaf.clone();
                let mut cand_parent = parent.clone();

                let borrowed_k = cand_right.keys.remove(0);
                let borrowed_v = cand_right.values.remove(0);
                cand_leaf.keys.push(borrowed_k);
                cand_leaf.values.push(borrowed_v);
                cand_parent.keys[leaf_idx] = cand_right.keys[0].clone();

                if cand_right.fits_in_page()
                    && cand_leaf.fits_in_page()
                    && cand_parent.fits_in_page()
                {
                    self.write_node(&cand_right, txn_id, prev_lsn)?;
                    self.write_node(&cand_leaf, txn_id, prev_lsn)?;
                    self.write_node(&cand_parent, txn_id, prev_lsn)?;
                    return Ok(());
                }
            }
        }

        if leaf_idx > 0 {
            let left_sibling_id = parent.children[leaf_idx - 1];
            let mut left_sibling = self.read_node(left_sibling_id)?;

            let mut cand_left = left_sibling.clone();
            cand_left.keys.extend(leaf.keys.iter().cloned());
            cand_left.values.extend(leaf.values.iter().cloned());
            let old_next = leaf.next_leaf_page_id;
            cand_left.next_leaf_page_id = old_next;

            if cand_left.keys.len() <= (self.max_keys as usize) && cand_left.fits_in_page() {
                let next_leaf_opt = if old_next != INVALID_PAGE_ID {
                    let mut nl = self.read_node(old_next)?;
                    nl.prev_leaf_page_id = cand_left.page_id;
                    Some(nl)
                } else {
                    None
                };

                let mut next_parent = parent;
                next_parent.keys.remove(leaf_idx - 1);
                next_parent.children.remove(leaf_idx);

                left_sibling = cand_left;
                self.write_node(&left_sibling, txn_id, prev_lsn)?;
                if let Some(ref nl) = next_leaf_opt {
                    self.write_node(nl, txn_id, prev_lsn)?;
                }

                self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
                self.free_btree_page(leaf.page_id, txn_id, prev_lsn)?;
                return Ok(());
            }
        } else {
            let right_sibling_id = parent.children[1];
            let right_sibling = self.read_node(right_sibling_id)?;

            let mut cand_leaf = leaf.clone();
            cand_leaf.keys.extend(right_sibling.keys.iter().cloned());
            cand_leaf
                .values
                .extend(right_sibling.values.iter().cloned());
            let old_next = right_sibling.next_leaf_page_id;
            cand_leaf.next_leaf_page_id = old_next;

            if cand_leaf.keys.len() <= (self.max_keys as usize) && cand_leaf.fits_in_page() {
                let next_leaf_opt = if old_next != INVALID_PAGE_ID {
                    let mut nl = self.read_node(old_next)?;
                    nl.prev_leaf_page_id = cand_leaf.page_id;
                    Some(nl)
                } else {
                    None
                };

                let mut next_parent = parent;
                next_parent.keys.remove(0);
                next_parent.children.remove(1);

                self.write_node(&cand_leaf, txn_id, prev_lsn)?;
                if let Some(ref nl) = next_leaf_opt {
                    self.write_node(nl, txn_id, prev_lsn)?;
                }

                self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
                self.free_btree_page(right_sibling.page_id, txn_id, prev_lsn)?;
                return Ok(());
            }
        }

        if !leaf.keys.is_empty() {
            self.write_node(&leaf, txn_id, prev_lsn)?;
            return Ok(());
        }

        if leaf_idx > 0 {
            let left_sibling_id = parent.children[leaf_idx - 1];
            let mut left_sibling = self.read_node(left_sibling_id)?;
            let old_next = leaf.next_leaf_page_id;
            left_sibling.next_leaf_page_id = old_next;

            let next_leaf_opt = if old_next != INVALID_PAGE_ID {
                let mut nl = self.read_node(old_next)?;
                nl.prev_leaf_page_id = left_sibling.page_id;
                Some(nl)
            } else {
                None
            };

            let mut next_parent = parent;
            next_parent.keys.remove(leaf_idx - 1);
            next_parent.children.remove(leaf_idx);

            self.write_node(&left_sibling, txn_id, prev_lsn)?;
            if let Some(ref nl) = next_leaf_opt {
                self.write_node(nl, txn_id, prev_lsn)?;
            }

            self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
            self.free_btree_page(leaf.page_id, txn_id, prev_lsn)
        } else {
            let right_sibling_id = parent.children[1];
            let right_sibling = self.read_node(right_sibling_id)?;
            let old_next = right_sibling.next_leaf_page_id;
            leaf.keys = right_sibling.keys;
            leaf.values = right_sibling.values;
            leaf.next_leaf_page_id = old_next;

            let next_leaf_opt = if old_next != INVALID_PAGE_ID {
                let mut nl = self.read_node(old_next)?;
                nl.prev_leaf_page_id = leaf.page_id;
                Some(nl)
            } else {
                None
            };

            let mut next_parent = parent;
            next_parent.keys.remove(0);
            next_parent.children.remove(1);

            self.write_node(&leaf, txn_id, prev_lsn)?;
            if let Some(ref nl) = next_leaf_opt {
                self.write_node(nl, txn_id, prev_lsn)?;
            }

            self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
            self.free_btree_page(right_sibling.page_id, txn_id, prev_lsn)
        }
    }

    fn rebalance_internal(
        &self,
        root_guard: &mut PageId,
        mut node: BTreeNode,
        txn_id: TxnId,
        prev_lsn: &mut Lsn,
    ) -> Result<()> {
        if node.page_id == *root_guard {
            if node.keys.is_empty() {
                if !node.children.is_empty() {
                    let child_id = node.children[0];
                    if child_id == 0 {
                        let mut child = self.read_node(0)?;
                        child.parent_page_id = INVALID_PAGE_ID;
                        self.write_node(&child, txn_id, prev_lsn)?;
                        *root_guard = 0;
                        self.free_btree_page(node.page_id, txn_id, prev_lsn)?;
                    } else {
                        let child = self.read_node(child_id)?;
                        node.is_leaf = child.is_leaf;
                        node.parent_page_id = INVALID_PAGE_ID;
                        node.prev_leaf_page_id = child.prev_leaf_page_id;
                        node.next_leaf_page_id = child.next_leaf_page_id;
                        node.keys = child.keys;
                        node.values = child.values;
                        node.children = child.children.clone();
                        if !node.is_leaf {
                            for grandchild_id in &child.children {
                                let mut gc = self.read_node(*grandchild_id)?;
                                gc.parent_page_id = node.page_id;
                                self.write_node(&gc, txn_id, prev_lsn)?;
                            }
                        }
                        self.write_node(&node, txn_id, prev_lsn)?;
                        self.free_btree_page(child_id, txn_id, prev_lsn)?;
                    }
                } else {
                    self.write_node(&node, txn_id, prev_lsn)?;
                }
            } else {
                self.write_node(&node, txn_id, prev_lsn)?;
            }
            return Ok(());
        }

        let min_keys = (self.max_keys as usize / 2).max(1);
        if node.keys.len() >= min_keys {
            self.write_node(&node, txn_id, prev_lsn)?;
            return Ok(());
        }

        let parent = self.read_node(node.parent_page_id)?;
        let node_idx = parent
            .children
            .iter()
            .position(|&c| c == node.page_id)
            .ok_or_else(|| {
                Error::PageCorrupted("Internal child not found in B+Tree parent".to_string())
            })?;

        if node_idx > 0 {
            let left_sibling_id = parent.children[node_idx - 1];
            let left_sibling = self.read_node(left_sibling_id)?;
            if left_sibling.keys.len() > min_keys {
                let mut cand_left = left_sibling;
                let mut cand_node = node.clone();
                let mut cand_parent = parent.clone();

                let sep_key = cand_parent.keys[node_idx - 1].clone();
                let borrowed_k = cand_left.keys.pop().unwrap();
                let moved_child_id = cand_left.children.pop().unwrap();

                cand_parent.keys[node_idx - 1] = borrowed_k;
                cand_node.keys.insert(0, sep_key);
                cand_node.children.insert(0, moved_child_id);

                if cand_left.fits_in_page()
                    && cand_node.fits_in_page()
                    && cand_parent.fits_in_page()
                {
                    let mut moved_child = self.read_node(moved_child_id)?;
                    moved_child.parent_page_id = cand_node.page_id;
                    self.write_node(&moved_child, txn_id, prev_lsn)?;

                    self.write_node(&cand_left, txn_id, prev_lsn)?;
                    self.write_node(&cand_node, txn_id, prev_lsn)?;
                    self.write_node(&cand_parent, txn_id, prev_lsn)?;
                    return Ok(());
                }
            }
        }

        if node_idx + 1 < parent.children.len() {
            let right_sibling_id = parent.children[node_idx + 1];
            let right_sibling = self.read_node(right_sibling_id)?;
            if right_sibling.keys.len() > min_keys {
                let mut cand_right = right_sibling;
                let mut cand_node = node.clone();
                let mut cand_parent = parent.clone();

                let sep_key = cand_parent.keys[node_idx].clone();
                let borrowed_k = cand_right.keys.remove(0);
                let moved_child_id = cand_right.children.remove(0);

                cand_parent.keys[node_idx] = borrowed_k;
                cand_node.keys.push(sep_key);
                cand_node.children.push(moved_child_id);

                if cand_right.fits_in_page()
                    && cand_node.fits_in_page()
                    && cand_parent.fits_in_page()
                {
                    let mut moved_child = self.read_node(moved_child_id)?;
                    moved_child.parent_page_id = cand_node.page_id;
                    self.write_node(&moved_child, txn_id, prev_lsn)?;

                    self.write_node(&cand_right, txn_id, prev_lsn)?;
                    self.write_node(&cand_node, txn_id, prev_lsn)?;
                    self.write_node(&cand_parent, txn_id, prev_lsn)?;
                    return Ok(());
                }
            }
        }

        if node_idx > 0 {
            let left_sibling_id = parent.children[node_idx - 1];
            let left_sibling = self.read_node(left_sibling_id)?;
            let mut next_parent = parent.clone();
            let sep_key = next_parent.keys.remove(node_idx - 1);
            next_parent.children.remove(node_idx);

            let mut cand_left = left_sibling.clone();
            cand_left.keys.push(sep_key);
            cand_left.keys.extend(node.keys.iter().cloned());
            let moved_children = node.children.clone();
            cand_left.children.extend(node.children.iter().cloned());

            if cand_left.keys.len() <= (self.max_keys as usize) && cand_left.fits_in_page() {
                let mut prepared_children = Vec::with_capacity(moved_children.len());
                for &cid in &moved_children {
                    let mut child = self.read_node(cid)?;
                    child.parent_page_id = cand_left.page_id;
                    prepared_children.push(child);
                }

                for child in prepared_children {
                    self.write_node(&child, txn_id, prev_lsn)?;
                }
                self.write_node(&cand_left, txn_id, prev_lsn)?;

                self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
                self.free_btree_page(node.page_id, txn_id, prev_lsn)?;
                return Ok(());
            } else if !node.keys.is_empty() {
                self.write_node(&node, txn_id, prev_lsn)?;
                return Ok(());
            } else if cand_left.keys.len() >= 2 {
                let mid = cand_left.keys.len() / 2;
                let mut right_keys = cand_left.keys.split_off(mid);
                let new_sep = right_keys.remove(0);
                let right_children = cand_left.children.split_off(mid + 1);

                let mut new_left = left_sibling;
                new_left.keys = cand_left.keys;
                new_left.children = cand_left.children;

                node.keys = right_keys;
                node.children = right_children;

                let mut updated_parent = parent;
                updated_parent.keys[node_idx - 1] = new_sep;

                for &cid in &new_left.children {
                    let mut child = self.read_node(cid)?;
                    if child.parent_page_id != new_left.page_id {
                        child.parent_page_id = new_left.page_id;
                        self.write_node(&child, txn_id, prev_lsn)?;
                    }
                }
                for &cid in &node.children {
                    let mut child = self.read_node(cid)?;
                    if child.parent_page_id != node.page_id {
                        child.parent_page_id = node.page_id;
                        self.write_node(&child, txn_id, prev_lsn)?;
                    }
                }

                self.write_node(&new_left, txn_id, prev_lsn)?;
                self.write_node(&node, txn_id, prev_lsn)?;
                self.write_node(&updated_parent, txn_id, prev_lsn)?;
                return Ok(());
            }
        } else {
            let right_sibling_id = parent.children[1];
            let right_sibling = self.read_node(right_sibling_id)?;
            let mut next_parent = parent.clone();
            let sep_key = next_parent.keys.remove(0);
            next_parent.children.remove(1);

            let mut cand_node = node.clone();
            cand_node.keys.push(sep_key);
            cand_node.keys.extend(right_sibling.keys.iter().cloned());
            let moved_children = right_sibling.children.clone();
            cand_node
                .children
                .extend(right_sibling.children.iter().cloned());

            if cand_node.keys.len() <= (self.max_keys as usize) && cand_node.fits_in_page() {
                let mut prepared_children = Vec::with_capacity(moved_children.len());
                for &cid in &moved_children {
                    let mut child = self.read_node(cid)?;
                    child.parent_page_id = cand_node.page_id;
                    prepared_children.push(child);
                }

                for child in prepared_children {
                    self.write_node(&child, txn_id, prev_lsn)?;
                }
                self.write_node(&cand_node, txn_id, prev_lsn)?;

                self.rebalance_internal(root_guard, next_parent, txn_id, prev_lsn)?;
                self.free_btree_page(right_sibling.page_id, txn_id, prev_lsn)?;
                return Ok(());
            } else if !node.keys.is_empty() {
                self.write_node(&node, txn_id, prev_lsn)?;
                return Ok(());
            } else if cand_node.keys.len() >= 2 {
                let mid = cand_node.keys.len() / 2;
                let mut right_keys = cand_node.keys.split_off(mid);
                let new_sep = right_keys.remove(0);
                let right_children = cand_node.children.split_off(mid + 1);

                node.keys = cand_node.keys;
                node.children = cand_node.children;

                let mut new_right = right_sibling;
                new_right.keys = right_keys;
                new_right.children = right_children;

                let mut updated_parent = parent;
                updated_parent.keys[0] = new_sep;

                for &cid in &node.children {
                    let mut child = self.read_node(cid)?;
                    if child.parent_page_id != node.page_id {
                        child.parent_page_id = node.page_id;
                        self.write_node(&child, txn_id, prev_lsn)?;
                    }
                }
                for &cid in &new_right.children {
                    let mut child = self.read_node(cid)?;
                    if child.parent_page_id != new_right.page_id {
                        child.parent_page_id = new_right.page_id;
                        self.write_node(&child, txn_id, prev_lsn)?;
                    }
                }

                self.write_node(&node, txn_id, prev_lsn)?;
                self.write_node(&new_right, txn_id, prev_lsn)?;
                self.write_node(&updated_parent, txn_id, prev_lsn)?;
                return Ok(());
            }
        }

        self.write_node(&node, txn_id, prev_lsn)
    }

    fn free_btree_page(&self, page_id: PageId, txn_id: TxnId, prev_lsn: &mut Lsn) -> Result<()> {
        if page_id == 0 {
            return Ok(());
        }
        if let Some(ref wal) = self.wal
            && txn_id != 0
        {
            let cur_page = self.bpm.fetch_page(page_id)?;
            let old_payload = cur_page.data[PAGE_HEADER_SIZE..PAGE_SIZE].to_vec();

            let mut empty_payload = vec![0u8; PAGE_SIZE - PAGE_HEADER_SIZE];
            empty_payload[0] = NODE_KIND_FREE;
            let append_res = wal.append_record(LogRecord::new_update(
                0,
                txn_id,
                *prev_lsn,
                page_id,
                PAGE_HEADER_SIZE as u16,
                empty_payload.clone(),
                old_payload,
            ));

            let lsn = match append_res {
                Ok(l) => {
                    *prev_lsn = l;
                    l
                }
                Err(e) => {
                    let _ = self.bpm.unpin_page(page_id, false);
                    return Err(e);
                }
            };

            self.bpm.unpin_page(page_id, false)?;

            self.bpm.update_page_data(page_id, |page| {
                page.set_page_type(PAGE_TYPE_RAW);
                page.set_flags(PAGE_FLAG_FREE);
                page.data[PAGE_HEADER_SIZE..PAGE_SIZE].copy_from_slice(&empty_payload);
                page.set_page_lsn(lsn);
                Ok(())
            })?;
            self.bpm.stage_free_page(txn_id, page_id)
        } else {
            self.bpm.free_page_in_op(page_id)
        }
    }

    pub fn collect_all_pages(&self) -> Result<Vec<PageId>> {
        let root_id = self.root_page_id();
        let mut pages = Vec::new();
        let mut queue = vec![root_id];
        let mut visited = std::collections::HashSet::new();
        while let Some(pid) = queue.pop() {
            if !visited.insert(pid) {
                continue;
            }
            pages.push(pid);
            if let Ok(node) = self.read_node(pid)
                && !node.is_leaf
            {
                for child_id in node.children {
                    if child_id != INVALID_PAGE_ID {
                        queue.push(child_id);
                    }
                }
            }
        }
        Ok(pages)
    }

    pub fn free_all_pages_logged(&self, txn_id: TxnId, prev_lsn: &mut Lsn) -> Result<()> {
        let _op_guard = self.bpm.acquire_op_shared();
        // Every page of this tree is about to be freed: the cached leaf must not survive.
        THREAD_LEAF_HINTS.with(|hints| {
            hints.borrow_mut().remove(&self.tree_id);
        });
        let pages = self.collect_all_pages()?;
        for pid in pages {
            self.free_btree_page(pid, txn_id, prev_lsn)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::recovery::RecoveryManager;

    #[test]
    fn test_btree_torn_leaf_rebuilt_after_checkpoint_first_write() {
        use std::io::{Seek, SeekFrom, Write};
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_torn_e2e_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let root;

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let tree = BPlusTree::create(bpm.clone(), Some(wal.clone()), 16).unwrap();

            let mut last = wal.append_record(LogRecord::new_begin(0, 7)).unwrap();
            for i in 0..5 {
                let k = format!("key_{:02}", i);
                tree.insert_logged(7, &mut last, k.as_bytes(), b"v")
                    .unwrap();
            }
            wal.commit(7, last).unwrap();
            recovery.create_checkpoint().unwrap();

            // First write to this leaf after the checkpoint: must be logged as a full image.
            let mut last = wal.append_record(LogRecord::new_begin(0, 8)).unwrap();
            tree.insert_logged(8, &mut last, b"key_new", b"v").unwrap();
            wal.commit(8, last).unwrap();
            root = tree.root_page_id();
        }

        // Torn write: only the first 2 KB of the leaf page are garbage, rest is old data.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&db_path)
                .unwrap();
            f.seek(SeekFrom::Start(root as u64 * PAGE_SIZE as u64))
                .unwrap();
            f.write_all(&[0xABu8; 2048]).unwrap();
            f.sync_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let report = recovery.recover().unwrap();
            assert!(report.redone_records >= 1);

            let page = bpm.fetch_page(root).unwrap();
            assert_eq!(page.page_type(), PAGE_TYPE_BTREE_LEAF);
            let node = BTreeNode::from_page(&page).unwrap();
            let mut keys: Vec<Vec<u8>> = node.keys.clone();
            keys.sort();
            assert_eq!(keys.len(), 6);
            assert!(keys.contains(&b"key_new".to_vec()));
            assert!(keys.contains(&b"key_00".to_vec()));
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_torn_leaf_after_rollback_across_checkpoint() {
        use std::io::{Seek, SeekFrom, Write};
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_torn_rb_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let root;

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let tree = BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap();

            // txn 6: baseline, then checkpoint A.
            let mut last = wal.append_record(LogRecord::new_begin(0, 6)).unwrap();
            for i in 0..2 {
                let k = format!("key_{:02}", i);
                tree.insert_logged(6, &mut last, k.as_bytes(), b"v")
                    .unwrap();
            }
            wal.commit(6, last).unwrap();
            recovery.create_checkpoint().unwrap();

            // txn 8: first write after checkpoint A (full image), committed.
            let mut last = wal.append_record(LogRecord::new_begin(0, 8)).unwrap();
            tree.insert_logged(8, &mut last, b"key_02", b"v").unwrap();
            wal.commit(8, last).unwrap();

            // txn 7: small update (page already has its image), stays active.
            let mut last7 = wal.append_record(LogRecord::new_begin(0, 7)).unwrap();
            tree.insert_logged(7, &mut last7, b"key_03", b"v").unwrap();

            // checkpoint B with txn 7 active, then rollback: the CLR is the first write after B.
            recovery.create_checkpoint().unwrap();
            recovery.rollback_txn(7).unwrap();
            root = tree.root_page_id();
        }

        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&db_path)
                .unwrap();
            f.seek(SeekFrom::Start(root as u64 * PAGE_SIZE as u64))
                .unwrap();
            f.write_all(&[0xABu8; 2048]).unwrap();
            f.sync_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            recovery.recover().unwrap();

            let page = bpm.fetch_page(root).unwrap();
            assert_eq!(page.page_type(), PAGE_TYPE_BTREE_LEAF);
            let node = BTreeNode::from_page(&page).unwrap();
            let mut keys: Vec<Vec<u8>> = node.keys.clone();
            keys.sort();
            assert_eq!(keys.len(), 3);
            assert!(keys.contains(&b"key_02".to_vec()));
            assert!(!keys.contains(&b"key_03".to_vec()));
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_torn_leaf_after_rollback_across_checkpoint_multi_segment() {
        use std::io::{Seek, SeekFrom, Write};
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_torn_rb_seg_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);
        let root;

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let wal =
                Arc::new(WalManager::open_with_segment_size(wal_path.as_path(), 256).unwrap());
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            let tree = BPlusTree::create(bpm.clone(), Some(wal.clone()), 4).unwrap();

            // txn 6: baseline, then checkpoint A.
            let mut last = wal.append_record(LogRecord::new_begin(0, 6)).unwrap();
            for i in 0..2 {
                let k = format!("key_{:02}", i);
                tree.insert_logged(6, &mut last, k.as_bytes(), b"v")
                    .unwrap();
            }
            wal.commit(6, last).unwrap();
            recovery.create_checkpoint().unwrap();

            // txn 8: first write after checkpoint A (full image), committed.
            let mut last = wal.append_record(LogRecord::new_begin(0, 8)).unwrap();
            tree.insert_logged(8, &mut last, b"key_02", b"v").unwrap();
            wal.commit(8, last).unwrap();

            // txn 7: small update (page already has its image), stays active.
            let mut last7 = wal.append_record(LogRecord::new_begin(0, 7)).unwrap();
            tree.insert_logged(7, &mut last7, b"key_03", b"v").unwrap();

            // checkpoint B with txn 7 active, then rollback: the CLR is the first write after B.
            recovery.create_checkpoint().unwrap();
            recovery.rollback_txn(7).unwrap();
            root = tree.root_page_id();
        }

        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&db_path)
                .unwrap();
            f.seek(SeekFrom::Start(root as u64 * PAGE_SIZE as u64))
                .unwrap();
            f.write_all(&[0xABu8; 2048]).unwrap();
            f.sync_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal =
                Arc::new(WalManager::open_with_segment_size(wal_path.as_path(), 256).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
            recovery.recover().unwrap();

            let page = bpm.fetch_page(root).unwrap();
            assert_eq!(page.page_type(), PAGE_TYPE_BTREE_LEAF);
            let node = BTreeNode::from_page(&page).unwrap();
            let mut keys: Vec<Vec<u8>> = node.keys.clone();
            keys.sort();
            assert_eq!(keys.len(), 3);
            assert!(keys.contains(&b"key_02".to_vec()));
            assert!(!keys.contains(&b"key_03".to_vec()));
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_delete_batch_logged_multi_leaf() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_batch_del_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&temp_dir);
        let dm = Arc::new(DiskManager::open_or_create(temp_dir.join("test.db")).unwrap());
        let wm = Arc::new(WalManager::open_or_create(temp_dir.join("test.wal")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(dm, 64));
        let tree = BPlusTree::create(bpm, Some(wm), 64).unwrap();

        let mut lsn = 0;
        let keys: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("key_{:04}", i).into_bytes())
            .collect();
        for k in &keys {
            tree.insert_logged(1, &mut lsn, k, b"val").unwrap();
        }

        // Delete even keys in batch
        let del_keys: Vec<Vec<u8>> = (0..200)
            .filter(|i| i % 2 == 0)
            .map(|i| format!("key_{:04}", i).into_bytes())
            .collect();
        let del_slices: Vec<&[u8]> = del_keys.iter().map(|k| k.as_slice()).collect();
        let deleted = tree.delete_batch_logged(1, &mut lsn, &del_slices).unwrap();
        assert_eq!(deleted, 100);

        for (i, k) in keys.iter().enumerate() {
            let res = tree.get(k).unwrap();
            if i % 2 == 0 {
                assert!(res.is_none(), "Key {} should be deleted", i);
            } else {
                assert_eq!(res, Some(b"val".to_vec()), "Key {} should exist", i);
            }
        }
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_insert_get_and_update() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test1_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
        let tree = BPlusTree::create(bpm.clone(), None, 4).unwrap();

        tree.insert(b"k20", b"val20").unwrap();
        tree.insert(b"k10", b"val10").unwrap();
        tree.insert(b"k30", b"val30").unwrap();

        assert_eq!(tree.get(b"k10").unwrap(), Some(b"val10".to_vec()));
        assert_eq!(tree.get(b"k20").unwrap(), Some(b"val20".to_vec()));
        assert_eq!(tree.get(b"k30").unwrap(), Some(b"val30".to_vec()));
        assert_eq!(tree.get(b"k99").unwrap(), None);

        tree.insert(b"k20", b"updated20").unwrap();
        assert_eq!(tree.get(b"k20").unwrap(), Some(b"updated20".to_vec()));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_sequential_append_leaf_fill() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_fill_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let tree = BPlusTree::create(bpm.clone(), None, 64).unwrap();

        for i in 0..1000u32 {
            let k = format!("key_{:04}", i);
            tree.insert(k.as_bytes(), b"val").unwrap();
        }

        let mut leaf = tree.find_leftmost_leaf(tree.root_page_id()).unwrap();
        let mut leaf_count = 1usize;
        while leaf.next_leaf_page_id != INVALID_PAGE_ID {
            leaf = tree.read_node(leaf.next_leaf_page_id).unwrap();
            leaf_count += 1;
        }
        eprintln!(
            "sequential 1000 keys, max_keys=64: leaf_count={}",
            leaf_count
        );
        // 50/50 split gives ~31 leaves; append-split should give ~16.
        assert!(leaf_count <= 20, "leaf_count={}", leaf_count);

        for i in 0..1000u32 {
            let k = format!("key_{:04}", i);
            assert_eq!(tree.get(k.as_bytes()).unwrap(), Some(b"val".to_vec()));
        }
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_multi_level_splits_and_range_scan() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test2_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let tree = BPlusTree::create(bpm.clone(), None, 3).unwrap();

        for i in (0..40u32).rev() {
            let k = format!("key_{:03}", i);
            let v = format!("value_payload_{:03}", i);
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let root_node = tree.read_node(tree.root_page_id()).unwrap();
        assert!(!root_node.is_leaf);

        for i in 0..40u32 {
            let k = format!("key_{:03}", i);
            let expected = format!("value_payload_{:03}", i);
            assert_eq!(tree.get(k.as_bytes()).unwrap(), Some(expected.into_bytes()));
        }

        let range = tree.range_scan(Some(b"key_010"), Some(b"key_015")).unwrap();
        assert_eq!(range.len(), 6);
        assert_eq!(range[0].0, b"key_010");
        assert_eq!(range[5].0, b"key_015");

        assert!(tree.delete(b"key_012").unwrap());
        assert!(!tree.delete(b"key_012").unwrap());
        assert_eq!(tree.get(b"key_012").unwrap(), None);

        let range_after = tree.range_scan(Some(b"key_010"), Some(b"key_015")).unwrap();
        assert_eq!(range_after.len(), 5);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_wal_crash_recovery_redo_and_undo() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test3_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let root_id: PageId;
        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let mut lsn_t1 = wal.append_record(LogRecord::new_begin(0, 1001)).unwrap();
            let tree =
                BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 3, 1001, &mut lsn_t1)
                    .unwrap();
            for i in 1..=6 {
                let k = format!("committed_{}", i);
                let v = format!("val_{}", i);
                tree.insert_logged(1001, &mut lsn_t1, k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
            wal.commit(1001, lsn_t1).unwrap();
            root_id = tree.root_page_id();

            let mut lsn_t2 = wal.append_record(LogRecord::new_begin(0, 1002)).unwrap();
            tree.insert_logged(1002, &mut lsn_t2, b"uncommitted_key", b"should_roll_back")
                .unwrap();
            wal.flush_up_to(lsn_t2).unwrap();
            bpm.flush_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 16));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let report = recovery.recover().unwrap();
            assert_eq!(report.committed_txns, vec![1001]);
            assert_eq!(report.aborted_txns, vec![1002]);
            assert!(report.undone_records >= 1);

            let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 3).unwrap();
            for i in 1..=6 {
                let k = format!("committed_{}", i);
                let v = format!("val_{}", i);
                assert_eq!(tree.get(k.as_bytes()).unwrap(), Some(v.into_bytes()));
            }
            assert_eq!(tree.get(b"uncommitted_key").unwrap(), None);
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_merge_undo_recovery_and_deferred_free_page_reuse() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mitrdb_btree_test_merge_undo_{}",
            std::process::id()
        ));
        let db_path = temp_dir.join("test.db");
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let root_id: PageId;
        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
            let _recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let mut lsn_t1 = wal.append_record(LogRecord::new_begin(0, 2001)).unwrap();
            let tree =
                BPlusTree::create_logged(bpm.clone(), Some(wal.clone()), 4, 2001, &mut lsn_t1)
                    .unwrap();
            for i in 1..=10 {
                let k = format!("key_{:02}", i);
                let v = format!("val_{:02}", i);
                tree.insert_logged(2001, &mut lsn_t1, k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
            wal.commit(2001, lsn_t1).unwrap();
            root_id = tree.root_page_id();
            assert_eq!(bpm.free_page_count(), 0);

            let mut lsn_t2 = wal.append_record(LogRecord::new_begin(0, 2002)).unwrap();
            for i in 1..=8 {
                let k = format!("key_{:02}", i);
                assert!(tree.delete_logged(2002, &mut lsn_t2, k.as_bytes()).unwrap());
            }
            assert_eq!(bpm.free_page_count(), 0);
            wal.flush_up_to(lsn_t2).unwrap();
            bpm.flush_all().unwrap();
        }

        {
            let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
            let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
            let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
            let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

            let report = recovery.recover().unwrap();
            assert_eq!(report.committed_txns, vec![2001]);
            assert_eq!(report.aborted_txns, vec![2002]);
            assert_eq!(bpm.free_page_count(), 0);

            let tree = BPlusTree::open(bpm.clone(), Some(wal.clone()), root_id, 4).unwrap();
            for i in 1..=10 {
                let k = format!("key_{:02}", i);
                let v = format!("val_{:02}", i);
                assert_eq!(tree.get(k.as_bytes()).unwrap(), Some(v.into_bytes()));
            }

            let mut lsn_t3 = wal.append_record(LogRecord::new_begin(0, 2003)).unwrap();
            for i in 1..=8 {
                let k = format!("key_{:02}", i);
                assert!(tree.delete_logged(2003, &mut lsn_t3, k.as_bytes()).unwrap());
            }
            assert_eq!(bpm.free_page_count(), 0);
            wal.commit(2003, lsn_t3).unwrap();
            assert!(bpm.free_page_count() >= 1);
            bpm.flush_all().unwrap();
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_variable_length_rebalance_and_merge_no_overflow() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test_varlen_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let tree = BPlusTree::create(bpm.clone(), None, 8).unwrap();

        let big_val = vec![b'V'; 1500];
        let small_val = vec![b's'; 32];

        tree.insert(&vec![b'a'; 600], &big_val).unwrap();
        tree.insert(&vec![b'b'; 600], &big_val).unwrap();
        tree.insert(&[b'c'; 40], &small_val).unwrap();
        tree.insert(&[b'd'; 40], &small_val).unwrap();
        tree.insert(&vec![b'e'; 600], &big_val).unwrap();
        tree.insert(&vec![b'f'; 600], &big_val).unwrap();

        assert!(tree.delete(&[b'c'; 40]).unwrap());
        assert!(tree.delete(&[b'd'; 40]).unwrap());
        assert!(tree.delete(&vec![b'a'; 600]).unwrap());
        assert!(tree.delete(&vec![b'b'; 600]).unwrap());

        assert_eq!(tree.get(&vec![b'e'; 600]).unwrap(), Some(big_val.clone()));
        assert_eq!(tree.get(&vec![b'f'; 600]).unwrap(), Some(big_val));
        assert_eq!(tree.range_scan(None, None).unwrap().len(), 2);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_root_collapse_preserves_root_page_identity() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test_root_id_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));

        let tree0 = BPlusTree::create(bpm.clone(), None, 3).unwrap();
        assert_eq!(tree0.root_page_id(), 0);
        for i in 1..=12u32 {
            let k = format!("cat_{:03}", i);
            tree0.insert(k.as_bytes(), b"meta").unwrap();
        }
        for i in 1..=11u32 {
            let k = format!("cat_{:03}", i);
            assert!(tree0.delete(k.as_bytes()).unwrap());
        }
        assert_eq!(tree0.root_page_id(), 0);
        let root0_node = tree0.read_node(0).unwrap();
        assert!(root0_node.is_leaf);
        assert_eq!(tree0.get(b"cat_012").unwrap(), Some(b"meta".to_vec()));

        let tree1 = BPlusTree::create(bpm.clone(), None, 3).unwrap();
        for i in 1..=12u32 {
            let k = format!("u_{:03}", i);
            tree1.insert(k.as_bytes(), b"payload").unwrap();
        }
        let split_root = tree1.root_page_id();
        assert!(!tree1.read_node(split_root).unwrap().is_leaf);
        for i in 1..=11u32 {
            let k = format!("u_{:03}", i);
            assert!(tree1.delete(k.as_bytes()).unwrap());
        }
        assert_eq!(tree1.root_page_id(), split_root);
        let collapsed_node = tree1.read_node(split_root).unwrap();
        assert!(collapsed_node.is_leaf);
        assert_eq!(tree1.get(b"u_012").unwrap(), Some(b"payload".to_vec()));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_btree_rev_scan_and_hint_survives_delete_then_insert() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitrdb_btree_test_rev_{}", std::process::id()));
        let db_path = temp_dir.join("test.db");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 32));
        let tree = BPlusTree::create(bpm.clone(), None, 3).unwrap();

        for i in 0..30u32 {
            let k = format!("r_{:03}", i);
            tree.insert(k.as_bytes(), b"v").unwrap();
        }
        // Deleting resets the hint; later sequential inserts must still be correct.
        for i in 10..25u32 {
            let k = format!("r_{:03}", i);
            assert!(tree.delete(k.as_bytes()).unwrap());
        }
        for i in 30..40u32 {
            let k = format!("r_{:03}", i);
            tree.insert(k.as_bytes(), b"w").unwrap();
        }

        let fwd = tree.range_scan_bounds(&KeyRange::full()).unwrap();
        let mut rev = tree.range_scan_bounds_rev(&KeyRange::full()).unwrap();
        rev.reverse();
        assert_eq!(fwd, rev);
        assert_eq!(fwd.len(), 10 + 5 + 10);
        assert_eq!(tree.get(b"r_015").unwrap(), None);
        assert_eq!(tree.get(b"r_035").unwrap(), Some(b"w".to_vec()));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
