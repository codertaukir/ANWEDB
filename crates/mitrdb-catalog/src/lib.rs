pub const DEFAULT_BTREE_MAX_KEYS: u16 = 64;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mitrdb_common::{Error, INVALID_LSN, INVALID_PAGE_ID, Lsn, PageId, Result, TxnId};
use mitrdb_storage::{
    BPlusTree, BTreeNode, BufferPoolManager, LogRecord, PAGE_TYPE_BTREE_INTERNAL,
    PAGE_TYPE_BTREE_LEAF, WalManager,
};
pub use mitrdb_storage::{KeyBound, KeyRange};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Text,
    Bool,
    Bytes,
}

impl DataType {
    fn to_u8(self) -> u8 {
        match self {
            DataType::Int64 => 1,
            DataType::Text => 2,
            DataType::Bool => 3,
            DataType::Bytes => 4,
        }
    }

    fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(DataType::Int64),
            2 => Ok(DataType::Text),
            3 => Ok(DataType::Bool),
            4 => Ok(DataType::Bytes),
            _ => Err(Error::PageCorrupted(format!(
                "Invalid catalog column type tag: {}",
                v
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

pub const DROPPED_PREFIX: &str = "__dropped_";

impl ColumnDef {
    /// Logically dropped column: kept in the schema so row positions stay valid.
    pub fn is_dropped(&self) -> bool {
        self.name.starts_with(DROPPED_PREFIX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table_id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page_id: PageId,
}

impl TableSchema {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.table_id.to_le_bytes());
        buf.extend_from_slice(&self.root_page_id.to_le_bytes());
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        for col in &self.columns {
            let cbytes = col.name.as_bytes();
            buf.extend_from_slice(&(cbytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(cbytes);
            buf.push(col.data_type.to_u8());
            buf.push(u8::from(col.nullable));
            buf.push(u8::from(col.is_primary_key));
        }
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(Error::PageCorrupted(
                "TableSchema buffer too short".to_string(),
            ));
        }
        let table_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let root_page_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let name_len = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
        let mut pos = 10;
        if pos + name_len + 2 > buf.len() {
            return Err(Error::PageCorrupted(
                "Truncated TableSchema name".to_string(),
            ));
        }
        let name = String::from_utf8(buf[pos..pos + name_len].to_vec())
            .map_err(|e| Error::PageCorrupted(e.to_string()))?;
        pos += name_len;
        let col_count = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if pos + 2 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated column name len".to_string(),
                ));
            }
            let clen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + clen + 3 > buf.len() {
                return Err(Error::PageCorrupted("Truncated column def".to_string()));
            }
            let cname = String::from_utf8(buf[pos..pos + clen].to_vec())
                .map_err(|e| Error::PageCorrupted(e.to_string()))?;
            pos += clen;
            let data_type = DataType::from_u8(buf[pos])?;
            let nullable = buf[pos + 1] != 0;
            let is_primary_key = buf[pos + 2] != 0;
            pos += 3;
            columns.push(ColumnDef {
                name: cname,
                data_type,
                nullable,
                is_primary_key,
            });
        }
        Ok(TableSchema {
            table_id,
            name,
            columns,
            root_page_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    pub index_id: u32,
    pub name: String,
    pub table_id: u32,
    pub column_name: String,
    pub root_page_id: PageId,
    pub unique: bool,
}

impl IndexMetadata {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.index_id.to_le_bytes());
        buf.extend_from_slice(&self.table_id.to_le_bytes());
        buf.extend_from_slice(&self.root_page_id.to_le_bytes());
        buf.push(u8::from(self.unique));
        let nbytes = self.name.as_bytes();
        buf.extend_from_slice(&(nbytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(nbytes);
        let cbytes = self.column_name.as_bytes();
        buf.extend_from_slice(&(cbytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(cbytes);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 15 {
            return Err(Error::PageCorrupted(
                "IndexMetadata buffer too short".to_string(),
            ));
        }
        let index_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let table_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let root_page_id = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let unique = buf[12] != 0;
        let nlen = u16::from_le_bytes(buf[13..15].try_into().unwrap()) as usize;
        let mut pos = 15;
        if pos + nlen + 2 > buf.len() {
            return Err(Error::PageCorrupted(
                "Truncated IndexMetadata name".to_string(),
            ));
        }
        let name = String::from_utf8(buf[pos..pos + nlen].to_vec())
            .map_err(|e| Error::PageCorrupted(e.to_string()))?;
        pos += nlen;
        let clen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + clen > buf.len() {
            return Err(Error::PageCorrupted(
                "Truncated IndexMetadata column_name".to_string(),
            ));
        }
        let column_name = String::from_utf8(buf[pos..pos + clen].to_vec())
            .map_err(|e| Error::PageCorrupted(e.to_string()))?;
        Ok(IndexMetadata {
            index_id,
            name,
            table_id,
            column_name,
            root_page_id,
            unique,
        })
    }
}

struct CatalogState {
    next_table_id: u32,
    next_index_id: u32,
    next_sys_txn_id: TxnId,
    schemas: HashMap<u32, TableSchema>,
    table_names: HashMap<String, u32>,
    indexes: HashMap<u32, IndexMetadata>,
    table_trees: HashMap<u32, Arc<BPlusTree>>,
    index_trees: HashMap<u32, Arc<BPlusTree>>,
    catalog_tree: Option<Arc<BPlusTree>>,
}

pub struct Catalog {
    pub tables: HashMap<String, u32>,
    state: RwLock<CatalogState>,
    bpm: Option<Arc<BufferPoolManager>>,
    wal: Option<Arc<WalManager>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
            state: RwLock::new(CatalogState {
                next_table_id: 1,
                next_index_id: 1,
                next_sys_txn_id: 1_000_000_000,
                schemas: HashMap::new(),
                table_names: HashMap::new(),
                indexes: HashMap::new(),
                table_trees: HashMap::new(),
                index_trees: HashMap::new(),
                catalog_tree: None,
            }),
            bpm: None,
            wal: None,
        }
    }

    pub fn with_storage(bpm: Arc<BufferPoolManager>, wal: Option<Arc<WalManager>>) -> Self {
        let initial_sys_tid = wal
            .as_ref()
            .and_then(|w| w.read_all_records().ok())
            .and_then(|records| records.iter().map(|r| r.txn_id).max())
            .unwrap_or(0)
            .max(wal.as_ref().map(|w| w.max_sys_txn_id()).unwrap_or(0))
            .max(1_000_000_000)
            + 1;

        let mut state = CatalogState {
            next_table_id: 1,
            next_index_id: 1,
            next_sys_txn_id: initial_sys_tid,
            schemas: HashMap::new(),
            table_names: HashMap::new(),
            indexes: HashMap::new(),
            table_trees: HashMap::new(),
            index_trees: HashMap::new(),
            catalog_tree: None,
        };

        if let Ok(page0) = bpm.fetch_page(0) {
            let ptype = page0.page_type();
            let _ = bpm.unpin_page(0, false);
            if ptype == PAGE_TYPE_BTREE_LEAF || ptype == PAGE_TYPE_BTREE_INTERNAL {
                let cat_root = Self::resolve_root_page(&bpm, 0);
                if let Ok(cat_tree) = BPlusTree::open(bpm.clone(), wal.clone(), cat_root, DEFAULT_BTREE_MAX_KEYS) {
                    let cat_arc = Arc::new(cat_tree);
                    if let Ok(entries) = cat_arc.range_scan(None, None) {
                        for (k, v) in entries {
                            if k.starts_with(b"tbl:")
                                && let Ok(mut schema) = TableSchema::deserialize(&v)
                            {
                                if schema.table_id >= state.next_table_id {
                                    state.next_table_id = schema.table_id + 1;
                                }
                                schema.root_page_id =
                                    Self::resolve_root_page(&bpm, schema.root_page_id);
                                if let Ok(tree) = BPlusTree::open(
                                    bpm.clone(),
                                    wal.clone(),
                                    schema.root_page_id,
                                    DEFAULT_BTREE_MAX_KEYS,
                                ) {
                                    state.table_trees.insert(schema.table_id, Arc::new(tree));
                                }
                                state
                                    .table_names
                                    .insert(schema.name.clone(), schema.table_id);
                                state.schemas.insert(schema.table_id, schema);
                            } else if k.starts_with(b"idx:")
                                && let Ok(mut meta) = IndexMetadata::deserialize(&v)
                            {
                                if meta.index_id >= state.next_index_id {
                                    state.next_index_id = meta.index_id + 1;
                                }
                                meta.root_page_id =
                                    Self::resolve_root_page(&bpm, meta.root_page_id);
                                if let Ok(tree) =
                                    BPlusTree::open(bpm.clone(), wal.clone(), meta.root_page_id, DEFAULT_BTREE_MAX_KEYS)
                                {
                                    state.index_trees.insert(meta.index_id, Arc::new(tree));
                                }
                                state.indexes.insert(meta.index_id, meta);
                            }
                        }
                    }
                    state.catalog_tree = Some(cat_arc);
                }
            }
        } else {
            let sys_tid = if wal.is_some() {
                let tid = state.next_sys_txn_id;
                state.next_sys_txn_id += 1;
                tid
            } else {
                0
            };
            let mut prev_lsn: Lsn = if let Some(ref w) = wal {
                w.append_record(LogRecord::new_begin(0, sys_tid))
                    .unwrap_or(INVALID_LSN)
            } else {
                INVALID_LSN
            };
            if let Ok(cat_tree) =
                BPlusTree::create_logged(bpm.clone(), wal.clone(), DEFAULT_BTREE_MAX_KEYS, sys_tid, &mut prev_lsn)
                && cat_tree.root_page_id() == 0
            {
                if let Some(ref w) = wal {
                    let _ = w.commit(sys_tid, prev_lsn);
                } else {
                    let _ = bpm.flush_all();
                }
                state.catalog_tree = Some(Arc::new(cat_tree));
            } else if let Some(ref w) = wal
                && sys_tid != 0
            {
                let recovery =
                    mitrdb_storage::recovery::RecoveryManager::new(w.clone(), bpm.clone());
                let _ = recovery.rollback_txn(sys_tid);
            }
        }

        Catalog {
            tables: HashMap::new(),
            state: RwLock::new(state),
            bpm: Some(bpm),
            wal,
        }
    }

    fn resolve_root_page(bpm: &BufferPoolManager, start_page_id: PageId) -> PageId {
        let mut root = start_page_id;
        let mut hops = 0usize;
        while let Ok(page) = bpm.fetch_page(root) {
            let node_res = BTreeNode::from_page(&page);
            let _ = bpm.unpin_page(root, false);
            match node_res {
                Ok(node) if node.parent_page_id != INVALID_PAGE_ID && hops < 1024 => {
                    root = node.parent_page_id;
                    hops += 1;
                }
                _ => break,
            }
        }
        root
    }

    fn begin_sys_txn(&self, state: &mut CatalogState) -> Result<(TxnId, Lsn)> {
        if let Some(ref wal) = self.wal {
            let sys_tid = state.next_sys_txn_id;
            state.next_sys_txn_id += 1;
            let lsn = wal.append_record(LogRecord::new_begin(0, sys_tid))?;
            Ok((sys_tid, lsn))
        } else {
            Ok((0, INVALID_LSN))
        }
    }

    fn finish_sys_txn(&self, sys_tid: TxnId, last_lsn: Lsn) -> Result<()> {
        if let Some(ref wal) = self.wal {
            if sys_tid != 0 {
                wal.commit(sys_tid, last_lsn)?;
                if let Some(ref bpm) = self.bpm {
                    let _ = bpm.finalize_free_pages(sys_tid);
                }
            }
        } else if let Some(ref bpm) = self.bpm {
            bpm.flush_all()?;
        }
        Ok(())
    }

    fn abort_sys_txn(&self, state: &CatalogState, sys_tid: TxnId, last_lsn: Lsn) {
        if let Some(ref wal) = self.wal {
            if sys_tid != 0 {
                if let Some(ref bpm) = self.bpm {
                    let recovery =
                        mitrdb_storage::recovery::RecoveryManager::new(wal.clone(), bpm.clone());
                    let _ = recovery.rollback_txn(sys_tid);
                    if let Some(ref cat_tree) = state.catalog_tree {
                        cat_tree.set_root_page_id(Self::resolve_root_page(bpm, 0));
                    }
                } else {
                    let _ = wal.abort(sys_tid, last_lsn);
                }
            }
        } else if let Some(ref bpm) = self.bpm {
            let _ = bpm.finalize_free_pages(sys_tid);
        }
    }

    pub fn get_table_id(&self, name: &str) -> Result<Option<u32>> {
        if let Some(&id) = self.tables.get(name) {
            return Ok(Some(id));
        }
        let state = self.state.read().unwrap();
        Ok(state.table_names.get(name).copied())
    }

    pub fn create_table(&self, name: &str, columns: Vec<ColumnDef>) -> Result<u32> {
        if columns.is_empty() {
            return Err(Error::TransactionError(
                "Table must have at least one column".to_string(),
            ));
        }
        let mut state = self.state.write().unwrap();
        if state.table_names.contains_key(name) || self.tables.contains_key(name) {
            return Err(Error::TransactionError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let table_id = state.next_table_id;
        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;

        let op_res: Result<(PageId, Option<Arc<BPlusTree>>, TableSchema)> = (|| {
            let (root_page_id, tree_opt) = if let Some(ref bpm) = self.bpm {
                let tree = Arc::new(BPlusTree::create_logged(
                    bpm.clone(),
                    self.wal.clone(),
                    DEFAULT_BTREE_MAX_KEYS,
                    sys_tid,
                    &mut prev_lsn,
                )?);
                (tree.root_page_id(), Some(tree))
            } else {
                (INVALID_PAGE_ID, None)
            };

            let schema = TableSchema {
                table_id,
                name: name.to_string(),
                columns,
                root_page_id,
            };

            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &schema.serialize(),
                )?;
            }

            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok((root_page_id, tree_opt, schema))
        })();

        match op_res {
            Ok((_root_page_id, tree_opt, schema)) => {
                state.next_table_id = table_id + 1;
                state.table_names.insert(name.to_string(), table_id);
                state.schemas.insert(table_id, schema);
                if let Some(tree) = tree_opt {
                    state.table_trees.insert(table_id, tree);
                }
                Ok(table_id)
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn drop_table(&self, name: &str) -> Result<bool> {
        let mut state = self.state.write().unwrap();
        if let Some(&table_id) = state.table_names.get(name) {
            let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
            let idx_ids: Vec<u32> = state
                .indexes
                .values()
                .filter(|idx| idx.table_id == table_id)
                .map(|idx| idx.index_id)
                .collect();
            let table_tree = state.table_trees.get(&table_id).cloned();
            let dropped_idx_trees: Vec<Arc<BPlusTree>> = idx_ids
                .iter()
                .filter_map(|id| state.index_trees.get(id).cloned())
                .collect();

            let op_res: Result<()> = (|| {
                if let Some(ref cat_tree) = state.catalog_tree {
                    let key = format!("tbl:{:08}", table_id);
                    let _ = cat_tree.delete_logged(sys_tid, &mut prev_lsn, key.as_bytes())?;
                    for id in &idx_ids {
                        let ikey = format!("idx:{:08}", id);
                        let _ = cat_tree.delete_logged(sys_tid, &mut prev_lsn, ikey.as_bytes())?;
                    }
                }
                if let Some(ref tree) = table_tree {
                    tree.free_all_pages_logged(sys_tid, &mut prev_lsn)?;
                }
                for itree in &dropped_idx_trees {
                    itree.free_all_pages_logged(sys_tid, &mut prev_lsn)?;
                }
                self.finish_sys_txn(sys_tid, prev_lsn)?;
                Ok(())
            })();

            match op_res {
                Ok(()) => {
                    state.table_names.remove(name);
                    state.schemas.remove(&table_id);
                    state.table_trees.remove(&table_id);
                    for id in idx_ids {
                        state.indexes.remove(&id);
                        state.index_trees.remove(&id);
                    }
                    drop(state);
                    let _ = self.sync_roots();
                    Ok(true)
                }
                Err(e) => {
                    self.abort_sys_txn(&state, sys_tid, prev_lsn);
                    Err(e)
                }
            }
        } else {
            Ok(false)
        }
    }

    pub fn get_table_schema(&self, name: &str) -> Result<Option<TableSchema>> {
        let state = self.state.read().unwrap();
        if let Some(&id) = state.table_names.get(name) {
            Ok(state.schemas.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    pub fn get_table_schema_by_id(&self, table_id: u32) -> Result<Option<TableSchema>> {
        let state = self.state.read().unwrap();
        Ok(state.schemas.get(&table_id).cloned())
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
        unique: bool,
    ) -> Result<u32> {
        let mut state = self.state.write().unwrap();
        let &table_id = state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;
        let schema = state.schemas.get(&table_id).unwrap();
        if !schema.columns.iter().any(|c| c.name == column_name) {
            return Err(Error::TransactionError(format!(
                "Column '{}' not found in table '{}'",
                column_name, table_name
            )));
        }
        if state.indexes.values().any(|idx| idx.name == index_name) {
            return Err(Error::TransactionError(format!(
                "Index '{}' already exists",
                index_name
            )));
        }

        let index_id = state.next_index_id;
        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;

        let op_res: Result<(Option<Arc<BPlusTree>>, IndexMetadata)> = (|| {
            let (root_page_id, tree_opt) = if let Some(ref bpm) = self.bpm {
                let tree = Arc::new(BPlusTree::create_logged(
                    bpm.clone(),
                    self.wal.clone(),
                    DEFAULT_BTREE_MAX_KEYS,
                    sys_tid,
                    &mut prev_lsn,
                )?);
                (tree.root_page_id(), Some(tree))
            } else {
                (INVALID_PAGE_ID, None)
            };

            let meta = IndexMetadata {
                index_id,
                name: index_name.to_string(),
                table_id,
                column_name: column_name.to_string(),
                root_page_id,
                unique,
            };

            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("idx:{:08}", index_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &meta.serialize(),
                )?;
            }

            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok((tree_opt, meta))
        })();

        match op_res {
            Ok((tree_opt, meta)) => {
                state.next_index_id = index_id + 1;
                state.indexes.insert(index_id, meta);
                if let Some(tree) = tree_opt {
                    state.index_trees.insert(index_id, tree);
                }
                Ok(index_id)
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn populate_index_entries(
        &self,
        index_id: u32,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut state = self.state.write().unwrap();
        let idx_tree = state.index_trees.get(&index_id).cloned().ok_or_else(|| {
            Error::TransactionError(format!("Index B+Tree {} not initialized", index_id))
        })?;
        let orig_root = idx_tree.root_page_id();
        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<PageId> = (|| {
            for (k, v) in entries {
                idx_tree.insert_logged(sys_tid, &mut prev_lsn, k, v)?;
            }
            let new_root = idx_tree.root_page_id();
            if let Some(meta) = state.indexes.get(&index_id)
                && meta.root_page_id != new_root
            {
                let mut updated = meta.clone();
                updated.root_page_id = new_root;
                if let Some(ref cat_tree) = state.catalog_tree {
                    let key = format!("idx:{:08}", index_id);
                    cat_tree.insert_logged(
                        sys_tid,
                        &mut prev_lsn,
                        key.as_bytes(),
                        &updated.serialize(),
                    )?;
                }
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(new_root)
        })();

        match op_res {
            Ok(new_root) => {
                if let Some(meta) = state.indexes.get_mut(&index_id) {
                    meta.root_page_id = new_root;
                }
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                idx_tree.set_root_page_id(orig_root);
                Err(e)
            }
        }
    }

    pub fn get_index_by_name(&self, index_name: &str) -> Option<IndexMetadata> {
        let state = self.state.read().unwrap();
        state
            .indexes
            .values()
            .find(|idx| idx.name == index_name)
            .cloned()
    }

    pub fn drop_index(&self, index_name: &str) -> Result<bool> {
        let mut state = self.state.write().unwrap();
        let idx_opt = state
            .indexes
            .values()
            .find(|idx| idx.name == index_name)
            .map(|idx| idx.index_id);
        if let Some(index_id) = idx_opt {
            let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
            let itree = state.index_trees.get(&index_id).cloned();
            let op_res: Result<()> = (|| {
                if let Some(ref cat_tree) = state.catalog_tree {
                    let key = format!("idx:{:08}", index_id);
                    let _ = cat_tree.delete_logged(sys_tid, &mut prev_lsn, key.as_bytes())?;
                }
                if let Some(ref tree) = itree {
                    tree.free_all_pages_logged(sys_tid, &mut prev_lsn)?;
                }
                self.finish_sys_txn(sys_tid, prev_lsn)?;
                Ok(())
            })();

            match op_res {
                Ok(()) => {
                    state.indexes.remove(&index_id);
                    state.index_trees.remove(&index_id);
                    drop(state);
                    let _ = self.sync_roots();
                    Ok(true)
                }
                Err(e) => {
                    self.abort_sys_txn(&state, sys_tid, prev_lsn);
                    Err(e)
                }
            }
        } else {
            Err(Error::TransactionError(format!(
                "Index '{}' does not exist",
                index_name
            )))
        }
    }

    pub fn add_column(&self, table_name: &str, column: ColumnDef) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let table_id = *state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;

        let schema = state.schemas.get(&table_id).unwrap();
        if schema
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(Error::TransactionError(format!(
                "Column '{}' already exists in table '{}'",
                column.name, table_name
            )));
        }
        if column.is_primary_key {
            return Err(Error::TransactionError(
                "Cannot add primary key column via ALTER TABLE".to_string(),
            ));
        }

        if !column.nullable
            && let Some(table_tree) = state.table_trees.get(&table_id)
        {
            let has_rows = !table_tree.range_scan(None, None)?.is_empty();
            if has_rows {
                return Err(Error::TransactionError(
                    "Cannot add NOT NULL column without a DEFAULT value to non-empty table"
                        .to_string(),
                ));
            }
        }

        let mut updated_schema = schema.clone();
        updated_schema.columns.push(column);

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &updated_schema.serialize(),
                )?;
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();

        match op_res {
            Ok(()) => {
                state.schemas.insert(table_id, updated_schema);
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let table_id = *state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;

        let schema = state.schemas.get(&table_id).unwrap();
        let col_pos = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(old_name))
            .ok_or_else(|| {
                Error::TransactionError(format!(
                    "Column '{}' not found in table '{}'",
                    old_name, table_name
                ))
            })?;

        if schema.columns[col_pos].is_dropped() {
            return Err(Error::TransactionError(format!(
                "Column '{}' not found in table '{}'",
                old_name, table_name
            )));
        }

        if schema
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(new_name))
        {
            return Err(Error::TransactionError(format!(
                "Column '{}' already exists in table '{}'",
                new_name, table_name
            )));
        }

        let mut updated_schema = schema.clone();
        updated_schema.columns[col_pos].name = new_name.to_string();

        let matching_indexes: Vec<IndexMetadata> = state
            .indexes
            .values()
            .filter(|idx| {
                idx.table_id == table_id && idx.column_name.eq_ignore_ascii_case(old_name)
            })
            .map(|idx| {
                let mut m = idx.clone();
                m.column_name = new_name.to_string();
                m
            })
            .collect();

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &updated_schema.serialize(),
                )?;
                for idx_meta in &matching_indexes {
                    let ikey = format!("idx:{:08}", idx_meta.index_id);
                    cat_tree.insert_logged(
                        sys_tid,
                        &mut prev_lsn,
                        ikey.as_bytes(),
                        &idx_meta.serialize(),
                    )?;
                }
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();

        match op_res {
            Ok(()) => {
                state.schemas.insert(table_id, updated_schema);
                for idx_meta in matching_indexes {
                    state.indexes.insert(idx_meta.index_id, idx_meta);
                }
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    /// Remove the NOT NULL constraint from a column (catalog flag only, rows untouched).
    pub fn set_nullable(&self, table_name: &str, col_name: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let table_id = *state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;
        let schema = state.schemas.get(&table_id).unwrap();
        let col_pos = schema
            .columns
            .iter()
            .position(|c| !c.is_dropped() && c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                Error::TransactionError(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        if schema.columns[col_pos].is_primary_key {
            return Err(Error::TransactionError(
                "Cannot make primary key column nullable".to_string(),
            ));
        }
        if schema.columns[col_pos].nullable {
            return Ok(());
        }
        let mut updated_schema = schema.clone();
        updated_schema.columns[col_pos].nullable = true;

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &updated_schema.serialize(),
                )?;
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();
        match op_res {
            Ok(()) => {
                state.schemas.insert(table_id, updated_schema);
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    /// Add the NOT NULL constraint to a column (catalog flag only). The caller must have
    /// verified that no row holds NULL in it; rows are not touched here.
    pub fn set_not_null(&self, table_name: &str, col_name: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let table_id = *state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;
        let schema = state.schemas.get(&table_id).unwrap();
        let col_pos = schema
            .columns
            .iter()
            .position(|c| !c.is_dropped() && c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                Error::TransactionError(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        if schema.columns[col_pos].is_primary_key || !schema.columns[col_pos].nullable {
            return Ok(());
        }
        let mut updated_schema = schema.clone();
        updated_schema.columns[col_pos].nullable = false;

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &updated_schema.serialize(),
                )?;
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();
        match op_res {
            Ok(()) => {
                state.schemas.insert(table_id, updated_schema);
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn drop_column(&self, table_name: &str, col_name: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let table_id = *state.table_names.get(table_name).ok_or_else(|| {
            Error::TransactionError(format!("Table '{}' does not exist", table_name))
        })?;
        let schema = state.schemas.get(&table_id).unwrap();
        let col_pos = schema
            .columns
            .iter()
            .position(|c| !c.is_dropped() && c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                Error::TransactionError(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        if schema.columns[col_pos].is_primary_key {
            return Err(Error::TransactionError(
                "Cannot drop primary key column".to_string(),
            ));
        }
        if state
            .indexes
            .values()
            .any(|i| i.table_id == table_id && i.column_name.eq_ignore_ascii_case(col_name))
        {
            return Err(Error::TransactionError(format!(
                "Column '{}' has an index; DROP INDEX first",
                col_name
            )));
        }
        if schema.columns.iter().filter(|c| !c.is_dropped()).count() <= 1 {
            return Err(Error::TransactionError(
                "Cannot drop the last column of a table".to_string(),
            ));
        }
        let mut updated_schema = schema.clone();
        updated_schema.columns[col_pos].name = format!("{}{}", DROPPED_PREFIX, col_pos);
        updated_schema.columns[col_pos].nullable = true;

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            if let Some(ref cat_tree) = state.catalog_tree {
                let key = format!("tbl:{:08}", table_id);
                cat_tree.insert_logged(
                    sys_tid,
                    &mut prev_lsn,
                    key.as_bytes(),
                    &updated_schema.serialize(),
                )?;
            }
            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();
        match op_res {
            Ok(()) => {
                state.schemas.insert(table_id, updated_schema);
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn sync_roots(&self) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let mut updated_tables = Vec::new();
        for (&tid, tree) in &state.table_trees {
            let current_root = tree.root_page_id();
            if let Some(schema) = state.schemas.get(&tid)
                && schema.root_page_id != current_root
            {
                updated_tables.push((tid, current_root));
            }
        }

        let mut updated_indexes = Vec::new();
        for (&iid, tree) in &state.index_trees {
            let current_root = tree.root_page_id();
            if let Some(meta) = state.indexes.get(&iid)
                && meta.root_page_id != current_root
            {
                updated_indexes.push((iid, current_root));
            }
        }

        if updated_tables.is_empty()
            && updated_indexes.is_empty()
        {
            return Ok(());
        }

        let (sys_tid, mut prev_lsn) = self.begin_sys_txn(&mut state)?;
        let op_res: Result<()> = (|| {
            for &(tid, new_root) in &updated_tables {
                if let Some(schema) = state.schemas.get(&tid) {
                    let mut updated = schema.clone();
                    updated.root_page_id = new_root;
                    if let Some(ref cat_tree) = state.catalog_tree {
                        let key = format!("tbl:{:08}", tid);
                        cat_tree.insert_logged(
                            sys_tid,
                            &mut prev_lsn,
                            key.as_bytes(),
                            &updated.serialize(),
                        )?;
                    }
                }
            }

            for &(iid, new_root) in &updated_indexes {
                if let Some(meta) = state.indexes.get(&iid) {
                    let mut updated = meta.clone();
                    updated.root_page_id = new_root;
                    if let Some(ref cat_tree) = state.catalog_tree {
                        let key = format!("idx:{:08}", iid);
                        cat_tree.insert_logged(
                            sys_tid,
                            &mut prev_lsn,
                            key.as_bytes(),
                            &updated.serialize(),
                        )?;
                    }
                }
            }

            self.finish_sys_txn(sys_tid, prev_lsn)?;
            Ok(())
        })();

        match op_res {
            Ok(()) => {
                for (tid, new_root) in updated_tables {
                    if let Some(schema) = state.schemas.get_mut(&tid) {
                        schema.root_page_id = new_root;
                    }
                }
                for (iid, new_root) in updated_indexes {
                    if let Some(meta) = state.indexes.get_mut(&iid) {
                        meta.root_page_id = new_root;
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.abort_sys_txn(&state, sys_tid, prev_lsn);
                Err(e)
            }
        }
    }

    pub fn get_table_indexes(&self, table_id: u32) -> Vec<IndexMetadata> {
        let state = self.state.read().unwrap();
        state
            .indexes
            .values()
            .filter(|idx| idx.table_id == table_id)
            .cloned()
            .collect()
    }

    pub fn table_tree(&self, table_id: u32) -> Option<Arc<BPlusTree>> {
        let state = self.state.read().unwrap();
        state.table_trees.get(&table_id).cloned()
    }

    pub fn index_tree(&self, index_id: u32) -> Option<Arc<BPlusTree>> {
        let state = self.state.read().unwrap();
        state.index_trees.get(&index_id).cloned()
    }

}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
