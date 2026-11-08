use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

use mitrdb_common::{Error, Result, TxnId};
use mitrdb_query::{
    AggregateFunc, BoundAggregate, BoundFilter, BoundGroupOrderBy, BoundHavingPredicate,
    BoundPredicate, BoundSelectItem, Catalog, ColumnDef, CompareOp, DataType, JoinProjSource,
    JoinType, PhysicalPlan, QueryEngine, TableSchema, Value,
};
use mitrdb_txn::{IsolationLevel, KeyBound, KeyRange, TxnManager};

const MULTI_PK_TAG: u8 = 0xFF;
/// Index head value whose PK list continues in overflow keys: [tag][n_overflow u16 LE][first chunk].
const CHUNKED_PK_TAG: u8 = 0xFD;
/// Overflow key prefix: [tag] + head_key + [chunk_no u16 BE]. Real key tags are 0..=4.
const OVERFLOW_KEY_TAG: u8 = 0xFE;
const IDX_CHUNK_BUDGET: usize = 2048;
/// Bulk DELETE/UPDATE: flush the WAL arena mid-statement once it holds more than this many bytes.
const WAL_PRESSURE_LIMIT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult {
    Ok,
    Affected(usize),
    Explained(String),
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

struct TopNCandidate<'a> {
    tuple: Vec<Value>,
    sort_keys: &'a [(usize, bool)],
    pk_idx: usize,
}

impl PartialEq for TopNCandidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        Executor::cmp_tuples(&self.tuple, &other.tuple, self.sort_keys, self.pk_idx)
            == Ordering::Equal
    }
}

impl Eq for TopNCandidate<'_> {}

impl PartialOrd for TopNCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        Executor::cmp_tuples(&self.tuple, &other.tuple, self.sort_keys, self.pk_idx)
    }
}

enum AggAccumulator {
    CountStar(i64),
    Count {
        col_idx: usize,
        count: i64,
    },
    Sum {
        col_idx: usize,
        sum: Option<i64>,
    },
    Avg {
        col_idx: usize,
        sum: i128,
        count: i128,
    },
    Min {
        col_idx: usize,
        val: Option<Value>,
    },
    Max {
        col_idx: usize,
        val: Option<Value>,
    },
}

impl AggAccumulator {
    fn new(agg: &BoundAggregate) -> Self {
        match agg.func {
            AggregateFunc::CountStar => AggAccumulator::CountStar(0),
            AggregateFunc::Count => AggAccumulator::Count {
                col_idx: agg.column_idx.unwrap(),
                count: 0,
            },
            AggregateFunc::Sum => AggAccumulator::Sum {
                col_idx: agg.column_idx.unwrap(),
                sum: None,
            },
            AggregateFunc::Avg => AggAccumulator::Avg {
                col_idx: agg.column_idx.unwrap(),
                sum: 0,
                count: 0,
            },
            AggregateFunc::Min => AggAccumulator::Min {
                col_idx: agg.column_idx.unwrap(),
                val: None,
            },
            AggregateFunc::Max => AggAccumulator::Max {
                col_idx: agg.column_idx.unwrap(),
                val: None,
            },
        }
    }

    fn update(&mut self, row: &[Value]) -> Result<()> {
        match self {
            AggAccumulator::CountStar(c) => {
                *c += 1;
            }
            AggAccumulator::Count { col_idx, count } => {
                if !matches!(row[*col_idx], Value::Null) {
                    *count += 1;
                }
            }
            AggAccumulator::Sum { col_idx, sum } => match &row[*col_idx] {
                Value::Null => {}
                Value::Int64(v) => {
                    *sum = Some(match *sum {
                        None => *v,
                        Some(acc) => acc.checked_add(*v).ok_or_else(|| {
                            Error::TransactionError(
                                "Integer overflow in SUM aggregation".to_string(),
                            )
                        })?,
                    });
                }
                other => {
                    return Err(Error::TransactionError(format!(
                        "Invalid non-integer value {:?} in SUM aggregation",
                        other
                    )));
                }
            },
            AggAccumulator::Avg {
                col_idx,
                sum,
                count,
            } => match &row[*col_idx] {
                Value::Null => {}
                Value::Int64(v) => {
                    *sum += i128::from(*v);
                    *count += 1;
                }
                other => {
                    return Err(Error::TransactionError(format!(
                        "Invalid non-integer value {:?} in AVG aggregation",
                        other
                    )));
                }
            },
            AggAccumulator::Min { col_idx, val } => {
                let v = &row[*col_idx];
                if !matches!(v, Value::Null) {
                    *val = Some(match val.take() {
                        None => v.clone(),
                        Some(cur) => {
                            if v < &cur {
                                v.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            AggAccumulator::Max { col_idx, val } => {
                let v = &row[*col_idx];
                if !matches!(v, Value::Null) {
                    *val = Some(match val.take() {
                        None => v.clone(),
                        Some(cur) => {
                            if v > &cur {
                                v.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    fn finalize(self) -> Result<Value> {
        let v = match self {
            AggAccumulator::CountStar(c) => Value::Int64(c),
            AggAccumulator::Count { count, .. } => Value::Int64(count),
            AggAccumulator::Sum { sum, .. } => sum.map_or(Value::Null, Value::Int64),
            AggAccumulator::Avg { sum, count, .. } => {
                if count == 0 {
                    Value::Null
                } else {
                    let avg_i128 = sum / count;
                    let avg_i64 = i64::try_from(avg_i128).map_err(|_| {
                        Error::TransactionError("Integer overflow in AVG aggregation".to_string())
                    })?;
                    Value::Int64(avg_i64)
                }
            }
            AggAccumulator::Min { val, .. } => val.unwrap_or(Value::Null),
            AggAccumulator::Max { val, .. } => val.unwrap_or(Value::Null),
        };
        Ok(v)
    }
}

pub struct Tuple;

impl Tuple {
    pub fn serialize(values: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
        for v in values {
            match v {
                Value::Null => buf.push(0),
                Value::Int64(n) => {
                    buf.push(1);
                    buf.extend_from_slice(&n.to_le_bytes());
                }
                Value::Text(s) => {
                    buf.push(2);
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                Value::Bool(b) => {
                    buf.push(3);
                    buf.push(u8::from(*b));
                }
                Value::Bytes(b) => {
                    buf.push(4);
                    buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
                    buf.extend_from_slice(b);
                }
                Value::Param(_) => unreachable!("unbound parameter reached the row codec"),
            }
        }
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Vec<Value>> {
        if buf.len() < 2 {
            return Err(Error::PageCorrupted("Tuple buffer too short".to_string()));
        }
        let count = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
        let mut pos = 2;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            if pos >= buf.len() {
                return Err(Error::PageCorrupted("Truncated tuple tag".to_string()));
            }
            let tag = buf[pos];
            pos += 1;
            match tag {
                0 => values.push(Value::Null),
                1 => {
                    if pos + 8 > buf.len() {
                        return Err(Error::PageCorrupted("Truncated Int64 in tuple".to_string()));
                    }
                    let n = i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    values.push(Value::Int64(n));
                }
                2 => {
                    if pos + 2 > buf.len() {
                        return Err(Error::PageCorrupted(
                            "Truncated Text length in tuple".to_string(),
                        ));
                    }
                    let len = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    if pos + len > buf.len() {
                        return Err(Error::PageCorrupted("Truncated Text in tuple".to_string()));
                    }
                    let s = String::from_utf8(buf[pos..pos + len].to_vec())
                        .map_err(|e| Error::PageCorrupted(e.to_string()))?;
                    pos += len;
                    values.push(Value::Text(s));
                }
                3 => {
                    if pos >= buf.len() {
                        return Err(Error::PageCorrupted("Truncated Bool in tuple".to_string()));
                    }
                    values.push(Value::Bool(buf[pos] != 0));
                    pos += 1;
                }
                4 => {
                    if pos + 2 > buf.len() {
                        return Err(Error::PageCorrupted(
                            "Truncated Bytes length in tuple".to_string(),
                        ));
                    }
                    let len = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    if pos + len > buf.len() {
                        return Err(Error::PageCorrupted("Truncated Bytes in tuple".to_string()));
                    }
                    values.push(Value::Bytes(buf[pos..pos + len].to_vec()));
                    pos += len;
                }
                other => {
                    return Err(Error::PageCorrupted(format!(
                        "Unknown tuple value tag: {}",
                        other
                    )));
                }
            }
        }
        Ok(values)
    }

    pub fn deserialize_for_table(buf: &[u8], expected_cols: usize) -> Result<Vec<Value>> {
        let mut values = Self::deserialize(buf)?;
        if values.len() < expected_cols {
            values.resize(expected_cols, Value::Null);
        }
        Ok(values)
    }
}

pub struct Executor;

impl Executor {
    pub const fn new() -> Self {
        Executor
    }

    pub fn execute(&self) -> Result<()> {
        Ok(())
    }

    pub fn execute_plan(
        &self,
        plan: PhysicalPlan,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        active_txn: &mut Option<TxnId>,
    ) -> Result<ExecutionResult> {
        self.execute_plan_with_isolation(
            plan,
            catalog,
            txn_mgr,
            active_txn,
            IsolationLevel::SnapshotIsolation,
        )
    }

    fn format_explain_plan(plan: &PhysicalPlan) -> String {
        match plan {
            PhysicalPlan::IndexScan {
                schema,
                index,
                top_n,
                ..
            } => {
                if *top_n {
                    format!(
                        "IndexScan(table={}, index={}, column={}, top_n=true)",
                        schema.name, index.name, index.column_name
                    )
                } else {
                    format!(
                        "IndexScan(table={}, index={}, column={})",
                        schema.name, index.name, index.column_name
                    )
                }
            }
            PhysicalPlan::IndexRangeScan {
                schema,
                index,
                ranges,
                top_n,
                ..
            } => {
                if *top_n {
                    format!(
                        "IndexRangeScan(table={}, index={}, column={}, ranges={}, top_n=true)",
                        schema.name,
                        index.name,
                        index.column_name,
                        ranges.len()
                    )
                } else {
                    format!(
                        "IndexRangeScan(table={}, index={}, column={}, ranges={})",
                        schema.name,
                        index.name,
                        index.column_name,
                        ranges.len()
                    )
                }
            }
            PhysicalPlan::IndexOrderedScan {
                schema,
                index,
                ranges,
                descending,
                limit,
                offset,
                ..
            } => format!(
                "IndexOrderedScan(table={}, index={}, column={}, dir={}, ranges={}, limit={:?}, offset={})",
                schema.name,
                index.name,
                index.column_name,
                if *descending { "DESC" } else { "ASC" },
                ranges.len(),
                limit,
                offset
            ),
            PhysicalPlan::SeqScan { schema, top_n, .. } => {
                if *top_n {
                    format!("SeqScan(table={}, top_n=true)", schema.name)
                } else {
                    format!("SeqScan(table={})", schema.name)
                }
            }
            PhysicalPlan::Aggregate {
                input, aggregates, ..
            } => {
                let funcs: Vec<&str> = aggregates.iter().map(|a| a.output_name.as_str()).collect();
                format!(
                    "Aggregate(funcs=[{}], input={})",
                    funcs.join(", "),
                    Self::format_explain_plan(input)
                )
            }
            PhysicalPlan::GroupAggregate {
                input,
                group_by,
                aggregates,
                having,
                order_by,
                limit,
                ..
            } => {
                let funcs: Vec<&str> = aggregates.iter().map(|a| a.output_name.as_str()).collect();
                format!(
                    "GroupAggregate(group_cols={:?}, funcs=[{}], having={}, top_n={}, input={})",
                    group_by,
                    funcs.join(", "),
                    having.is_some(),
                    !order_by.is_empty() && limit.is_some(),
                    Self::format_explain_plan(input)
                )
            }
            PhysicalPlan::Distinct {
                input,
                limit,
                offset,
            } => {
                format!(
                    "Distinct(limit={:?}, offset={}, input={})",
                    limit,
                    offset,
                    Self::format_explain_plan(input)
                )
            }
            PhysicalPlan::Union {
                left,
                right,
                all,
                limit,
                offset,
                ..
            } => {
                format!(
                    "Union(all={}, limit={:?}, offset={}, left={}, right={})",
                    all,
                    limit,
                    offset,
                    Self::format_explain_plan(left),
                    Self::format_explain_plan(right)
                )
            }
            PhysicalPlan::NestedLoopJoin {
                join_type,
                left_schema,
                right_schema,
                left,
                right,
                limit,
                offset,
                ..
            } => {
                format!(
                    "NestedLoopJoin(type={:?}, left={}, right={}, limit={:?}, offset={}, left_input={}, right_input={})",
                    join_type,
                    left_schema.name,
                    right_schema.name,
                    limit,
                    offset,
                    Self::format_explain_plan(left),
                    Self::format_explain_plan(right)
                )
            }
            PhysicalPlan::AlterTableAddColumn { table_name, column } => {
                format!(
                    "AlterTableAddColumn(table={}, column={})",
                    table_name, column.name
                )
            }
            PhysicalPlan::AlterTableRenameColumn {
                table_name,
                old_name,
                new_name,
            } => {
                format!(
                    "AlterTableRenameColumn(table={}, old={}, new={})",
                    table_name, old_name, new_name
                )
            }
            other => format!("{:?}", other),
        }
    }

    fn idx_overflow_key(head: &[u8], n: u16) -> Vec<u8> {
        let mut k = Vec::with_capacity(head.len() + 3);
        k.push(OVERFLOW_KEY_TAG);
        k.extend_from_slice(head);
        k.extend_from_slice(&n.to_be_bytes());
        k
    }

    fn idx_split_chunks(key_len: usize, pks: &[Vec<u8>]) -> Vec<(usize, usize)> {
        let budget = IDX_CHUNK_BUDGET.saturating_sub(key_len + 64).max(256);
        let mut out = Vec::new();
        let mut start = 0usize;
        let mut size = 3usize;
        for (i, pk) in pks.iter().enumerate() {
            let add = 2 + pk.len();
            if i > start && size + add > budget {
                out.push((start, i));
                start = i;
                size = 3;
            }
            size += add;
        }
        if start < pks.len() {
            out.push((start, pks.len()));
        }
        out
    }

    /// Entries (head first, then overflow chunks) for one index key.
    fn idx_encode_entries(key: &[u8], pks: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let plain = Self::encode_index_pks(pks);
        if plain.len() + 3 <= IDX_CHUNK_BUDGET.saturating_sub(key.len() + 64).max(256) {
        return Ok(vec![(key.to_vec(), plain)]);
        }
        let chunks = Self::idx_split_chunks(key.len(), pks);
        if chunks.len() <= 1 {
        return Ok(vec![(key.to_vec(), plain)]);
        }
        if chunks.len() - 1 > u16::MAX as usize {
            return Err(Error::OutOfSpace);
        }
        let mut out = Vec::with_capacity(chunks.len());
        let (s0, e0) = chunks[0];
        let mut head = vec![CHUNKED_PK_TAG];
        head.extend_from_slice(&((chunks.len() - 1) as u16).to_le_bytes());
        head.extend_from_slice(&Self::encode_index_pks(&pks[s0..e0]));
        out.push((key.to_vec(), head));
        for (i, &(s, e)) in chunks.iter().enumerate().skip(1) {
            out.push((
                Self::idx_overflow_key(key, i as u16),
                Self::encode_index_pks(&pks[s..e]),
            ));
        }
        Ok(out)
    }

    /// Decodes a head value (plain, multi, or chunked). `raw` collects the stored
    /// values (head first, then overflow chunks) when the caller wants to rewrite.
    fn idx_decode_entry(
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        tree: &Arc<mitrdb_txn::BPlusTree>,
        key: &[u8],
        head: &[u8],
        mut raw: Option<&mut Vec<Vec<u8>>>,
    ) -> Result<Vec<Vec<u8>>> {
        if head.first() != Some(&CHUNKED_PK_TAG) {
        return Self::decode_index_pks(head);
        }
        if head.len() < 3 {
            return Err(Error::PageCorrupted(
                "Truncated chunked index entry".to_string(),
            ));
        }
        let n = u16::from_le_bytes([head[1], head[2]]);
        let mut pks = Self::decode_index_pks(&head[3..])?;
        if let Some(r) = raw.as_deref_mut() {
            r.push(head.to_vec());
        }
        for i in 1..=n {
            let okey = Self::idx_overflow_key(key, i);
            let v = txn_mgr.get(txn_id, tree, &okey)?.ok_or_else(|| {
                Error::PageCorrupted("Missing index overflow chunk".to_string())
            })?;
            pks.extend(Self::decode_index_pks(&v)?);
            if let Some(r) = raw.as_deref_mut() {
                r.push(v);
            }
        }
        Ok(pks)
    }

    /// (all PKs, stored raw values) for an index key; both empty if the key is absent.
    fn idx_read_all(
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        tree: &Arc<mitrdb_txn::BPlusTree>,
        key: &[u8],
    ) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
        let mut raw = Vec::new();
        match txn_mgr.get(txn_id, tree, key)? {
            None => Ok((Vec::new(), raw)),
            Some(head) => {
                let pks = Self::idx_decode_entry(txn_mgr, txn_id, tree, key, &head, Some(&mut raw))?;
                Ok((pks, raw))
            }
        }
    }

    /// Writes only the chunks that changed; removes leftover overflow chunks.
    /// Empty `pks` deletes the whole entry.
    fn idx_write_all(
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        tree: &Arc<mitrdb_txn::BPlusTree>,
        key: &[u8],
        pks: &[Vec<u8>],
        old_raw: &[Vec<u8>],
    ) -> Result<()> {
        let new_len = if pks.is_empty() {
            let _ = txn_mgr.delete(txn_id, tree, key)?;
            1
        } else {
            let entries = Self::idx_encode_entries(key, pks)?;
            for (i, (k, v)) in entries.iter().enumerate() {
                if old_raw.get(i) != Some(v) {
                    txn_mgr.put(txn_id, tree, k, v)?;
                }
            }
            entries.len()
        };
        for i in new_len..old_raw.len() {
            let _ = txn_mgr.delete(txn_id, tree, &Self::idx_overflow_key(key, i as u16))?;
        }
        Ok(())
    }

    fn encode_index_pks(pks: &[Vec<u8>]) -> Vec<u8> {
        if pks.len() == 1 {
            return pks[0].clone();
        }
        let mut out = vec![MULTI_PK_TAG];
        out.extend_from_slice(&(pks.len() as u16).to_le_bytes());
        for pk in pks {
            out.extend_from_slice(&(pk.len() as u16).to_le_bytes());
            out.extend_from_slice(pk);
        }
        out
    }

    fn decode_index_pks(buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        if buf[0] != MULTI_PK_TAG {
            return Ok(vec![buf.to_vec()]);
        }
        if buf.len() < 3 {
            return Err(Error::PageCorrupted(
                "Truncated multi-PK index entry".to_string(),
            ));
        }
        let count = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;
        let mut pos = 3;
        let mut pks = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 2 > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated PK length in index entry".to_string(),
                ));
            }
            let len = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + len > buf.len() {
                return Err(Error::PageCorrupted(
                    "Truncated PK bytes in index entry".to_string(),
                ));
            }
            pks.push(buf[pos..pos + len].to_vec());
            pos += len;
        }
        Ok(pks)
    }

    pub fn execute_plan_with_isolation(
        &self,
        plan: PhysicalPlan,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        active_txn: &mut Option<TxnId>,
        default_isolation: IsolationLevel,
    ) -> Result<ExecutionResult> {
        if active_txn.is_some()
            && matches!(
                plan,
                PhysicalPlan::CreateTable { .. }
                    | PhysicalPlan::DropTable { .. }
                    | PhysicalPlan::CreateIndex { .. }
                    | PhysicalPlan::DropIndex { .. }
                    | PhysicalPlan::AlterTableAddColumn { .. }
                    | PhysicalPlan::AlterTableRenameColumn { .. }
                    | PhysicalPlan::AlterTableDropColumn { .. }
            )
        {
            return Err(Error::TransactionError(
                "DDL (CREATE/DROP/ALTER) is not allowed inside a transaction; COMMIT or ROLLBACK first"
                    .to_string(),
            ));
        }
        match plan {
            PhysicalPlan::Begin => {
                if active_txn.is_some() {
                    return Err(Error::TransactionError(
                        "Transaction already active".to_string(),
                    ));
                }
                let tid = txn_mgr.begin_with_isolation(default_isolation)?;
                *active_txn = Some(tid);
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::Commit => {
                let tid = active_txn.take().ok_or_else(|| {
                    Error::TransactionError("No active transaction to commit".to_string())
                })?;
                Self::commit_or_abort(txn_mgr, tid)?;
                catalog.sync_roots()?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::Rollback => {
                let tid = active_txn.take().ok_or_else(|| {
                    Error::TransactionError("No active transaction to roll back".to_string())
                })?;
                txn_mgr.abort(tid)?;
                catalog.sync_roots()?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::Checkpoint => {
                if active_txn.is_none() && txn_mgr.active_txn_count() == 0 {
                    catalog.sync_roots()?;
                }
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::Explain(inner) => {
                let desc = Self::format_explain_plan(&inner);
                Ok(ExecutionResult::Explained(desc))
            }
            PhysicalPlan::CreateTable {
                name,
                columns,
                if_not_exists,
            } => {
                if if_not_exists && catalog.get_table_schema(&name)?.is_some() {
                    return Ok(ExecutionResult::Affected(0));
                }
                catalog.create_table(&name, columns)?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::DropTable { name } => {
                let mut dropped_trees = Vec::new();
                if let Some(schema) = catalog.get_table_schema(&name)? {
                    if let Some(t) = catalog.table_tree(schema.table_id) {
                        dropped_trees.push(t);
                    }
                    for idx_meta in catalog.get_table_indexes(schema.table_id) {
                        if let Some(it) = catalog.index_tree(idx_meta.index_id) {
                            dropped_trees.push(it);
                        }
                    }
                }
                txn_mgr.purge_trees_state(&dropped_trees).map_err(|_| {
                    Error::WriteConflict(format!(
                        "Cannot drop table '{}' while referenced by an active transaction",
                        name
                    ))
                })?;
                catalog.drop_table(&name)?;
                txn_mgr.purge_trees_state(&dropped_trees)?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::CreateIndex {
                index_name,
                table_name,
                column_name,
                unique,
                if_not_exists,
            } => {
                if if_not_exists && catalog.get_index_by_name(&index_name).is_some() {
                    return Ok(ExecutionResult::Affected(0));
                }
                let mut staged_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                if let Some(schema) = catalog.get_table_schema(&table_name)? {
                    let mut table_trees = Vec::new();
                    if let Some(table_tree) = catalog.table_tree(schema.table_id) {
                        table_trees.push(table_tree);
                    }
                    for idx_meta in catalog.get_table_indexes(schema.table_id) {
                        if let Some(it) = catalog.index_tree(idx_meta.index_id) {
                            table_trees.push(it);
                        }
                    }
                    if table_trees
                        .iter()
                        .any(|t| txn_mgr.has_active_tree_references(t))
                    {
                        return Err(Error::WriteConflict(format!(
                            "Cannot create index '{}' on table '{}' while referenced by an active transaction",
                            index_name, table_name
                        )));
                    }
                    if let Some(col_pos) = schema.columns.iter().position(|c| c.name == column_name)
                        && let Some(table_tree) = catalog.table_tree(schema.table_id)
                    {
                        let pk_idx = Self::primary_key_index(&schema);
                        let mut key_map: std::collections::BTreeMap<Vec<u8>, Vec<Vec<u8>>> =
                            std::collections::BTreeMap::new();
                        for (_pk, row_bytes) in table_tree.range_scan(None, None)? {
                            let tuple =
                                Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
                            let pk_bytes = tuple[pk_idx].to_key_bytes();
                            let idx_key = tuple[col_pos].to_key_bytes();
                            let pks = key_map.entry(idx_key).or_default();
                            if unique
                                && !matches!(tuple[col_pos], Value::Null)
                                && pks.iter().any(|existing_pk| existing_pk != &pk_bytes)
                            {
                                return Err(Error::WriteConflict(format!(
                                    "Duplicate key violates unique index '{}'",
                                    index_name
                                )));
                            }
                            if !pks.contains(&pk_bytes) {
                                pks.push(pk_bytes);
                                pks.sort();
                            }
                        }
                        for (k, pks) in key_map {
                            staged_entries.extend(Self::idx_encode_entries(&k, &pks)?);
                        }
                    }
                }
                let index_id =
                    catalog.create_index(&index_name, &table_name, &column_name, unique)?;
                if !staged_entries.is_empty()
                    && let Err(e) = catalog.populate_index_entries(index_id, &staged_entries)
                {
                    let _ = catalog.drop_index(&index_name);
                    return Err(e);
                }
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::DropIndex { index_name } => {
                let meta = catalog.get_index_by_name(&index_name).ok_or_else(|| {
                    Error::TransactionError(format!("Index '{}' does not exist", index_name))
                })?;
                let idx_tree = catalog.index_tree(meta.index_id);
                if let Some(ref t) = idx_tree
                    && txn_mgr.has_active_tree_references(t)
                {
                    return Err(Error::WriteConflict(format!(
                        "Cannot drop index '{}' while referenced by an active transaction",
                        index_name
                    )));
                }
                catalog.drop_index(&index_name)?;
                if let Some(t) = idx_tree {
                    txn_mgr.purge_trees_state(&[t])?;
                }
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::AlterTableAddColumn { table_name, column } => {
                if let Some(schema) = catalog.get_table_schema(&table_name)? {
                    let mut table_trees = Vec::new();
                    if let Some(table_tree) = catalog.table_tree(schema.table_id) {
                        table_trees.push(table_tree);
                    }
                    for idx_meta in catalog.get_table_indexes(schema.table_id) {
                        if let Some(it) = catalog.index_tree(idx_meta.index_id) {
                            table_trees.push(it);
                        }
                    }
                    if table_trees
                        .iter()
                        .any(|t| txn_mgr.has_active_tree_references(t))
                    {
                        return Err(Error::WriteConflict(format!(
                            "Cannot alter table '{}' while referenced by an active transaction",
                            table_name
                        )));
                    }
                }
                catalog.add_column(&table_name, column)?;
                catalog.sync_roots()?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::AlterTableRenameColumn {
                table_name,
                old_name,
                new_name,
            } => {
                if let Some(schema) = catalog.get_table_schema(&table_name)? {
                    let mut table_trees = Vec::new();
                    if let Some(table_tree) = catalog.table_tree(schema.table_id) {
                        table_trees.push(table_tree);
                    }
                    for idx_meta in catalog.get_table_indexes(schema.table_id) {
                        if let Some(it) = catalog.index_tree(idx_meta.index_id) {
                            table_trees.push(it);
                        }
                    }
                    if table_trees
                        .iter()
                        .any(|t| txn_mgr.has_active_tree_references(t))
                    {
                        return Err(Error::WriteConflict(format!(
                            "Cannot alter table '{}' while referenced by an active transaction",
                            table_name
                        )));
                    }
                }
                catalog.rename_column(&table_name, &old_name, &new_name)?;
                catalog.sync_roots()?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::AlterTableDropColumn {
                table_name,
                column_name,
            } => {
                if let Some(schema) = catalog.get_table_schema(&table_name)? {
                    let mut table_trees = Vec::new();
                    if let Some(table_tree) = catalog.table_tree(schema.table_id) {
                        table_trees.push(table_tree);
                    }
                    for idx_meta in catalog.get_table_indexes(schema.table_id) {
                        if let Some(it) = catalog.index_tree(idx_meta.index_id) {
                            table_trees.push(it);
                        }
                    }
                    if table_trees
                        .iter()
                        .any(|t| txn_mgr.has_active_tree_references(t))
                    {
                        return Err(Error::WriteConflict(format!(
                            "Cannot alter table '{}' while referenced by an active transaction",
                            table_name
                        )));
                    }
                }
                catalog.drop_column(&table_name, &column_name)?;
                catalog.sync_roots()?;
                Ok(ExecutionResult::Ok)
            }
            PhysicalPlan::Insert { schema, values } => {
                let (tid, auto_commit) = Self::ensure_txn(txn_mgr, *active_txn, default_isolation)?;
                let mark = Self::stmt_mark(txn_mgr, tid, auto_commit);
                let res = self.exec_insert(catalog, txn_mgr, tid, &schema, &values);
                let res = Self::stmt_rollback_on_err(txn_mgr, tid, mark, active_txn, res);
                if auto_commit {
                    match res {
                        Ok(_) => {
                            Self::commit_or_abort(txn_mgr, tid)?;
                            catalog.sync_roots()?;
                        }
                        Err(e) => {
                            txn_mgr.abort(tid)?;
                            catalog.sync_roots()?;
                            return Err(e);
                        }
                    }
                }
                res
            }
            PhysicalPlan::InsertMany { schema, rows } => {
                let (tid, auto_commit) = Self::ensure_txn(txn_mgr, *active_txn, default_isolation)?;
                let mark = Self::stmt_mark(txn_mgr, tid, auto_commit);
                let mut res: Result<ExecutionResult> = Ok(ExecutionResult::Affected(rows.len()));
                for values in &rows {
                    if let Err(e) = self.exec_insert(catalog, txn_mgr, tid, &schema, values) {
                        res = Err(e);
                        break;
                    }
                }
                let res = Self::stmt_rollback_on_err(txn_mgr, tid, mark, active_txn, res);
                if auto_commit {
                    match res {
                        Ok(_) => {
                            Self::commit_or_abort(txn_mgr, tid)?;
                            catalog.sync_roots()?;
                        }
                        Err(e) => {
                            txn_mgr.abort(tid)?;
                            catalog.sync_roots()?;
                            return Err(e);
                        }
                    }
                }
                res
            }
            PhysicalPlan::Update {
                schema,
                assignments,
                increments,
                filter,
            } => {
                let (tid, auto_commit) = Self::ensure_txn(txn_mgr, *active_txn, default_isolation)?;
                let mark = Self::stmt_mark(txn_mgr, tid, auto_commit);
                let res = self.exec_update(
                    catalog,
                    txn_mgr,
                    tid,
                    &schema,
                    &assignments,
                    &increments,
                    filter.as_ref(),
                );
                let res = Self::stmt_rollback_on_err(txn_mgr, tid, mark, active_txn, res);
                if auto_commit {
                    match res {
                        Ok(_) => {
                            Self::commit_or_abort(txn_mgr, tid)?;
                            catalog.sync_roots()?;
                        }
                        Err(e) => {
                            txn_mgr.abort(tid)?;
                            catalog.sync_roots()?;
                            return Err(e);
                        }
                    }
                }
                res
            }
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexOrderedScan { .. }
            | PhysicalPlan::Aggregate { .. }
            | PhysicalPlan::GroupAggregate { .. }
            | PhysicalPlan::Distinct { .. }
            | PhysicalPlan::Union { .. }
            | PhysicalPlan::NestedLoopJoin { .. } => {
                let (tid, auto_commit) = Self::ensure_txn(txn_mgr, *active_txn, default_isolation)?;
                let res = self.exec_read_plan(plan, catalog, txn_mgr, tid);
                if auto_commit {
                    match res {
                        Ok(_) => {
                            Self::commit_or_abort(txn_mgr, tid)?;
                        }
                        Err(e) => {
                            txn_mgr.abort(tid)?;
                            return Err(e);
                        }
                    }
                }
                res
            }
            PhysicalPlan::Delete { schema, filter } => {
                let (tid, auto_commit) = Self::ensure_txn(txn_mgr, *active_txn, default_isolation)?;
                let mark = Self::stmt_mark(txn_mgr, tid, auto_commit);
                let res = self.exec_delete(catalog, txn_mgr, tid, &schema, filter.as_ref());
                let res = Self::stmt_rollback_on_err(txn_mgr, tid, mark, active_txn, res);
                if auto_commit {
                    match res {
                        Ok(_) => {
                            Self::commit_or_abort(txn_mgr, tid)?;
                            catalog.sync_roots()?;
                        }
                        Err(e) => {
                            txn_mgr.abort(tid)?;
                            catalog.sync_roots()?;
                            return Err(e);
                        }
                    }
                }
                res
            }
        }
    }

    fn exec_read_plan(
        &self,
        plan: PhysicalPlan,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        tid: TxnId,
    ) -> Result<ExecutionResult> {
        match plan {
            PhysicalPlan::SeqScan {
                schema,
                projection,
                filter,
                order_by,
                sort_keys,
                limit,
                offset,
                top_n,
            } => {
                let effective_sort = if !sort_keys.is_empty() {
                    sort_keys
                } else {
                    order_by.into_iter().collect()
                };
                self.exec_seq_scan(
                    catalog,
                    txn_mgr,
                    tid,
                    &schema,
                    &projection,
                    filter.as_ref(),
                    &effective_sort,
                    limit,
                    offset,
                    top_n,
                )
            }
            PhysicalPlan::IndexScan {
                schema,
                index,
                lookup_value,
                projection,
                order_by,
                sort_keys,
                limit,
                offset,
                top_n,
            } => {
                let effective_sort = if !sort_keys.is_empty() {
                    sort_keys
                } else {
                    order_by.into_iter().collect()
                };
                self.exec_index_scan(
                    catalog,
                    txn_mgr,
                    tid,
                    &schema,
                    index.index_id,
                    &lookup_value,
                    &projection,
                    &effective_sort,
                    limit,
                    offset,
                    top_n,
                )
            }
            PhysicalPlan::IndexRangeScan {
                schema,
                index,
                ranges,
                filter,
                projection,
                order_by,
                sort_keys,
                limit,
                offset,
                top_n,
            } => {
                let effective_sort = if !sort_keys.is_empty() {
                    sort_keys
                } else {
                    order_by.into_iter().collect()
                };
                self.exec_index_range_scan(
                    catalog,
                    txn_mgr,
                    tid,
                    &schema,
                    index.index_id,
                    &ranges,
                    filter.as_ref(),
                    &projection,
                    &effective_sort,
                    limit,
                    offset,
                    top_n,
                )
            }
            PhysicalPlan::IndexOrderedScan {
                schema,
                index,
                ranges,
                descending,
                pk_descending,
                filter,
                projection,
                limit,
                offset,
            } => self.exec_index_ordered_scan(
                catalog,
                txn_mgr,
                tid,
                &schema,
                index.index_id,
                &ranges,
                descending,
                pk_descending,
                filter.as_ref(),
                &projection,
                limit,
                offset,
            ),
            PhysicalPlan::Aggregate {
                input,
                aggregates,
                having,
                order_by: _,
                limit,
                offset,
            } => {
                let scan_res = self.exec_read_plan(*input, catalog, txn_mgr, tid)?;
                match scan_res {
                    ExecutionResult::Rows { rows, .. } => {
                        Self::compute_aggregates(&rows, &aggregates, having.as_ref(), limit, offset)
                    }
                    other => Ok(other),
                }
            }
            PhysicalPlan::GroupAggregate {
                input,
                group_by,
                select_items,
                aggregates,
                having,
                order_by,
                limit,
                offset,
            } => {
                let scan_res = self.exec_read_plan(*input, catalog, txn_mgr, tid)?;
                match scan_res {
                    ExecutionResult::Rows { rows, .. } => Self::compute_group_aggregates(
                        &rows,
                        &group_by,
                        &select_items,
                        &aggregates,
                        having.as_ref(),
                        &order_by,
                        limit,
                        offset,
                    ),
                    other => Ok(other),
                }
            }
            PhysicalPlan::Distinct {
                input,
                limit,
                offset,
            } => {
                let scan_res = self.exec_read_plan(*input, catalog, txn_mgr, tid)?;
                match scan_res {
                    ExecutionResult::Rows { columns, rows } => {
                        let mut seen = HashSet::new();
                        let mut distinct_rows = Vec::new();
                        for row in rows {
                            if seen.insert(row.clone()) {
                                distinct_rows.push(row);
                            }
                        }
                        let total = distinct_rows.len();
                        let final_rows = if offset >= total {
                            Vec::new()
                        } else {
                            let sliced = distinct_rows.into_iter().skip(offset);
                            match limit {
                                Some(lim) => sliced.take(lim).collect(),
                                None => sliced.collect(),
                            }
                        };
                        Ok(ExecutionResult::Rows {
                            columns,
                            rows: final_rows,
                        })
                    }
                    other => Ok(other),
                }
            }
            PhysicalPlan::Union {
                left,
                right,
                all,
                order_by_list,
                limit,
                offset,
            } => {
                let left_res = self.exec_read_plan(*left, catalog, txn_mgr, tid)?;
                let right_res = self.exec_read_plan(*right, catalog, txn_mgr, tid)?;
                let (left_cols, left_rows) = match left_res {
                    ExecutionResult::Rows { columns, rows } => (columns, rows),
                    _ => (Vec::new(), Vec::new()),
                };
                let (right_cols, right_rows) = match right_res {
                    ExecutionResult::Rows { columns, rows } => (columns, rows),
                    _ => (Vec::new(), Vec::new()),
                };
                if left_cols.len() != right_cols.len() {
                    return Err(Error::TransactionError(format!(
                        "UNION queries have different number of columns: {} vs {}",
                        left_cols.len(),
                        right_cols.len()
                    )));
                }

                let mut combined_rows = if all {
                    let mut rows = left_rows;
                    rows.extend(right_rows);
                    rows
                } else {
                    let mut seen = HashSet::new();
                    let mut rows = Vec::new();
                    for row in left_rows.into_iter().chain(right_rows) {
                        if seen.insert(row.clone()) {
                            rows.push(row);
                        }
                    }
                    rows
                };

                if !order_by_list.is_empty() {
                    combined_rows.sort_by(|a, b| {
                        for &(col_idx, desc) in &order_by_list {
                            if col_idx < a.len() && col_idx < b.len() {
                                let c = a[col_idx].cmp(&b[col_idx]);
                                let directed = if desc { c.reverse() } else { c };
                                if directed != Ordering::Equal {
                                    return directed;
                                }
                            }
                        }
                        Ordering::Equal
                    });
                }

                let total = combined_rows.len();
                let final_rows = if offset >= total {
                    Vec::new()
                } else {
                    let sliced = combined_rows.into_iter().skip(offset);
                    match limit {
                        Some(lim) => sliced.take(lim).collect(),
                        None => sliced.collect(),
                    }
                };
                Ok(ExecutionResult::Rows {
                    columns: left_cols,
                    rows: final_rows,
                })
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                left_schema: _,
                right_schema,
                join_conditions,
                projection,
                filter,
                sort_keys,
                limit,
                offset,
            } => {
                let left_res = self.exec_read_plan(*left, catalog, txn_mgr, tid)?;
                let right_res = self.exec_read_plan(*right, catalog, txn_mgr, tid)?;
                let left_rows = match left_res {
                    ExecutionResult::Rows { rows, .. } => rows,
                    _ => Vec::new(),
                };
                let right_rows = match right_res {
                    ExecutionResult::Rows { rows, .. } => rows,
                    _ => Vec::new(),
                };

                let columns: Vec<String> =
                    projection.iter().map(|p| p.output_name.clone()).collect();
                let mut result_tuples = Vec::new();

                let eq_pairs: Vec<(usize, usize)> = join_conditions
                    .iter()
                    .filter(|(_, _, op)| *op == mitrdb_query::CompareOp::Eq)
                    .map(|&(l, r, _)| (l, r))
                    .collect();
                let use_hash = !eq_pairs.is_empty();
                let all_idx: Vec<usize> = (0..right_rows.len()).collect();
                let no_match: Vec<usize> = Vec::new();
                let mut hash_index: std::collections::HashMap<Vec<Value>, Vec<usize>> =
                    std::collections::HashMap::new();
                if use_hash {
                    for (ri, rrow) in right_rows.iter().enumerate() {
                        let key: Vec<Value> = eq_pairs.iter().map(|&(_, r)| rrow[r].clone()).collect();
                        if key.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        hash_index.entry(key).or_default().push(ri);
                    }
                }
                for left_row in &left_rows {
                    let mut matched = false;
                    let cands: &[usize] = if use_hash {
                        let key: Vec<Value> = eq_pairs.iter().map(|&(l, _)| left_row[l].clone()).collect();
                        if key.iter().any(|v| matches!(v, Value::Null)) {
                            no_match.as_slice()
                        } else {
                            hash_index
                                .get(&key)
                                .map(|v| v.as_slice())
                                .unwrap_or(no_match.as_slice())
                        }
                    } else {
                        all_idx.as_slice()
                    };
                    for &ri in cands {
                        let right_row = &right_rows[ri];
                        let mut conds_met = true;
                        for &(l_idx, r_idx, op) in &join_conditions {
                            if !op.evaluate(&left_row[l_idx], &right_row[r_idx]) {
                                conds_met = false;
                                break;
                            }
                        }
                        if conds_met
                            && filter
                                .as_ref()
                                .is_none_or(|f| f.evaluate(left_row, right_row))
                        {
                            matched = true;
                            let mut tuple = Vec::with_capacity(projection.len());
                            for item in &projection {
                                match item.source {
                                    JoinProjSource::Left(i) => tuple.push(left_row[i].clone()),
                                    JoinProjSource::Right(j) => tuple.push(right_row[j].clone()),
                                }
                            }
                            result_tuples.push(tuple);
                        }
                    }
                    if !matched && join_type == JoinType::LeftOuter {
                        let null_right: Vec<Value> = vec![Value::Null; right_schema.columns.len()];
                        if filter
                            .as_ref()
                            .is_none_or(|f| f.evaluate(left_row, &null_right))
                        {
                            let mut tuple = Vec::with_capacity(projection.len());
                            for item in &projection {
                                match item.source {
                                    JoinProjSource::Left(i) => tuple.push(left_row[i].clone()),
                                    JoinProjSource::Right(_) => tuple.push(Value::Null),
                                }
                            }
                            result_tuples.push(tuple);
                        }
                    }
                }

                if !sort_keys.is_empty() {
                    result_tuples.sort_by(|a, b| {
                        for &(col_idx, desc) in &sort_keys {
                            if col_idx < a.len() && col_idx < b.len() {
                                let c = a[col_idx].cmp(&b[col_idx]);
                                let directed = if desc { c.reverse() } else { c };
                                if directed != Ordering::Equal {
                                    return directed;
                                }
                            }
                        }
                        Ordering::Equal
                    });
                }

                let total = result_tuples.len();
                let final_rows = if offset >= total {
                    Vec::new()
                } else {
                    let sliced = result_tuples.into_iter().skip(offset);
                    match limit {
                        Some(lim) => sliced.take(lim).collect(),
                        None => sliced.collect(),
                    }
                };

                Ok(ExecutionResult::Rows {
                    columns,
                    rows: final_rows,
                })
            }
            _ => unreachable!("Non-read plan passed to exec_read_plan"),
        }
    }

    fn eval_single_aggregate(rows: &[Vec<Value>], agg: &BoundAggregate) -> Result<Value> {
        let val = match agg.func {
            AggregateFunc::CountStar => Value::Int64(rows.len() as i64),
            AggregateFunc::Count => {
                let col_idx = agg.column_idx.unwrap();
                let count = rows
                    .iter()
                    .filter(|r| !matches!(r[col_idx], Value::Null))
                    .count();
                Value::Int64(count as i64)
            }
            AggregateFunc::Sum => {
                let col_idx = agg.column_idx.unwrap();
                let mut sum: Option<i64> = None;
                for r in rows {
                    match &r[col_idx] {
                        Value::Null => {}
                        Value::Int64(v) => {
                            sum = Some(match sum {
                                None => *v,
                                Some(acc) => acc.checked_add(*v).ok_or_else(|| {
                                    Error::TransactionError(
                                        "Integer overflow in SUM aggregation".to_string(),
                                    )
                                })?,
                            });
                        }
                        other => {
                            return Err(Error::TransactionError(format!(
                                "Invalid non-integer value {:?} in SUM aggregation",
                                other
                            )));
                        }
                    }
                }
                sum.map_or(Value::Null, Value::Int64)
            }
            AggregateFunc::Avg => {
                let col_idx = agg.column_idx.unwrap();
                let mut sum: i128 = 0;
                let mut count: i128 = 0;
                for r in rows {
                    match &r[col_idx] {
                        Value::Null => {}
                        Value::Int64(v) => {
                            sum += i128::from(*v);
                            count += 1;
                        }
                        other => {
                            return Err(Error::TransactionError(format!(
                                "Invalid non-integer value {:?} in AVG aggregation",
                                other
                            )));
                        }
                    }
                }
                if count == 0 {
                    Value::Null
                } else {
                    let avg_i128 = sum / count;
                    let avg_i64 = i64::try_from(avg_i128).map_err(|_| {
                        Error::TransactionError("Integer overflow in AVG aggregation".to_string())
                    })?;
                    Value::Int64(avg_i64)
                }
            }
            AggregateFunc::Min => {
                let col_idx = agg.column_idx.unwrap();
                let mut min_val: Option<Value> = None;
                for r in rows {
                    let v = &r[col_idx];
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    min_val = Some(match min_val {
                        None => v.clone(),
                        Some(cur) => {
                            if v < &cur {
                                v.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                min_val.unwrap_or(Value::Null)
            }
            AggregateFunc::Max => {
                let col_idx = agg.column_idx.unwrap();
                let mut max_val: Option<Value> = None;
                for r in rows {
                    let v = &r[col_idx];
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    max_val = Some(match max_val {
                        None => v.clone(),
                        Some(cur) => {
                            if v > &cur {
                                v.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                max_val.unwrap_or(Value::Null)
            }
        };
        Ok(val)
    }

    fn compute_aggregates(
        rows: &[Vec<Value>],
        aggregates: &[BoundAggregate],
        having: Option<&BoundHavingPredicate>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<ExecutionResult> {
        let columns: Vec<String> = aggregates.iter().map(|a| a.output_name.clone()).collect();
        if limit == Some(0) || offset > 0 {
            return Ok(ExecutionResult::Rows {
                columns,
                rows: Vec::new(),
            });
        }

        let mut all_aggs = aggregates.to_vec();
        if let Some(hp) = having {
            hp.collect_aggregates(&mut all_aggs);
        }

        let mut all_vals = Vec::with_capacity(all_aggs.len());
        for agg in &all_aggs {
            all_vals.push(Self::eval_single_aggregate(rows, agg)?);
        }

        if let Some(hp) = having
            && !hp.evaluate(&[], &[], &all_aggs, &all_vals)
        {
            return Ok(ExecutionResult::Rows {
                columns,
                rows: Vec::new(),
            });
        }

        let agg_row = all_vals.into_iter().take(aggregates.len()).collect();
        Ok(ExecutionResult::Rows {
            columns,
            rows: vec![agg_row],
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_group_aggregates(
        rows: &[Vec<Value>],
        group_by: &[usize],
        select_items: &[BoundSelectItem],
        aggregates: &[BoundAggregate],
        having: Option<&BoundHavingPredicate>,
        order_by: &[BoundGroupOrderBy],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<ExecutionResult> {
        let columns: Vec<String> = select_items
            .iter()
            .map(|item| match item {
                BoundSelectItem::Column { output_name, .. } => output_name.clone(),
                BoundSelectItem::Aggregate(agg) => agg.output_name.clone(),
            })
            .collect();

        if limit == Some(0) {
            return Ok(ExecutionResult::Rows {
                columns,
                rows: Vec::new(),
            });
        }

        let mut all_aggs = aggregates.to_vec();
        if let Some(hp) = having {
            hp.collect_aggregates(&mut all_aggs);
        }

        let mut grouped_accs: BTreeMap<Vec<Value>, Vec<AggAccumulator>> = BTreeMap::new();
        for row in rows {
            let group_key: Vec<Value> = group_by.iter().map(|&idx| row[idx].clone()).collect();
            let accs = grouped_accs
                .entry(group_key)
                .or_insert_with(|| all_aggs.iter().map(AggAccumulator::new).collect());
            for acc in accs {
                acc.update(row)?;
            }
        }

        let mut surviving_groups: Vec<(Vec<Value>, Vec<Value>)> =
            Vec::with_capacity(grouped_accs.len());
        for (group_key, accs) in grouped_accs {
            let mut all_vals = Vec::with_capacity(accs.len());
            for acc in accs {
                all_vals.push(acc.finalize()?);
            }
            if let Some(hp) = having
                && !hp.evaluate(group_by, &group_key, &all_aggs, &all_vals)
            {
                continue;
            }
            surviving_groups.push((group_key, all_vals));
        }

        if !order_by.is_empty() {
            surviving_groups.sort_by(|(ka, aa), (kb, ab)| {
                for ob in order_by {
                    let va = ob.target.resolve_value(group_by, ka, &all_aggs, aa);
                    let vb = ob.target.resolve_value(group_by, kb, &all_aggs, ab);
                    let c = va.cmp(vb);
                    let directed = if ob.descending { c.reverse() } else { c };
                    if directed != Ordering::Equal {
                        return directed;
                    }
                }
                ka.cmp(kb)
            });
        }

        let iter = surviving_groups.into_iter().skip(offset);
        let final_groups: Vec<(Vec<Value>, Vec<Value>)> = match limit {
            Some(lim) => iter.take(lim).collect(),
            None => iter.collect(),
        };

        let mut out_rows = Vec::with_capacity(final_groups.len());
        for (group_key, agg_values) in final_groups {
            let mut row = Vec::with_capacity(select_items.len());
            for item in select_items {
                match item {
                    BoundSelectItem::Column { column_idx, .. } => {
                        let pos = group_by.iter().position(|c| c == column_idx).unwrap();
                        row.push(group_key[pos].clone());
                    }
                    BoundSelectItem::Aggregate(agg) => {
                        let pos = all_aggs.iter().position(|a| a.same_metric(agg)).unwrap();
                        row.push(agg_values[pos].clone());
                    }
                }
            }
            out_rows.push(row);
        }

        Ok(ExecutionResult::Rows {
            columns,
            rows: out_rows,
        })
    }

    fn ensure_txn(
        txn_mgr: &TxnManager,
        active_txn: Option<TxnId>,
        default_isolation: IsolationLevel,
    ) -> Result<(TxnId, bool)> {
        match active_txn {
            Some(tid) => Ok((tid, false)),
            None => Ok((txn_mgr.begin_with_isolation(default_isolation)?, true)),
        }
    }

    fn stmt_mark(txn_mgr: &TxnManager, tid: TxnId, auto_commit: bool) -> Option<usize> {
        if auto_commit {
            None
        } else {
            txn_mgr.write_mark(tid).ok()
        }
    }

    /// Explicit txn: a failed statement must not leave partial writes behind.
    fn stmt_rollback_on_err(
        txn_mgr: &TxnManager,
        tid: TxnId,
        mark: Option<usize>,
        active_txn: &mut Option<TxnId>,
        res: Result<ExecutionResult>,
    ) -> Result<ExecutionResult> {
        if res.is_err()
            && let Some(m) = mark
            && txn_mgr.rollback_to_mark(tid, m).is_err()
            && txn_mgr.abort(tid).is_ok()
        {
            *active_txn = None;
        }
        res
    }

    /// A failed commit must not leak the txn (and its locks): roll it back, keep the original error.
    /// abort() refuses non-Active txns (poisoned / already committed); that Err is ignored on purpose.
    fn commit_or_abort(txn_mgr: &TxnManager, tid: TxnId) -> Result<u64> {
        match txn_mgr.commit(tid) {
            Ok(ts) => Ok(ts),
            Err(e) => {
                let _ = txn_mgr.abort(tid);
                Err(e)
            }
        }
    }

    fn primary_key_index(schema: &TableSchema) -> usize {
        schema
            .columns
            .iter()
            .position(|c| c.is_primary_key)
            .unwrap_or(0)
    }

    fn filter_key_ranges(op: CompareOp, key_bytes: Vec<u8>) -> Vec<KeyRange> {
        match op {
            CompareOp::Eq => vec![KeyRange::point(key_bytes)],
            CompareOp::Gt => vec![KeyRange::from_exclusive(key_bytes)],
            CompareOp::Ge => vec![KeyRange::from_inclusive(key_bytes)],
            CompareOp::Lt => vec![KeyRange::to_exclusive(key_bytes)],
            CompareOp::Le => vec![KeyRange::to_inclusive(key_bytes)],
            CompareOp::Ne => vec![
                KeyRange::to_exclusive(key_bytes.clone()),
                KeyRange::from_exclusive(key_bytes),
            ],
        }
    }

    fn collect_predicate_range_locks(
        catalog: &Catalog,
        schema: &TableSchema,
        table_tree: &Arc<mitrdb_txn::BPlusTree>,
        pred: &BoundPredicate,
    ) -> Option<Vec<(Arc<mitrdb_txn::BPlusTree>, KeyRange)>> {
        let pk_idx = Self::primary_key_index(schema);
        let indexes = catalog.get_table_indexes(schema.table_id);

        // First check if the predicate narrows the PK or one or more secondary indexes
        let mut constrained_locks = Vec::new();
        if let Some(pk_ranges) = QueryEngine::extract_column_ranges(pred, pk_idx) {
            for r in pk_ranges {
                constrained_locks.push((table_tree.clone(), r));
            }
        }
        for idx_meta in &indexes {
            if let Some(col_pos) = schema
                .columns
                .iter()
                .position(|c| c.name == idx_meta.column_name)
                && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
                && let Some(idx_ranges) = QueryEngine::extract_column_ranges(pred, col_pos)
            {
                for r in idx_ranges {
                    constrained_locks.push((idx_tree.clone(), r));
                }
            }
        }
        if !constrained_locks.is_empty() {
            return Some(constrained_locks);
        }

        match pred {
            BoundPredicate::Comparison {
                column_idx,
                op,
                value,
            } => {
                if matches!(value, Value::Null) {
                    return Some(Vec::new());
                }
                let key_bytes = value.to_key_bytes();
                if *column_idx == pk_idx {
                    return Some(
                        Self::filter_key_ranges(*op, key_bytes)
                            .into_iter()
                            .map(|r| (table_tree.clone(), r))
                            .collect(),
                    );
                }
                let col_name = &schema.columns[*column_idx].name;
                if let Some(idx_meta) = indexes.iter().find(|i| &i.column_name == col_name)
                    && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
                {
                    return Some(
                        Self::filter_key_ranges(*op, key_bytes)
                            .into_iter()
                            .map(|r| (idx_tree.clone(), r))
                            .collect(),
                    );
                }
                None
            }
            BoundPredicate::IsNull(column_idx) => {
                let key_bytes = Value::Null.to_key_bytes();
                if *column_idx == pk_idx {
                    return Some(vec![(table_tree.clone(), KeyRange::point(key_bytes))]);
                }
                let col_name = &schema.columns[*column_idx].name;
                if let Some(idx_meta) = indexes.iter().find(|i| &i.column_name == col_name)
                    && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
                {
                    return Some(vec![(idx_tree, KeyRange::point(key_bytes))]);
                }
                None
            }
            BoundPredicate::IsNotNull(_) => None,
            BoundPredicate::And(left, right) => {
                let l_locks =
                    Self::collect_predicate_range_locks(catalog, schema, table_tree, left);
                let r_locks =
                    Self::collect_predicate_range_locks(catalog, schema, table_tree, right);
                match (l_locks, r_locks) {
                    (Some(mut l), Some(r)) => {
                        l.extend(r);
                        Some(l)
                    }
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                }
            }
            BoundPredicate::Or(left, right) => {
                let mut l = Self::collect_predicate_range_locks(catalog, schema, table_tree, left)?;
                let r = Self::collect_predicate_range_locks(catalog, schema, table_tree, right)?;
                l.extend(r);
                Some(l)
            }
        }
    }

    fn acquire_pre_scan_range_locks(
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        table_tree: &Arc<mitrdb_txn::BPlusTree>,
        filter: Option<&BoundFilter>,
    ) -> Result<()> {
        if txn_mgr.isolation_level(txn_id) != Some(IsolationLevel::Serializable) {
            return Ok(());
        }
        let indexes = catalog.get_table_indexes(schema.table_id);
        match filter {
            None => {
                txn_mgr.lock_range(txn_id, table_tree, &KeyRange::full())?;
                for idx_meta in &indexes {
                    if let Some(idx_tree) = catalog.index_tree(idx_meta.index_id) {
                        txn_mgr.lock_range(txn_id, &idx_tree, &KeyRange::full())?;
                    }
                }
            }
            Some(bf) => {
                if let Some(locks) =
                    Self::collect_predicate_range_locks(catalog, schema, table_tree, &bf.predicate)
                {
                    for (tree_ref, range) in locks {
                        txn_mgr.lock_range(txn_id, &tree_ref, &range)?;
                    }
                } else {
                    txn_mgr.lock_range(txn_id, table_tree, &KeyRange::full())?;
                    for idx_meta in &indexes {
                        if let Some(idx_tree) = catalog.index_tree(idx_meta.index_id) {
                            txn_mgr.lock_range(txn_id, &idx_tree, &KeyRange::full())?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn lock_matched_row_keys(
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        table_tree: &Arc<mitrdb_txn::BPlusTree>,
        rows: &[(Vec<u8>, Vec<Value>)],
    ) -> Result<()> {
        if txn_mgr.isolation_level(txn_id) != Some(IsolationLevel::Serializable) || rows.is_empty()
        {
            return Ok(());
        }
        let indexes = catalog.get_table_indexes(schema.table_id);
        for (pk, _) in rows {
            let _ = txn_mgr.get(txn_id, table_tree, pk)?;
        }
        for idx_meta in &indexes {
            if let Some(col_pos) = schema
                .columns
                .iter()
                .position(|c| c.name == idx_meta.column_name)
                && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
            {
                for (_pk, tuple) in rows {
                    let idx_key = tuple[col_pos].to_key_bytes();
                    let _ = txn_mgr.get(txn_id, &idx_tree, &idx_key)?;
                }
            }
        }
        Ok(())
    }

    fn acquire_filter_range_locks(
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        table_tree: &Arc<mitrdb_txn::BPlusTree>,
        filter: Option<&BoundFilter>,
        matched_rows: &[(Vec<u8>, Vec<Value>)],
    ) -> Result<()> {
        Self::acquire_pre_scan_range_locks(catalog, txn_mgr, txn_id, schema, table_tree, filter)?;
        Self::lock_matched_row_keys(catalog, txn_mgr, txn_id, schema, table_tree, matched_rows)
    }

    fn validate_column_value(table_name: &str, col: &ColumnDef, val: &Value) -> Result<()> {
        match val {
            Value::Null => {
                if !col.nullable || col.is_primary_key {
                    return Err(Error::TransactionError(format!(
                        "NOT NULL constraint failed: {}.{}",
                        table_name, col.name
                    )));
                }
                Ok(())
            }
            Value::Int64(_) if col.data_type == DataType::Int64 => Ok(()),
            Value::Text(_) if col.data_type == DataType::Text => Ok(()),
            Value::Bool(_) if col.data_type == DataType::Bool => Ok(()),
            Value::Bytes(_) if col.data_type == DataType::Bytes => Ok(()),
            _ => Err(Error::TransactionError(format!(
                "Type mismatch for column '{}.{}': expected {:?}",
                table_name, col.name, col.data_type
            ))),
        }
    }

    fn exec_insert(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        values: &[Value],
    ) -> Result<ExecutionResult> {
        let profiling = mitrdb_observe::profiling_enabled();
        let insert_started = profiling.then(std::time::Instant::now);
        if values.len() != schema.columns.len() {
            return Err(Error::TransactionError(format!(
                "Column count mismatch on INSERT into '{}'",
                schema.name
            )));
        }
        for (col, val) in schema.columns.iter().zip(values.iter()) {
            Self::validate_column_value(&schema.name, col, val)?;
        }

        let tree: Arc<_> = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let pk_idx = Self::primary_key_index(schema);
        let pk_bytes = values[pk_idx].to_key_bytes();
        let row_bytes = Tuple::serialize(values);

        let indexes = catalog.get_table_indexes(schema.table_id);
        let mut idx_writes = Vec::new();
        for idx_meta in &indexes {
            if let Some(col_pos) = schema
                .columns
                .iter()
                .position(|c| c.name == idx_meta.column_name)
                && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
            {
                let idx_key = values[col_pos].to_key_bytes();
                idx_writes.push((idx_meta.clone(), idx_tree, idx_key));
            }
        }

        let mut lock_targets: Vec<(&Arc<mitrdb_txn::BPlusTree>, Vec<u8>)> =
            Vec::with_capacity(1 + idx_writes.len());
        lock_targets.push((&tree, pk_bytes.clone()));
        for (_, idx_tree, idx_key) in &idx_writes {
            lock_targets.push((idx_tree, idx_key.clone()));
        }
        txn_mgr.acquire_exclusive_keys(txn_id, &lock_targets)?;

        if txn_mgr.get(txn_id, &tree, &pk_bytes)?.is_some() {
            return Err(Error::WriteConflict(format!(
                "Duplicate primary key violates PRIMARY KEY constraint on table '{}'",
                schema.name
            )));
        }

        let mut encoded_idx_writes = Vec::with_capacity(idx_writes.len());
        for (idx_meta, idx_tree, idx_key) in &idx_writes {
            let (mut pks, old_raw) = Self::idx_read_all(txn_mgr, txn_id, idx_tree, idx_key)?;
            if idx_meta.unique
                && pks.iter().any(|existing_pk| existing_pk != &pk_bytes)
                && !schema
                    .columns
                    .iter()
                    .position(|c| c.name == idx_meta.column_name)
                    .is_some_and(|cp| matches!(values[cp], Value::Null))
            {
                return Err(Error::WriteConflict(format!(
                    "Duplicate key violates unique index '{}'",
                    idx_meta.name
                )));
            }
            if !pks.contains(&pk_bytes) {
                pks.push(pk_bytes.clone());
                pks.sort();
            }
            encoded_idx_writes.push((idx_tree, idx_key, pks, old_raw));
        }

        for (idx_tree, idx_key, pks, old_raw) in encoded_idx_writes {
            Self::idx_write_all(txn_mgr, txn_id, idx_tree, idx_key, &pks, &old_raw)?;
        }

        txn_mgr.put(txn_id, &tree, &pk_bytes, &row_bytes)?;
        if let Some(started) = insert_started {
            mitrdb_observe::record_profile_duration(
                mitrdb_observe::ProfilePhase::InsertExecution,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        Ok(ExecutionResult::Affected(1))
    }

    /// Candidate rows for UPDATE/DELETE. PK ranges when the filter bounds the PK;
    /// else a secondary index, but only when every extracted range is a single
    /// point (`= k`, `IS NULL`, OR of points); else full scan. The caller's
    /// bf.evaluate still filters every row, so a superset is sound.
    fn narrow_dml_entries(
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        tree: &Arc<mitrdb_txn::BPlusTree>,
        filter: Option<&BoundFilter>,
        pk_idx: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(bf) = filter {
            if let Some(ranges) = QueryEngine::extract_column_ranges(&bf.predicate, pk_idx) {
                let mut seen_pks = HashSet::new();
                let mut out = Vec::new();
                for r in &ranges {
                    for (k, v) in txn_mgr.range_scan(txn_id, tree, r)? {
                        if seen_pks.insert(k.clone()) {
                            out.push((k, v));
                        }
                    }
                }
                return Ok(out);
            }
            let is_point = |r: &KeyRange| {
                matches!((&r.start, &r.end), (KeyBound::Included(a), KeyBound::Included(b)) if a == b)
            };
            let indexes = catalog.get_table_indexes(schema.table_id);
            for idx_meta in &indexes {
                let Some(col_pos) = schema
                    .columns
                    .iter()
                    .position(|c| c.name == idx_meta.column_name)
                else {
                    continue;
                };
                let Some(idx_tree) = catalog.index_tree(idx_meta.index_id) else {
                    continue;
                };
                let Some(ranges) = QueryEngine::extract_column_ranges(&bf.predicate, col_pos)
                else {
                    continue;
                };
                if ranges.is_empty() || !ranges.iter().all(is_point) {
                    continue;
                }
                let mut seen_pks = HashSet::new();
                let mut out = Vec::new();
                for r in &ranges {
                    for (idx_key, encoded) in txn_mgr.range_scan(txn_id, &idx_tree, r)? {
                        if idx_key.first() == Some(&OVERFLOW_KEY_TAG) {
                            continue;
                        }
                        let pks = Self::idx_decode_entry(
                            txn_mgr, txn_id, &idx_tree, &idx_key, &encoded, None,
                        )?;
                        for pk in pks {
                            if !seen_pks.insert(pk.clone()) {
                                continue;
                            }
                            if let Some(row) = txn_mgr.get(txn_id, tree, &pk)? {
                                out.push((pk, row));
                            }
                        }
                    }
                }
                // keep PK order, same as the full-scan / PK-range paths
                out.sort_by(|a, b| a.0.cmp(&b.0));
                return Ok(out);
            }
        }
        txn_mgr.scan_unlocked(txn_id, tree)
    }

    fn exec_update(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        assignments: &[(usize, Value)],
        increments: &[(usize, i64)],
        filter: Option<&BoundFilter>,
    ) -> Result<ExecutionResult> {
        for (col_idx, new_val) in assignments {
            if *col_idx >= schema.columns.len() {
                return Err(Error::TransactionError(format!(
                    "Column index {} out of bounds for table '{}'",
                    col_idx, schema.name
                )));
            }
            Self::validate_column_value(&schema.name, &schema.columns[*col_idx], new_val)?;
        }
        let tree = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let pk_idx = Self::primary_key_index(schema);
        // Narrow the scan (PK ranges, else point lookups on a secondary index);
        // bf.evaluate below still filters every row. Otherwise full scan as before.
        let entries =
            Self::narrow_dml_entries(catalog, txn_mgr, txn_id, schema, &tree, filter, pk_idx)?;
        let indexes = catalog.get_table_indexes(schema.table_id);

        let mut planned_updates = Vec::new();
        let mut updating_old_pks = HashSet::new();
        let mut matched_rows = Vec::new();

        for (old_pk_bytes, row_bytes) in entries {
            let mut tuple = Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
            if let Some(bf) = filter
                && !bf.evaluate(&tuple)
            {
                continue;
            }
            let old_tuple = tuple.clone();
            matched_rows.push((old_pk_bytes.clone(), old_tuple.clone()));
            for (col_idx, new_val) in assignments {
                tuple[*col_idx] = new_val.clone();
            }
            for (col_idx, delta) in increments {
                let next = match tuple.get(*col_idx) {
                    Some(Value::Int64(v)) => Value::Int64(v.checked_add(*delta).ok_or_else(|| {
                        Error::TransactionError("Integer overflow in UPDATE".to_string())
                    })?),
                    Some(Value::Null) => Value::Null,
                    _ => {
                        return Err(Error::TransactionError(
                            "UPDATE increment needs an INT column".to_string(),
                        ));
                    }
                };
                tuple[*col_idx] = next;
            }
            let new_pk_bytes = tuple[pk_idx].to_key_bytes();
            updating_old_pks.insert(old_pk_bytes.clone());
            planned_updates.push((old_pk_bytes, old_tuple, new_pk_bytes, tuple));
        }

        Self::acquire_filter_range_locks(
            catalog,
            txn_mgr,
            txn_id,
            schema,
            &tree,
            filter,
            &matched_rows,
        )?;

        let mut idx_trees = Vec::new();
        for idx_meta in &indexes {
            if let Some(col_pos) = schema
                .columns
                .iter()
                .position(|c| c.name == idx_meta.column_name)
                && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
            {
                idx_trees.push((idx_meta.clone(), col_pos, idx_tree));
            }
        }

        let mut lock_targets: Vec<(&Arc<mitrdb_txn::BPlusTree>, Vec<u8>)> = Vec::new();
        for (old_pk_bytes, old_tuple, new_pk_bytes, new_tuple) in &planned_updates {
            lock_targets.push((&tree, old_pk_bytes.clone()));
            if new_pk_bytes != old_pk_bytes {
                lock_targets.push((&tree, new_pk_bytes.clone()));
            }
            for (_, col_pos, idx_tree) in &idx_trees {
                let old_idx_key = old_tuple[*col_pos].to_key_bytes();
                let new_idx_key = new_tuple[*col_pos].to_key_bytes();
                lock_targets.push((idx_tree, old_idx_key.clone()));
                if new_idx_key != old_idx_key {
                    lock_targets.push((idx_tree, new_idx_key));
                }
            }
        }
        txn_mgr.acquire_exclusive_keys(txn_id, &lock_targets)?;

        let mut batch_seen_pks = HashSet::new();
        for (old_pk_bytes, _, new_pk_bytes, _) in &planned_updates {
            if !batch_seen_pks.insert(new_pk_bytes.clone()) {
                return Err(Error::WriteConflict(format!(
                    "Duplicate primary key in UPDATE on table '{}'",
                    schema.name
                )));
            }
            if new_pk_bytes != old_pk_bytes
                && !updating_old_pks.contains(new_pk_bytes)
                && txn_mgr.get(txn_id, &tree, new_pk_bytes)?.is_some()
            {
                return Err(Error::WriteConflict(format!(
                    "Primary key conflict in UPDATE on table '{}'",
                    schema.name
                )));
            }
        }

        for (idx_meta, col_pos, idx_tree) in &idx_trees {
            if idx_meta.unique {
                let mut batch_seen_keys = HashSet::new();
                for (old_pk_bytes, _, _, new_tuple) in &planned_updates {
                    if matches!(new_tuple[*col_pos], Value::Null) {
                        continue;
                    }
                    let new_idx_key = new_tuple[*col_pos].to_key_bytes();
                    if !batch_seen_keys.insert(new_idx_key.clone()) {
                        return Err(Error::WriteConflict(format!(
                            "Duplicate key violates unique index '{}'",
                            idx_meta.name
                        )));
                    }
                    {
                        let (existing_pks, _) = Self::idx_read_all(txn_mgr, txn_id, idx_tree, &new_idx_key)?;
                        if existing_pks.iter().any(|existing_pk| {
                            existing_pk != old_pk_bytes && !updating_old_pks.contains(existing_pk)
                        }) {
                            return Err(Error::WriteConflict(format!(
                                "Duplicate key violates unique index '{}'",
                                idx_meta.name
                            )));
                        }
                    }
                }
            }
        }

        for (old_pk_bytes, old_tuple, new_pk_bytes, new_tuple) in &planned_updates {
            if new_pk_bytes != old_pk_bytes {
                let _ = txn_mgr.delete(txn_id, &tree, old_pk_bytes)?;
            }
            for (_, col_pos, idx_tree) in &idx_trees {
                let old_idx_key = old_tuple[*col_pos].to_key_bytes();
                let new_idx_key = new_tuple[*col_pos].to_key_bytes();
                if old_idx_key != new_idx_key || old_pk_bytes != new_pk_bytes {
                    let (mut pks, old_raw) = Self::idx_read_all(txn_mgr, txn_id, idx_tree, &old_idx_key)?;
                    if !pks.is_empty() {
                        pks.retain(|pk| pk != old_pk_bytes);
                        Self::idx_write_all(txn_mgr, txn_id, idx_tree, &old_idx_key, &pks, &old_raw)?;
                    }
                }
            }
        }

        let mut updated = 0;
        for (old_pk_bytes, old_tuple, new_pk_bytes, new_tuple) in &planned_updates {
            if updated % 64 == 63 {
                txn_mgr.relieve_wal_pressure(WAL_PRESSURE_LIMIT_BYTES)?;
            }
            let new_row_bytes = Tuple::serialize(new_tuple);
            txn_mgr.put(txn_id, &tree, new_pk_bytes, &new_row_bytes)?;

            for (_, col_pos, idx_tree) in &idx_trees {
                let old_idx_key = old_tuple[*col_pos].to_key_bytes();
                let new_idx_key = new_tuple[*col_pos].to_key_bytes();
                if old_idx_key != new_idx_key || old_pk_bytes != new_pk_bytes {
                    let (mut pks, old_raw) = Self::idx_read_all(txn_mgr, txn_id, idx_tree, &new_idx_key)?;
                    if !pks.contains(new_pk_bytes) {
                        pks.push(new_pk_bytes.clone());
                        pks.sort();
                    }
                    Self::idx_write_all(txn_mgr, txn_id, idx_tree, &new_idx_key, &pks, &old_raw)?;
                }
            }

            updated += 1;
        }

        Ok(ExecutionResult::Affected(updated))
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_seq_scan(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        projection: &[usize],
        filter: Option<&BoundFilter>,
        sort_keys: &[(usize, bool)],
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    ) -> Result<ExecutionResult> {
        let tree = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let pk_idx = Self::primary_key_index(schema);

        Self::acquire_pre_scan_range_locks(catalog, txn_mgr, txn_id, schema, &tree, filter)?;

        let mut matched_tuples = Vec::new();
        match filter {
            None => {
                let entries = txn_mgr.scan(txn_id, &tree)?;
                for (_pk_bytes, row_bytes) in entries {
                    matched_tuples.push(Tuple::deserialize_for_table(
                        &row_bytes,
                        schema.columns.len(),
                    )?);
                }
            }
            Some(bf) => {
                if let Some(pk_ranges) = QueryEngine::extract_column_ranges(&bf.predicate, pk_idx) {
                    let mut matched_rows = Vec::new();
                    let mut seen_pks = HashSet::new();
                    for r in &pk_ranges {
                        let entries = txn_mgr.range_scan(txn_id, &tree, r)?;
                        for (pk_bytes, row_bytes) in entries {
                            if !seen_pks.insert(pk_bytes.clone()) {
                                continue;
                            }
                            let tuple =
                                Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
                            if bf.evaluate(&tuple) {
                                matched_rows.push((pk_bytes, tuple.clone()));
                                matched_tuples.push(tuple);
                            }
                        }
                    }
                    Self::lock_matched_row_keys(
                        catalog,
                        txn_mgr,
                        txn_id,
                        schema,
                        &tree,
                        &matched_rows,
                    )?;
                } else {
                    let entries = txn_mgr.scan_unlocked(txn_id, &tree)?;
                    let mut matched_rows = Vec::new();
                    for (pk_bytes, row_bytes) in entries {
                        let tuple = Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
                        if bf.evaluate(&tuple) {
                            matched_rows.push((pk_bytes, tuple.clone()));
                            matched_tuples.push(tuple);
                        }
                    }
                    Self::lock_matched_row_keys(
                        catalog,
                        txn_mgr,
                        txn_id,
                        schema,
                        &tree,
                        &matched_rows,
                    )?;
                }
            }
        }

        Self::finalize_rows(
            schema,
            matched_tuples,
            projection,
            sort_keys,
            limit,
            offset,
            top_n,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_index_scan(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        index_id: u32,
        lookup_value: &Value,
        projection: &[usize],
        sort_keys: &[(usize, bool)],
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    ) -> Result<ExecutionResult> {
        let range = KeyRange::point(lookup_value.to_key_bytes());
        self.exec_index_range_scan(
            catalog,
            txn_mgr,
            txn_id,
            schema,
            index_id,
            &[range],
            None,
            projection,
            sort_keys,
            limit,
            offset,
            top_n,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_index_range_scan(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        index_id: u32,
        ranges: &[KeyRange],
        filter: Option<&BoundFilter>,
        projection: &[usize],
        sort_keys: &[(usize, bool)],
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    ) -> Result<ExecutionResult> {
        let table_tree = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let idx_tree = catalog.index_tree(index_id).ok_or_else(|| {
            Error::TransactionError("Secondary index B+Tree not initialized".to_string())
        })?;

        if txn_mgr.isolation_level(txn_id) == Some(IsolationLevel::Serializable) {
            for range in ranges {
                txn_mgr.lock_range(txn_id, &idx_tree, range)?;
            }
            if let Some(bf) = filter {
                Self::acquire_pre_scan_range_locks(
                    catalog,
                    txn_mgr,
                    txn_id,
                    schema,
                    &table_tree,
                    Some(bf),
                )?;
            }
        }

        let mut matched_tuples = Vec::new();
        let mut matched_rows = Vec::new();
        let mut seen_pks = HashSet::new();

        for range in ranges {
            let idx_entries = txn_mgr.range_scan(txn_id, &idx_tree, range)?;
            for (idx_key, encoded_pks) in idx_entries {
                if idx_key.first() == Some(&OVERFLOW_KEY_TAG) {
                    continue;
                }
                let pks = Self::idx_decode_entry(txn_mgr, txn_id, &idx_tree, &idx_key, &encoded_pks, None)?;
                for pk_bytes in pks {
                    if !seen_pks.insert(pk_bytes.clone()) {
                        continue;
                    }
                    if let Some(row_bytes) = txn_mgr.get(txn_id, &table_tree, &pk_bytes)? {
                        let tuple = Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
                        if filter.is_none_or(|bf| bf.evaluate(&tuple)) {
                            matched_rows.push((pk_bytes, tuple.clone()));
                            matched_tuples.push(tuple);
                        }
                    }
                }
            }
        }

        if txn_mgr.isolation_level(txn_id) == Some(IsolationLevel::Serializable) {
            Self::lock_matched_row_keys(
                catalog,
                txn_mgr,
                txn_id,
                schema,
                &table_tree,
                &matched_rows,
            )?;
        }

        Self::finalize_rows(
            schema,
            matched_tuples,
            projection,
            sort_keys,
            limit,
            offset,
            top_n,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_index_ordered_scan(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        index_id: u32,
        ranges: &[KeyRange],
        descending: bool,
        pk_descending: bool,
        filter: Option<&BoundFilter>,
        projection: &[usize],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<ExecutionResult> {
        let table_tree = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let idx_tree = catalog.index_tree(index_id).ok_or_else(|| {
            Error::TransactionError("Secondary index B+Tree not initialized".to_string())
        })?;

        let columns: Vec<String> = projection
            .iter()
            .map(|&i| schema.columns[i].name.clone())
            .collect();

        if limit == Some(0) {
            for range in ranges {
                if txn_mgr.isolation_level(txn_id) == Some(IsolationLevel::Serializable) {
                    txn_mgr.lock_range(txn_id, &idx_tree, range)?;
                }
            }
            Self::acquire_filter_range_locks(
                catalog,
                txn_mgr,
                txn_id,
                schema,
                &table_tree,
                filter,
                &[],
            )?;
            return Ok(ExecutionResult::Rows {
                columns,
                rows: Vec::new(),
            });
        }

        if txn_mgr.isolation_level(txn_id) == Some(IsolationLevel::Serializable) {
            for range in ranges {
                txn_mgr.lock_range(txn_id, &idx_tree, range)?;
            }
            Self::acquire_pre_scan_range_locks(
                catalog,
                txn_mgr,
                txn_id,
                schema,
                &table_tree,
                filter,
            )?;
        }

        let mut ordered_ranges: Vec<&KeyRange> = ranges.iter().collect();
        if descending {
            ordered_ranges.reverse();
        }

        let mut result_tuples = Vec::new();
        let mut matched_rows = Vec::new();
        let mut seen_pks = HashSet::new();
        let mut skipped = 0usize;

        'range_loop: for range in ordered_ranges {
            let idx_entries = txn_mgr.range_scan_directed(txn_id, &idx_tree, range, descending)?;
            for (idx_key, encoded_pks) in idx_entries {
                if idx_key.first() == Some(&OVERFLOW_KEY_TAG) {
                    continue;
                }
                let mut pks = Self::idx_decode_entry(txn_mgr, txn_id, &idx_tree, &idx_key, &encoded_pks, None)?;
                if pk_descending {
                    pks.reverse();
                }
                for pk_bytes in pks {
                    if !seen_pks.insert(pk_bytes.clone()) {
                        continue;
                    }
                    if let Some(row_bytes) = txn_mgr.get(txn_id, &table_tree, &pk_bytes)? {
                        let tuple = Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
                        if filter.is_none_or(|bf| bf.evaluate(&tuple)) {
                            matched_rows.push((pk_bytes, tuple.clone()));
                            if skipped < offset {
                                skipped += 1;
                            } else {
                                result_tuples.push(tuple);
                                if let Some(lim) = limit
                                    && result_tuples.len() >= lim
                                {
                                    break 'range_loop;
                                }
                            }
                        }
                    }
                }
            }
        }

        if txn_mgr.isolation_level(txn_id) == Some(IsolationLevel::Serializable) {
            Self::lock_matched_row_keys(
                catalog,
                txn_mgr,
                txn_id,
                schema,
                &table_tree,
                &matched_rows,
            )?;
        }

        let rows = result_tuples
            .into_iter()
            .map(|tuple| projection.iter().map(|&i| tuple[i].clone()).collect())
            .collect();
        Ok(ExecutionResult::Rows { columns, rows })
    }

    pub fn cmp_tuples(
        a: &[Value],
        b: &[Value],
        sort_keys: &[(usize, bool)],
        pk_idx: usize,
    ) -> Ordering {
        for &(col_idx, desc) in sort_keys {
            let c = a[col_idx].cmp(&b[col_idx]);
            let directed = if desc { c.reverse() } else { c };
            if directed != Ordering::Equal {
                return directed;
            }
        }
        a[pk_idx].cmp(&b[pk_idx])
    }

    fn finalize_rows(
        schema: &TableSchema,
        mut tuples: Vec<Vec<Value>>,
        projection: &[usize],
        sort_keys: &[(usize, bool)],
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    ) -> Result<ExecutionResult> {
        let columns: Vec<String> = projection
            .iter()
            .map(|&i| schema.columns[i].name.clone())
            .collect();

        if limit == Some(0) || offset >= tuples.len() {
            return Ok(ExecutionResult::Rows {
                columns,
                rows: Vec::new(),
            });
        }

        let pk_idx = Self::primary_key_index(schema);
        let selected_tuples: Vec<Vec<Value>> = if !sort_keys.is_empty() {
            if let Some(lim) = limit {
                let k = offset.saturating_add(lim);
                if top_n && k < tuples.len() {
                    let mut heap: BinaryHeap<TopNCandidate<'_>> = BinaryHeap::with_capacity(k);
                    for tuple in tuples {
                        let cand = TopNCandidate {
                            tuple,
                            sort_keys,
                            pk_idx,
                        };
                        if heap.len() < k {
                            heap.push(cand);
                        } else if let Some(worst) = heap.peek()
                            && cand < *worst
                        {
                            let _ = heap.pop();
                            heap.push(cand);
                        }
                    }
                    let sorted_top = heap.into_sorted_vec();
                    sorted_top
                        .into_iter()
                        .skip(offset)
                        .take(lim)
                        .map(|c| c.tuple)
                        .collect()
                } else {
                    tuples.sort_by(|a, b| Self::cmp_tuples(a, b, sort_keys, pk_idx));
                    tuples.into_iter().skip(offset).take(lim).collect()
                }
            } else {
                tuples.sort_by(|a, b| Self::cmp_tuples(a, b, sort_keys, pk_idx));
                tuples.into_iter().skip(offset).collect()
            }
        } else {
            let iter = tuples.into_iter().skip(offset);
            match limit {
                Some(lim) => iter.take(lim).collect(),
                None => iter.collect(),
            }
        };

        let rows = selected_tuples
            .into_iter()
            .map(|tuple| projection.iter().map(|&i| tuple[i].clone()).collect())
            .collect();
        Ok(ExecutionResult::Rows { columns, rows })
    }

    fn exec_delete(
        &self,
        catalog: &Catalog,
        txn_mgr: &TxnManager,
        txn_id: TxnId,
        schema: &TableSchema,
        filter: Option<&BoundFilter>,
    ) -> Result<ExecutionResult> {
        let tree = catalog.table_tree(schema.table_id).ok_or_else(|| {
            Error::TransactionError("Table storage B+Tree not initialized".to_string())
        })?;
        let indexes = catalog.get_table_indexes(schema.table_id);
        let pk_idx = Self::primary_key_index(schema);
        // Narrow the scan (PK ranges, else point lookups on a secondary index);
        // bf.evaluate below still filters every row. Otherwise full scan as before.
        let entries =
            Self::narrow_dml_entries(catalog, txn_mgr, txn_id, schema, &tree, filter, pk_idx)?;

        let mut to_delete = Vec::new();
        for (pk_bytes, row_bytes) in entries {
            let tuple = Tuple::deserialize_for_table(&row_bytes, schema.columns.len())?;
            if let Some(bf) = filter
                && !bf.evaluate(&tuple)
            {
                continue;
            }
            to_delete.push((pk_bytes, tuple));
        }

        Self::acquire_filter_range_locks(
            catalog, txn_mgr, txn_id, schema, &tree, filter, &to_delete,
        )?;

        let mut idx_trees = Vec::new();
        for idx_meta in &indexes {
            if let Some(col_pos) = schema
                .columns
                .iter()
                .position(|c| c.name == idx_meta.column_name)
                && let Some(idx_tree) = catalog.index_tree(idx_meta.index_id)
            {
                idx_trees.push((col_pos, idx_tree));
            }
        }

        let mut lock_targets: Vec<(&Arc<mitrdb_txn::BPlusTree>, Vec<u8>)> = Vec::new();
        if to_delete.is_empty()
            && let Some(bf) = filter
            && matches!(
                bf.predicate,
                BoundPredicate::Comparison {
                    op: CompareOp::Eq,
                    ..
                }
            )
        {
            if bf.column_idx == pk_idx {
                lock_targets.push((&tree, bf.value.to_key_bytes()));
            } else {
                for (col_pos, idx_tree) in &idx_trees {
                    if *col_pos == bf.column_idx {
                        lock_targets.push((idx_tree, bf.value.to_key_bytes()));
                    }
                }
            }
        }
        for (pk_bytes, tuple) in &to_delete {
            lock_targets.push((&tree, pk_bytes.clone()));
            for (col_pos, idx_tree) in &idx_trees {
                lock_targets.push((idx_tree, tuple[*col_pos].to_key_bytes()));
            }
        }
        txn_mgr.acquire_exclusive_keys(txn_id, &lock_targets)?;

        let mut deleted = 0;
        for (pk_bytes, tuple) in to_delete {
            if deleted % 64 == 63 {
                txn_mgr.relieve_wal_pressure(WAL_PRESSURE_LIMIT_BYTES)?;
            }
            if txn_mgr.delete(txn_id, &tree, &pk_bytes)? {
                for (col_pos, idx_tree) in &idx_trees {
                    let idx_key = tuple[*col_pos].to_key_bytes();
                    let (mut pks, old_raw) = Self::idx_read_all(txn_mgr, txn_id, idx_tree, &idx_key)?;
                    if !pks.is_empty() {
                        pks.retain(|pk| pk != &pk_bytes);
                        Self::idx_write_all(txn_mgr, txn_id, idx_tree, &idx_key, &pks, &old_raw)?;
                    }
                }
                deleted += 1;
            }
        }

        Ok(ExecutionResult::Affected(deleted))
    }

}


impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}
