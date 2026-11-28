use mitrdb_catalog::{ColumnDef, DataType};
use mitrdb_common::{Error, Result};

use crate::Server;

/// Easy table helper: `server.table("users").sync("id int primary key, name text")?`
pub struct Table<'a> {
    server: &'a Server,
    name: String,
    session_id: Option<crate::SessionId>,
}

impl<'a> Table<'a> {
    fn with_name(&self, name: &str) -> Table<'a> {
        Table {
            server: self.server,
            name: name.to_string(),
            session_id: self.session_id,
        }
    }

    fn exec_plan(&self, plan: PhysicalPlan) -> Result<ExecutionResult> {
        match self.session_id {
            Some(sid) => self.server.execute_plan_in_session_internal(sid, plan),
            None => self.server.execute_plan_internal(plan),
        }
    }

    fn exec_sql_params(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        match self.session_id {
            Some(sid) => self
                .server
                .execute_sql_params_in_session_internal(sid, sql, params),
            None => self.server.execute_sql_params_internal(sql, params),
        }
    }
}

impl Server {
    pub fn table(&self, name: &str) -> Table<'_> {
        Table {
            server: self,
            name: name.to_string(),
            session_id: None,
        }
    }

    /// Session-scoped table helper for concurrent handlers using dedicated `SessionId`s.
    pub fn session_table(&self, session_id: crate::SessionId, name: &str) -> Table<'_> {
        Table {
            server: self,
            name: name.to_string(),
            session_id: Some(session_id),
        }
    }
}

fn bad(msg: impl Into<String>) -> Error {
    Error::TransactionError(msg.into())
}

fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !s.chars().next().unwrap().is_ascii_digit()
}

fn type_sql(t: DataType) -> &'static str {
    match t {
        DataType::Int64 => "int",
        DataType::Text => "text",
        DataType::Bool => "bool",
        DataType::Bytes => "bytes",
        DataType::Timestamp => "timestamp",
        DataType::Float => "float",
    }
}

fn col_sql(c: &ColumnDef) -> String {
    let mut s = format!("{} {}", c.name, type_sql(c.data_type));
    if c.is_primary_key {
        s.push_str(" primary key");
    } else if !c.nullable {
        s.push_str(" not null");
    }
    s
}

fn parse_columns(spec: &str) -> Result<Vec<ColumnDef>> {
    let mut cols: Vec<ColumnDef> = Vec::new();
    for part in spec.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(bad(format!("bad column definition: '{}'", part.trim())));
        }
        if !valid_ident(tokens[0]) {
            return Err(bad(format!("bad column name: '{}'", tokens[0])));
        }
        let data_type = match tokens[1].to_uppercase().as_str() {
            "INT" | "INT64" | "BIGINT" | "INTEGER" => DataType::Int64,
            "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
            "BOOL" | "BOOLEAN" => DataType::Bool,
            "BYTES" | "BLOB" => DataType::Bytes,
            "TIMESTAMP" | "TIMESTAMPTZ" => DataType::Timestamp,
            "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => DataType::Float,
            other => return Err(bad(format!("unknown column type: {other}"))),
        };
        let rest = tokens[2..].join(" ").to_uppercase();
        let is_pk = rest.contains("PRIMARY KEY");
        let nullable = !is_pk && !rest.contains("NOT NULL");
        if cols.iter().any(|c| c.name == tokens[0]) {
            return Err(bad(format!("duplicate column: {}", tokens[0])));
        }
        cols.push(ColumnDef {
            name: tokens[0].to_string(),
            data_type,
            nullable,
            is_primary_key: is_pk,
        });
    }
    Ok(cols)
}

/// Names of columns marked `unique` in the spec (primary key columns are skipped).
fn unique_columns(spec: &str, wanted: &[ColumnDef]) -> Vec<String> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 3 {
            continue;
        }
        let is_unique = tokens[2..].iter().any(|t| t.eq_ignore_ascii_case("unique"));
        let is_pk = wanted
            .iter()
            .any(|c| c.name == tokens[0] && c.is_primary_key);
        if is_unique && !is_pk {
            out.push(tokens[0].to_string());
        }
    }
    out
}

/// Names of columns marked `index` in the spec (non-unique index). PK and unique columns are skipped.
fn indexed_columns(spec: &str, wanted: &[ColumnDef], uniques: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 3 {
            continue;
        }
        let is_index = tokens[2..].iter().any(|t| t.eq_ignore_ascii_case("index"));
        let is_pk = wanted
            .iter()
            .any(|c| c.name == tokens[0] && c.is_primary_key);
        if is_index && !is_pk && !uniques.iter().any(|u| u == tokens[0]) {
            out.push(tokens[0].to_string());
        }
    }
    out
}

impl Table<'_> {
    fn check_name(&self) -> Result<()> {
        if valid_ident(&self.name) {
            Ok(())
        } else {
            Err(bad(format!("bad table name: '{}'", self.name)))
        }
    }

    /// Make the table match `spec`, keeping data. Returns the SQL it ran.
    /// Handles new nullable columns, NOT NULL <-> nullable (SET NOT NULL fails naming the first
    /// bad primary key if a row holds NULL) and unique/index. Column type changes are refused
    /// here (they rewrite every row): use `sync_allow_type_change`.
    pub fn sync(&self, spec: &str) -> Result<Vec<String>> {
        self.sync_inner(spec, false)
    }

    /// Like `sync`, but a changed column type is applied too (rows rewritten through a hidden
    /// shadow column; strict casts, first bad row aborts). Refuses PK and indexed columns.
    pub fn sync_allow_type_change(&self, spec: &str) -> Result<Vec<String>> {
        self.sync_inner(spec, true)
    }

    fn sync_inner(&self, spec: &str, allow_type_change: bool) -> Result<Vec<String>> {
        self.check_name()?;
        let wanted = parse_columns(spec)?;
        let mut plan: Vec<String> = Vec::new();
        let uniques = unique_columns(spec, &wanted);
        let indexed = indexed_columns(spec, &wanted, &uniques);

        match self.server.get_table_schema(&self.name)? {
            None => {
                let cols: Vec<String> = wanted.iter().map(col_sql).collect();
                plan.push(format!("CREATE TABLE {} ({})", self.name, cols.join(", ")));
            }
            Some(schema) => {
                for old in &schema.columns {
                    if old.is_dropped() {
                        continue;
                    }
                    if !wanted.iter().any(|w| w.name == old.name) {
                        return Err(bad(format!(
                            "column '{}' is missing from your definition; removing columns is not supported (use rename or keep it)",
                            old.name
                        )));
                    }
                }
                for w in &wanted {
                    match schema.columns.iter().find(|c| c.name == w.name) {
                        Some(old) => {
                            if old.is_primary_key != w.is_primary_key {
                                return Err(bad(format!(
                                    "column '{}': changing the primary key is not supported yet (needs a rebuild of the table and all indexes)",
                                    w.name
                                )));
                            }
                            if old.data_type != w.data_type {
                                if !allow_type_change {
                                    return Err(bad(format!(
                                        "column '{}' changed type ({} -> {}); this rewrites every row, call sync_allow_type_change() to do it",
                                        w.name,
                                        type_sql(old.data_type),
                                        type_sql(w.data_type)
                                    )));
                                }
                                if old.is_primary_key {
                                    return Err(bad(format!(
                                        "column '{}': changing the type of a primary key is not supported yet",
                                        w.name
                                    )));
                                }
                                if self
                                    .server
                                    .catalog()
                                    .get_table_indexes(schema.table_id)
                                    .iter()
                                    .any(|i| i.column_name == w.name)
                                {
                                    return Err(bad(format!(
                                        "column '{}' has an index; remove it before changing the type",
                                        w.name
                                    )));
                                }
                                plan.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                                    self.name,
                                    w.name,
                                    type_sql(w.data_type)
                                ));
                            }
                            if old.nullable != w.nullable {
                                plan.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} {}",
                                    self.name,
                                    w.name,
                                    if w.nullable {
                                        "DROP NOT NULL"
                                    } else {
                                        "SET NOT NULL"
                                    }
                                ));
                            }
                        }
                        None => {
                            if w.is_primary_key || !w.nullable {
                                return Err(bad(format!(
                                    "new column '{}' must be nullable (existing rows have no value)",
                                    w.name
                                )));
                            }
                            plan.push(format!(
                                "ALTER TABLE {} ADD COLUMN {}",
                                self.name,
                                col_sql(w)
                            ));
                        }
                    }
                }
            }
        }

        let existing = self.server.get_table_schema(&self.name)?;
        let existing_idx = match &existing {
            Some(sc) => self.server.catalog().get_table_indexes(sc.table_id),
            None => Vec::new(),
        };
        let uq_name = |col: &str| format!("{}_{}_uq", self.name, col);
        let mut dropped_uq: Vec<String> = Vec::new();
        for idx in &existing_idx {
            if idx.unique
                && idx.name == uq_name(&idx.column_name)
                && !uniques.contains(&idx.column_name)
            {
                // unique removed from the definition: drop only the index (rows are untouched)
                dropped_uq.push(idx.column_name.clone());
                plan.push(format!("DROP INDEX {}", idx.name));
            }
        }
        for col in &uniques {
            if existing_idx
                .iter()
                .any(|i| i.unique && i.column_name == *col)
            {
                continue;
            }
            plan.push(format!(
                "CREATE UNIQUE INDEX {} ON {} ({})",
                uq_name(col),
                self.name,
                col
            ));
        }

        let ix_name = |col: &str| format!("{}_{}_idx", self.name, col);
        for idx in &existing_idx {
            if !idx.unique
                && idx.name == ix_name(&idx.column_name)
                && !indexed.contains(&idx.column_name)
            {
                return Err(bad(format!(
                    "column '{}' has an index in the database but not in your definition; removing an index is not supported here (run DROP INDEX {} first)",
                    idx.column_name, idx.name
                )));
            }
        }
        for col in &indexed {
            if existing_idx
                .iter()
                .any(|i| i.column_name == *col && !dropped_uq.contains(&i.column_name))
            {
                continue;
            }
            plan.push(format!(
                "CREATE INDEX {} ON {} ({})",
                ix_name(col),
                self.name,
                col
            ));
        }

        for sql in &plan {
            self.server.execute_sql_internal(sql)?;
        }
        Ok(plan)
    }

    /// Drop a column (logical drop: no row rewrite, no slowdown). Refuses PK / indexed columns.
    pub fn drop_column(&self, name: &str) -> Result<()> {
        self.check_name()?;
        if !valid_ident(name) {
            return Err(bad("bad column name"));
        }
        self.server
            .execute_sql_internal(&format!("ALTER TABLE {} DROP COLUMN {}", self.name, name))
            .map(|_| ())
    }

    /// Rename a column, keeping data.
    pub fn rename(&self, old: &str, new: &str) -> Result<()> {
        self.check_name()?;
        if !valid_ident(old) || !valid_ident(new) {
            return Err(bad("bad column name"));
        }
        self.server
            .execute_sql_internal(&format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {}",
                self.name, old, new
            ))
            .map(|_| ())
    }

    /// Remove NOT NULL from a column (catalog flag only, no row rewrite). Refuses PK columns.
    pub fn set_nullable(&self, name: &str) -> Result<()> {
        self.check_name()?;
        if !valid_ident(name) {
            return Err(bad("bad column name"));
        }
        self.server
            .execute_sql_internal(&format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                self.name, name
            ))
            .map(|_| ())
    }

    /// Add NOT NULL to a column. Fails (naming the first bad primary key) if any row has NULL.
    /// With `default`, NULL rows are first backfilled in their own transaction (int/text/bool);
    /// the backfill and the flip are not atomic and not safe against concurrent writers.
    pub fn set_not_null(&self, name: &str, default: Option<Value>) -> Result<()> {
        self.check_name()?;
        if !valid_ident(name) {
            return Err(bad("bad column name"));
        }
        let sql = match default {
            Some(Value::Int64(n)) => format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL DEFAULT {}",
                self.name, name, n
            ),
            Some(Value::Text(t)) => format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL DEFAULT '{}'",
                self.name,
                name,
                t.replace('\'', "''")
            ),
            Some(Value::Bool(b)) => format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL DEFAULT {}",
                self.name,
                name,
                if b { "TRUE" } else { "FALSE" }
            ),
            Some(_) => return Err(bad("default must be an int, text or bool value")),
            None => format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                self.name, name
            ),
        };
        self.server.execute_sql_internal(&sql).map(|_| ())
    }

    /// Change a column's type (`ty`: "int", "text", "bool", "bytes"), rewriting rows through a
    /// hidden shadow column. Strict casts (int<->text, int<->bool 0/1, text<->bytes UTF-8);
    /// the first bad row aborts the whole change. The column moves to the end of the column
    /// list. Refuses PK / indexed columns. Not safe against concurrent writers.
    pub fn change_type(&self, name: &str, ty: &str) -> Result<()> {
        self.check_name()?;
        if !valid_ident(name) {
            return Err(bad("bad column name"));
        }
        if ty.is_empty() || !ty.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(bad("bad type name"));
        }
        self.server
            .execute_sql_internal(&format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                self.name, name, ty
            ))
            .map(|_| ())
    }
}

use crate::{ExecutionResult, PhysicalPlan, Value};

/// Turn plain Rust values into database values: `val(1)`, `val("text")`, `val(true)`.
pub trait IntoValue {
    fn into_value(self) -> Value;
}
impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}
impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::Int64(self)
    }
}
impl IntoValue for i32 {
    fn into_value(self) -> Value {
        Value::Int64(self as i64)
    }
}
impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}
impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::Text(self)
    }
}
impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::Float(mitrdb_query::F64::new(self))
    }
}
impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self)
    }
}

pub fn val<T: IntoValue>(x: T) -> Value {
    x.into_value()
}

fn check_value(table: &str, col: &ColumnDef, v: &Value) -> Result<()> {
    match v {
        Value::Null => {
            if !col.nullable || col.is_primary_key {
                Err(bad(format!(
                    "NOT NULL constraint failed: {}.{}",
                    table, col.name
                )))
            } else {
                Ok(())
            }
        }
        Value::Int64(_) if matches!(col.data_type, DataType::Int64 | DataType::Timestamp) => Ok(()),
        Value::Text(_) if col.data_type == DataType::Text => Ok(()),
        Value::Bool(_) if col.data_type == DataType::Bool => Ok(()),
        Value::Bytes(_) if col.data_type == DataType::Bytes => Ok(()),
        Value::BlobRef(_, _, _) if col.data_type == DataType::Bytes => Ok(()),
        Value::Float(f) if col.data_type == DataType::Float && !f.0.is_nan() => Ok(()),
        _ => Err(bad(format!(
            "type mismatch for column '{}.{}': expected {:?}",
            table, col.name, col.data_type
        ))),
    }
}

impl Table<'_> {
    /// Insert one row. Columns can be in any order; missing columns become NULL.
    /// `db.table("users").insert(&[("id", val(1)), ("name", val("Taukir"))])?;`
    pub fn insert(&self, row: &[(&str, Value)]) -> Result<usize> {
        self.check_name()?;
        let schema = self
            .server
            .get_table_schema(&self.name)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.name)))?;

        for (i, (name, _)) in row.iter().enumerate() {
            if !schema.columns.iter().any(|c| c.name == *name) {
                return Err(bad(format!("unknown column '{}'", name)));
            }
            if row[..i].iter().any(|(n, _)| n == name) {
                return Err(bad(format!("column '{}' given twice", name)));
            }
        }

        let defaults = self.load_defaults()?;
        let mut values = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            let v = row
                .iter()
                .find(|(n, _)| *n == col.name)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| defaults.get(&col.name).cloned().unwrap_or(Value::Null));
            check_value(&self.name, col, &v)?;
            values.push(v);
        }

        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        let _fk_guard = self.fk_guard(false)?;
        self.check_fks(&names, &values)?;
        self.check_checks(&names, &values)?;

        match self.exec_plan(PhysicalPlan::Insert { schema, values })? {
            ExecutionResult::Affected(n) => Ok(n),
            _ => Ok(1),
        }
    }

    /// Insert with auto-increment int primary key (max(pk)+1). Returns (id, affected).
    /// Retries if a concurrent insert grabbed the same id.
    pub fn insert_auto(&self, pk: &str, row: &[(&str, Value)]) -> Result<(i64, usize)> {
        if row.iter().any(|(n, _)| *n == pk) {
            return Err(bad(format!("column '{}' is auto, do not pass it", pk)));
        }
        let table_lock = auto_table_lock(&self.name);
        let _auto_guard = table_lock.lock().unwrap_or_else(|e| e.into_inner());
        const SEQ: &str = "mitrdb_seq";
        let g0 = crate::ForceAutocommit::new();
        ensure_internal(
            self.server,
            SEQ,
            "tbl text primary key, last_id int not null",
        )?;
        drop(g0);
        let seq = self.server.table(SEQ);
        let key = Value::Text(self.name.clone());
        let mut need_scan = false;
        for attempt in 0..1000usize {
            if attempt > 0 {
                seq_conflict_backoff(attempt);
            }
            let g = crate::ForceAutocommit::new();
            let cur = seq
                .find()
                .columns(&["last_id"])
                .filter("tbl", "=", key.clone())
                .run()?;
            drop(g);
            let prev = match cur.rows.first().and_then(|r| r.first()) {
                Some(Value::Int64(n)) => Some(*n),
                _ => None,
            };
            // Full-table max(pk) scan only for the first claim, or after a duplicate pk
            // (someone inserted an explicit id above the counter).
            let max_pk = if prev.is_none() || need_scan {
                let top = self
                    .find()
                    .columns(&[pk])
                    .order_by_desc(pk)
                    .limit(1)
                    .run()?;
                match top.rows.first().and_then(|r| r.first()) {
                    Some(Value::Int64(n)) => *n,
                    _ => 0,
                }
            } else {
                0
            };
            let next = prev
                .unwrap_or(0)
                .max(max_pk)
                .checked_add(1)
                .ok_or_else(|| bad("auto id overflow"))?;
            let g = crate::ForceAutocommit::new();
            let claimed = match prev {
                None => {
                    match seq.insert(&[("tbl", key.clone()), ("last_id", Value::Int64(next))]) {
                        Ok(_) => true,
                        Err(e) if e.to_string().contains("Duplicate") || seq_retryable(&e) => false,
                        Err(e) => return Err(e),
                    }
                }
                Some(p) => {
                    match seq
                        .update()
                        .set("last_id", Value::Int64(next))
                        .filter("tbl", "=", key.clone())
                        .filter("last_id", "=", Value::Int64(p))
                        .run()
                    {
                        Ok(n) => n == 1,
                        Err(e) if seq_retryable(&e) => false,
                        Err(e) => return Err(e),
                    }
                }
            };
            drop(g);
            if !claimed {
                continue;
            }
            let mut full: Vec<(&str, Value)> = Vec::with_capacity(row.len() + 1);
            full.push((pk, Value::Int64(next)));
            full.extend(row.iter().cloned());
            let in_explicit_txn = self.server.session_has_active_txn(self.session_id);
            match self.insert(&full) {
                Ok(n) => return Ok((next, n)),
                Err(e)
                    if !in_explicit_txn
                        && (e.to_string().contains("Duplicate primary key")
                            || seq_retryable(&e)) =>
                {
                    need_scan = true;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(bad("auto id: too many conflicts"))
    }
}

/// One lock per table name, so threads of this process do not fight over the same
/// auto-id counter row and the same rightmost leaf. Other processes still rely on the CAS.
fn auto_table_lock(name: &str) -> std::sync::Arc<std::sync::Mutex<()>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut m = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    m.entry(name.to_string()).or_default().clone()
}

/// Errors that mean another thread won the race for the auto-id counter or key: retry.
fn seq_retryable(e: &impl std::fmt::Display) -> bool {
    let m = e.to_string();
    m.contains("WRITE_CONFLICT") || m.contains("LOCK_TIMEOUT")
}

/// Small backoff for the hot auto-id counter row: yield first, then short capped sleeps.
fn seq_conflict_backoff(attempt: usize) {
    if attempt < 8 {
        std::thread::yield_now();
    } else {
        let us = (attempt as u64 * 20).min(2000);
        std::thread::sleep(std::time::Duration::from_micros(us));
    }
}

/// Create/sync an internal table; if another thread created it first, that is fine.
fn ensure_internal(server: &Server, name: &str, spec: &str) -> Result<()> {
    if server.get_table_schema(name)?.is_some() {
        return Ok(());
    }
    match server.table(name).sync(spec) {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("already exists") => Ok(()),
        Err(e) => Err(e),
    }
}

const FK_TABLE: &str = "mitrdb_fk";

/// Serializes FK check + write (child insert/update, parent delete) within this process.
static FK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

thread_local! {
    static FK_HELD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Reentrant (per thread) wrapper, so cascade deletes can recurse without deadlock.
struct FkGuard(Option<std::sync::MutexGuard<'static, ()>>);
impl Drop for FkGuard {
    fn drop(&mut self) {
        if self.0.is_some() {
            FK_HELD.with(|h| h.set(false));
        }
    }
}

fn txt(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        _ => String::new(),
    }
}

impl Table<'_> {
    /// Declare `child_col` -> `parent_tbl.parent_col` (parent col must be the primary key).
    /// Enforced by Table/ORM insert, update, delete (not by raw SQL). Safe to call again.
    pub fn add_foreign_key(
        &self,
        child_col: &str,
        parent_tbl: &str,
        parent_col: &str,
    ) -> Result<()> {
        self.add_foreign_key_on(child_col, parent_tbl, parent_col, false)
    }

    /// Like `add_foreign_key`; `cascade = true` deletes child rows when the parent row is deleted.
    pub fn add_foreign_key_on(
        &self,
        child_col: &str,
        parent_tbl: &str,
        parent_col: &str,
        cascade: bool,
    ) -> Result<()> {
        self.check_name()?;
        if !valid_ident(child_col) || !valid_ident(parent_tbl) || !valid_ident(parent_col) {
            return Err(bad("bad name in foreign key"));
        }
        let cs = self
            .server
            .get_table_schema(&self.name)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.name)))?;
        let ps = self.server.get_table_schema(parent_tbl)?.ok_or_else(|| {
            bad(format!(
                "parent table '{}' not found (sync it first)",
                parent_tbl
            ))
        })?;
        let cc = cs
            .columns
            .iter()
            .find(|c| c.name == child_col && !c.is_dropped())
            .ok_or_else(|| bad(format!("unknown column '{}'", child_col)))?;
        let pc = ps
            .columns
            .iter()
            .find(|c| c.name == parent_col && !c.is_dropped())
            .ok_or_else(|| bad(format!("unknown parent column '{}'", parent_col)))?;
        if !pc.is_primary_key {
            return Err(bad("foreign key must point to the parent's primary key"));
        }
        if cc.data_type != pc.data_type {
            return Err(bad(
                "foreign key column type differs from parent column type",
            ));
        }
        if cc.data_type == DataType::Bytes {
            return Err(bad("foreign key on bytes column is not supported"));
        }
        ensure_internal(
            self.server,
            FK_TABLE,
            "id int primary key, child_tbl text not null, child_col text not null, parent_tbl text not null, parent_col text not null, on_delete text",
        )?;
        let has_od = self
            .server
            .get_table_schema(FK_TABLE)?
            .is_some_and(|sc| sc.columns.iter().any(|c| c.name == "on_delete"));
        if !has_od {
            match self.server.table(FK_TABLE).sync("id int primary key, child_tbl text not null, child_col text not null, parent_tbl text not null, parent_col text not null, on_delete text") {
                Ok(_) => {}
                Err(e) if e.to_string().contains("already exists") => {}
                Err(e) => return Err(e),
            }
        }
        let fk = self.server.table(FK_TABLE);
        let t = |x: &str| Value::Text(x.to_string());
        let exists = fk
            .find()
            .columns(&["id"])
            .filter("child_tbl", "=", t(&self.name))
            .filter("child_col", "=", t(child_col))
            .filter("parent_tbl", "=", t(parent_tbl))
            .filter("parent_col", "=", t(parent_col))
            .limit(1)
            .run()?;
        if let Some(row) = exists.rows.first() {
            fk.update()
                .set("on_delete", t(if cascade { "cascade" } else { "restrict" }))
                .filter("id", "=", row[0].clone())
                .run()?;
            return Ok(());
        }
        fk.insert_auto(
            "id",
            &[
                ("child_tbl", t(&self.name)),
                ("child_col", t(child_col)),
                ("parent_tbl", t(parent_tbl)),
                ("parent_col", t(parent_col)),
                ("on_delete", t(if cascade { "cascade" } else { "restrict" })),
            ],
        )?;
        Ok(())
    }

    /// Lock held across FK check + write. None when this table has no FK role (no cost).
    fn fk_guard(&self, as_parent: bool) -> Result<Option<FkGuard>> {
        let involved = if as_parent {
            !self.fks_of_parent()?.is_empty()
        } else {
            !self.fks_of_child()?.is_empty()
        };
        if !involved {
            return Ok(None);
        }
        if FK_HELD.with(|h| h.get()) {
            return Ok(None);
        }
        let g = FK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        FK_HELD.with(|h| h.set(true));
        Ok(Some(FkGuard(Some(g))))
    }

    fn fk_rows(&self, by: &str, outs: &[&str]) -> Result<Vec<(String, String, String)>> {
        if self.name.starts_with("mitrdb_") || self.server.get_table_schema(FK_TABLE)?.is_none() {
            return Ok(Vec::new());
        }
        let r = self
            .server
            .table(FK_TABLE)
            .find()
            .columns(outs)
            .filter(by, "=", Value::Text(self.name.clone()))
            .run()?;
        Ok(r.rows
            .iter()
            .map(|row| (txt(&row[0]), txt(&row[1]), txt(&row[2])))
            .collect())
    }

    /// (child_col, parent_tbl, parent_col)
    fn fks_of_child(&self) -> Result<Vec<(String, String, String)>> {
        self.fk_rows("child_tbl", &["child_col", "parent_tbl", "parent_col"])
    }

    /// (child_tbl, child_col, parent_col)
    fn fks_of_parent(&self) -> Result<Vec<(String, String, String)>> {
        self.fk_rows("parent_tbl", &["child_tbl", "child_col", "parent_col"])
    }

    fn check_fks(&self, names: &[&str], values: &[Value]) -> Result<()> {
        for (ccol, ptbl, pcol) in self.fks_of_child()? {
            let Some(i) = names.iter().position(|n| *n == ccol) else {
                continue;
            };
            let v = &values[i];
            if matches!(v, Value::Null) {
                continue;
            }
            let n = self
                .with_name(&ptbl)
                .find()
                .columns(&[pcol.as_str()])
                .filter(&pcol, "=", v.clone())
                .limit(1)
                .run()?
                .rows
                .len();
            if n == 0 {
                return Err(bad(format!(
                    "foreign key: {}.{} = {:?} has no row in {}.{}",
                    self.name, ccol, v, ptbl, pcol
                )));
            }
        }
        Ok(())
    }
}

const CHECK_TABLE: &str = "mitrdb_check";

impl Table<'_> {
    /// CHECK: `column op literal`, op = `=` `!=` `>` `<` `>=` `<=` `like`. Literal Int64 or Text.
    /// Enforced on Table/ORM insert and update (NULL passes). Old rows are not verified.
    pub fn add_check(&self, col: &str, op: &str, value: Value) -> Result<()> {
        self.check_name()?;
        let op = op.to_lowercase();
        if !["=", "!=", ">", "<", ">=", "<=", "like"].contains(&op.as_str()) {
            return Err(bad(format!("unknown check operator '{}'", op)));
        }
        let schema = self
            .server
            .get_table_schema(&self.name)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.name)))?;
        let c = schema
            .columns
            .iter()
            .find(|c| c.name == col && !c.is_dropped())
            .ok_or_else(|| bad(format!("unknown column '{}'", col)))?;
        let (iv, tv) = match (&value, c.data_type) {
            (Value::Int64(n), DataType::Int64) if op != "like" => (Value::Int64(*n), Value::Null),
            (Value::Text(x), DataType::Text) => (Value::Null, Value::Text(x.clone())),
            _ => {
                return Err(bad(
                    "check value must match the column type (int or text; like = text)",
                ));
            }
        };
        ensure_internal(
            self.server,
            CHECK_TABLE,
            "id int primary key, tbl text not null, col text not null, op text not null, int_val int, text_val text",
        )?;
        let ck = self.server.table(CHECK_TABLE);
        let t = |x: &str| Value::Text(x.to_string());
        let exists = ck
            .find()
            .columns(&["id"])
            .filter("tbl", "=", t(&self.name))
            .filter("col", "=", t(col))
            .filter("op", "=", t(&op))
            .filter("int_val", "=", iv.clone())
            .filter("text_val", "=", tv.clone())
            .limit(1)
            .run()?;
        if !exists.rows.is_empty() {
            return Ok(());
        }
        ck.insert_auto(
            "id",
            &[
                ("tbl", t(&self.name)),
                ("col", t(col)),
                ("op", t(&op)),
                ("int_val", iv),
                ("text_val", tv),
            ],
        )?;
        Ok(())
    }

    fn check_checks(&self, names: &[&str], values: &[Value]) -> Result<()> {
        if self.name.starts_with("mitrdb_") || self.server.get_table_schema(CHECK_TABLE)?.is_none()
        {
            return Ok(());
        }
        let r = self
            .server
            .table(CHECK_TABLE)
            .find()
            .columns(&["col", "op", "int_val", "text_val"])
            .filter("tbl", "=", Value::Text(self.name.clone()))
            .run()?;
        for row in r.rows {
            let col = txt(&row[0]);
            let op = txt(&row[1]);
            let rhs = match (&row[2], &row[3]) {
                (Value::Int64(n), _) => Value::Int64(*n),
                (_, Value::Text(x)) => Value::Text(x.clone()),
                _ => continue,
            };
            let Some(i) = names.iter().position(|n| *n == col) else {
                continue;
            };
            let v = &values[i];
            if matches!(v, Value::Null) {
                continue;
            }
            use mitrdb_query::CompareOp as C;
            let cop = match op.as_str() {
                "=" => C::Eq,
                "!=" => C::Ne,
                ">" => C::Gt,
                "<" => C::Lt,
                ">=" => C::Ge,
                "<=" => C::Le,
                _ => C::Like,
            };
            if !cop.evaluate(v, &rhs) {
                return Err(bad(format!(
                    "CHECK failed: {}.{} {} {:?} (got {:?})",
                    self.name, col, op, rhs, v
                )));
            }
        }
        Ok(())
    }
}

impl Table<'_> {
    /// Composite (multi-column) index, 2..=4 columns. Safe to call again.
    /// `db.table("likes").create_index("likes_u_p", &["user_id", "post_id"], true)?`
    pub fn create_index(&self, name: &str, cols: &[&str], unique: bool) -> Result<()> {
        self.check_name()?;
        if cols.is_empty()
            || cols.len() > 4
            || !valid_ident(name)
            || !cols.iter().all(|c| valid_ident(c))
        {
            return Err(bad("bad index name or columns (1..=4 columns)"));
        }
        let sql = format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
            if unique { "UNIQUE " } else { "" },
            name,
            self.name,
            cols.join(", ")
        );
        self.server.execute_sql_internal(&sql).map(|_| ())
    }
}

const DEFAULT_TABLE: &str = "mitrdb_default";

impl Table<'_> {
    fn fk_is_cascade(&self, child_tbl: &str, child_col: &str, parent_col: &str) -> Result<bool> {
        if self.server.get_table_schema(FK_TABLE)?.is_none() {
            return Ok(false);
        }
        let t = |x: &str| Value::Text(x.to_string());
        let r = self
            .server
            .table(FK_TABLE)
            .find()
            .columns(&["on_delete"])
            .filter("parent_tbl", "=", t(&self.name))
            .filter("child_tbl", "=", t(child_tbl))
            .filter("child_col", "=", t(child_col))
            .filter("parent_col", "=", t(parent_col))
            .limit(1)
            .run()?;
        Ok(matches!(r.rows.first().and_then(|x| x.first()), Some(Value::Text(m)) if m == "cascade"))
    }

    /// Default for `col` (Int64/Timestamp, Text or Bool). Used by `insert` when the column is not given.
    /// Table/ORM API only; raw SQL ignores it. Safe to call again (replaces the old default).
    pub fn set_default(&self, col: &str, value: Value) -> Result<()> {
        self.check_name()?;
        let schema = self
            .server
            .get_table_schema(&self.name)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.name)))?;
        let c = schema
            .columns
            .iter()
            .find(|c| c.name == col && !c.is_dropped())
            .ok_or_else(|| bad(format!("unknown column '{}'", col)))?;
        let (iv, tv, bv) = match (&value, c.data_type) {
            (Value::Int64(n), DataType::Int64 | DataType::Timestamp) => {
                (Value::Int64(*n), Value::Null, Value::Null)
            }
            (Value::Text(x), DataType::Text) => (Value::Null, Value::Text(x.clone()), Value::Null),
            (Value::Bool(b), DataType::Bool) => (Value::Null, Value::Null, Value::Bool(*b)),
            _ => {
                return Err(bad(
                    "default must be int, text or bool and match the column type",
                ));
            }
        };
        ensure_internal(
            self.server,
            DEFAULT_TABLE,
            "id int primary key, tbl text not null, col text not null, int_val int, text_val text, bool_val bool",
        )?;
        let d = self.server.table(DEFAULT_TABLE);
        let t = |x: &str| Value::Text(x.to_string());
        d.delete()
            .filter("tbl", "=", t(&self.name))
            .filter("col", "=", t(col))
            .run()?;
        d.insert_auto(
            "id",
            &[
                ("tbl", t(&self.name)),
                ("col", t(col)),
                ("int_val", iv),
                ("text_val", tv),
                ("bool_val", bv),
            ],
        )?;
        Ok(())
    }

    fn load_defaults(&self) -> Result<std::collections::HashMap<String, Value>> {
        let mut m = std::collections::HashMap::new();
        if self.name.starts_with("mitrdb_")
            || self.server.get_table_schema(DEFAULT_TABLE)?.is_none()
        {
            return Ok(m);
        }
        let r = self
            .server
            .table(DEFAULT_TABLE)
            .find()
            .columns(&["col", "int_val", "text_val", "bool_val"])
            .filter("tbl", "=", Value::Text(self.name.clone()))
            .run()?;
        for row in r.rows {
            let v = match (&row[1], &row[2], &row[3]) {
                (Value::Int64(n), _, _) => Value::Int64(*n),
                (_, Value::Text(x), _) => Value::Text(x.clone()),
                (_, _, Value::Bool(b)) => Value::Bool(*b),
                _ => continue,
            };
            m.insert(txt(&row[0]), v);
        }
        Ok(m)
    }
}

impl Query<'_> {
    /// One OR group: `(c1 OR c2 ...)`, combined with the other conditions by AND.
    pub fn filter_any(mut self, group: Vec<(String, String, Value)>) -> Self {
        self.anys.push(group);
        self
    }
}
impl Update<'_> {
    pub fn filter_any(mut self, group: Vec<(String, String, Value)>) -> Self {
        self.anys.push(group);
        self
    }
}
impl Delete<'_> {
    pub fn filter_any(mut self, group: Vec<(String, String, Value)>) -> Self {
        self.anys.push(group);
        self
    }
}

/// Result of `find().run()`.
#[derive(Debug, Clone)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// Built by `table.find()`. Chain `filter`, `columns`, `order_by`, `limit`, `offset`, then `run()`.
pub struct Query<'a> {
    server: &'a Server,
    session_id: Option<crate::SessionId>,
    table: String,
    conds: Vec<(String, String, Value)>,
    anys: Vec<Vec<(String, String, Value)>>,
    cols: Option<Vec<String>>,
    order: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'a> Query<'a> {
    /// Bind this query to a specific session instead of the default session.
    pub fn in_session(mut self, session_id: crate::SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    fn exec_sql_params(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        match self.session_id {
            Some(sid) => self
                .server
                .execute_sql_params_in_session_internal(sid, sql, params),
            None => self.server.execute_sql_params_internal(sql, params),
        }
    }
}

impl<'a> Table<'a> {
    /// Start a query: `db.table("users").find().filter("age", ">", val(26)).run()?`
    pub fn find(&self) -> Query<'a> {
        Query {
            server: self.server,
            session_id: self.session_id,
            table: self.name.clone(),
            conds: Vec::new(),
            anys: Vec::new(),
            cols: None,
            order: None,
            limit: None,
            offset: None,
        }
    }
}

fn lit(v: &Value) -> Result<String> {
    match v {
        Value::Null => Ok("NULL".to_string()),
        Value::Int64(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Text(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Value::Bytes(_) => Err(bad("bytes values cannot be used in a filter")),
        Value::BlobRef(_, _, _) => Err(bad("blob_ref values cannot be used in a filter")),
        Value::Float(f) if !f.0.is_nan() => Ok(format!("{:?}", f.0)),
        Value::Float(_) => Err(bad("NaN cannot be used")),
        Value::Param(_) => Err(bad("parameter placeholders are not allowed here")),
    }
}

impl Query<'_> {
    /// Add a condition (all conditions are combined with AND).
    /// Operators: =  !=  >  <  >=  <=   (value Null with = or != means IS NULL / IS NOT NULL)
    pub fn filter(mut self, column: &str, op: &str, value: Value) -> Self {
        self.conds.push((column.to_string(), op.to_string(), value));
        self
    }

    /// Only return these columns.
    pub fn columns(mut self, cols: &[&str]) -> Self {
        self.cols = Some(cols.iter().map(|c| c.to_string()).collect());
        self
    }

    pub fn order_by(mut self, column: &str) -> Self {
        self.order = Some((column.to_string(), false));
        self
    }

    pub fn order_by_desc(mut self, column: &str) -> Self {
        self.order = Some((column.to_string(), true));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    pub fn run(self) -> Result<Rows> {
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);

        let mut sql = String::from("SELECT ");
        match &self.cols {
            None => sql.push('*'),
            Some(cols) => {
                if cols.is_empty() {
                    return Err(bad("columns() needs at least one column"));
                }
                for c in cols {
                    if !known(c) {
                        return Err(bad(format!("unknown column '{}'", c)));
                    }
                }
                sql.push_str(&cols.join(", "));
            }
        }
        sql.push_str(&format!(" FROM {}", self.table));

        let mut params: Vec<Value> = Vec::new();
        sql.push_str(&where_sql(&known, &self.conds, &self.anys, &mut params)?);
        if let Some((col, desc)) = &self.order {
            if !known(col) {
                return Err(bad(format!("unknown column '{}'", col)));
            }
            sql.push_str(&format!(
                " ORDER BY {}{}",
                col,
                if *desc { " DESC" } else { "" }
            ));
        }
        if let Some(n) = self.limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }
        if let Some(n) = self.offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }

        match self.exec_sql_params(&sql, &params)? {
            ExecutionResult::Rows { columns, rows } => Ok(Rows { columns, rows }),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }
}

fn cond_sql(
    known: &dyn Fn(&str) -> bool,
    col: &str,
    op: &str,
    v: &Value,
    params: &mut Vec<Value>,
) -> Result<String> {
    if !known(col) {
        return Err(bad(format!("unknown column '{}'", col)));
    }
    Ok(match (op, v) {
        ("=", Value::Null) => format!("{} IS NULL", col),
        ("!=", Value::Null) => format!("{} IS NOT NULL", col),
        (_, Value::Null) => return Err(bad("NULL can only be used with = or !=")),
        ("like" | "LIKE", Value::Text(_)) => {
            params.push(v.clone());
            format!("{} LIKE ${}", col, params.len())
        }
        ("like" | "LIKE", _) => return Err(bad("LIKE needs a text pattern")),
        ("=" | "!=" | ">" | "<" | ">=" | "<=", _) => {
            lit(v)?;
            params.push(v.clone());
            format!("{} {} ${}", col, op, params.len())
        }
        _ => return Err(bad(format!("unknown operator '{}'", op))),
    })
}

fn where_sql(
    known: &dyn Fn(&str) -> bool,
    conds: &[(String, String, Value)],
    anys: &[Vec<(String, String, Value)>],
    params: &mut Vec<Value>,
) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();
    for (col, op, v) in conds {
        parts.push(cond_sql(known, col, op, v, params)?);
    }
    for g in anys {
        if g.is_empty() {
            return Err(bad("empty OR / IN list"));
        }
        let mut ps = Vec::new();
        for (col, op, v) in g {
            ps.push(cond_sql(known, col, op, v, params)?);
        }
        parts.push(if ps.len() == 1 {
            ps.remove(0)
        } else {
            format!("({})", ps.join(" OR "))
        });
    }
    if parts.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" WHERE {}", parts.join(" AND ")))
    }
}

/// Built by `table.update()`. Chain `set`, `filter` (or `all`), then `run()`.
pub struct Update<'a> {
    server: &'a Server,
    session_id: Option<crate::SessionId>,
    table: String,
    sets: Vec<(String, Value)>,
    conds: Vec<(String, String, Value)>,
    anys: Vec<Vec<(String, String, Value)>>,
    all: bool,
}

/// Built by `table.delete()`. Chain `filter` (or `all`), then `run()`.
pub struct Delete<'a> {
    server: &'a Server,
    session_id: Option<crate::SessionId>,
    table: String,
    conds: Vec<(String, String, Value)>,
    anys: Vec<Vec<(String, String, Value)>>,
    all: bool,
}

impl<'a> Update<'a> {
    pub fn in_session(mut self, session_id: crate::SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    fn table_ref(&self) -> Table<'a> {
        Table {
            server: self.server,
            name: self.table.clone(),
            session_id: self.session_id,
        }
    }
}

impl<'a> Delete<'a> {
    pub fn in_session(mut self, session_id: crate::SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    fn table_ref(&self) -> Table<'a> {
        Table {
            server: self.server,
            name: self.table.clone(),
            session_id: self.session_id,
        }
    }
}

impl<'a> Table<'a> {
    /// `db.table("users").update().set("age", val(26)).filter("id", "=", val(1)).run()?`
    pub fn update(&self) -> Update<'a> {
        Update {
            server: self.server,
            session_id: self.session_id,
            table: self.name.clone(),
            sets: Vec::new(),
            conds: Vec::new(),
            anys: Vec::new(),
            all: false,
        }
    }

    /// `db.table("users").delete().filter("id", "=", val(1)).run()?`
    pub fn delete(&self) -> Delete<'a> {
        Delete {
            server: self.server,
            session_id: self.session_id,
            table: self.name.clone(),
            conds: Vec::new(),
            anys: Vec::new(),
            all: false,
        }
    }
}

impl Update<'_> {
    pub fn set(mut self, column: &str, value: Value) -> Self {
        self.sets.push((column.to_string(), value));
        self
    }

    pub fn filter(mut self, column: &str, op: &str, value: Value) -> Self {
        self.conds.push((column.to_string(), op.to_string(), value));
        self
    }

    /// Explicitly update every row (needed when there is no filter).
    pub fn all(mut self) -> Self {
        self.all = true;
        self
    }

    /// Returns how many rows were changed.
    pub fn run(self) -> Result<usize> {
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        if self.sets.is_empty() {
            return Err(bad("update needs at least one set(...)"));
        }
        if self.conds.is_empty() && self.anys.iter().all(|g| g.is_empty()) && !self.all {
            return Err(bad("update without filter: add filter(...) or all()"));
        }

        let mut params: Vec<Value> = Vec::new();
        let mut assigns: Vec<String> = Vec::new();
        for (i, (name, v)) in self.sets.iter().enumerate() {
            let col = schema
                .columns
                .iter()
                .find(|c| c.name == *name)
                .ok_or_else(|| bad(format!("unknown column '{}'", name)))?;
            if col.is_primary_key {
                return Err(bad(format!("cannot update primary key column '{}'", name)));
            }
            if self.sets[..i].iter().any(|(n, _)| n == name) {
                return Err(bad(format!("column '{}' set twice", name)));
            }
            check_value(&self.table, col, v)?;
            if !matches!(v, Value::BlobRef(_, _, _)) {
                lit(v)?;
            }
            params.push(v.clone());
            assigns.push(format!("{} = ${}", name, params.len()));
        }

        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        let tbl = self.table_ref();
        let _fk_guard = tbl.fk_guard(false)?;
        {
            let names: Vec<&str> = self.sets.iter().map(|(n, _)| n.as_str()).collect();
            let vals: Vec<Value> = self.sets.iter().map(|(_, v)| v.clone()).collect();
            tbl.check_fks(&names, &vals)?;
            tbl.check_checks(&names, &vals)?;
        }
        let wh = where_sql(&known, &self.conds, &self.anys, &mut params)?;
        let sql = format!("UPDATE {} SET {}{}", self.table, assigns.join(", "), wh);
        match tbl.exec_sql_params(&sql, &params)? {
            ExecutionResult::Affected(n) => Ok(n),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }
}

impl Delete<'_> {
    pub fn filter(mut self, column: &str, op: &str, value: Value) -> Self {
        self.conds.push((column.to_string(), op.to_string(), value));
        self
    }

    /// Explicitly delete every row (needed when there is no filter).
    pub fn all(mut self) -> Self {
        self.all = true;
        self
    }

    /// Returns how many rows were deleted.
    pub fn run(self) -> Result<usize> {
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        if self.conds.is_empty() && self.anys.iter().all(|g| g.is_empty()) && !self.all {
            return Err(bad("delete without filter: add filter(...) or all()"));
        }
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        let mut params: Vec<Value> = Vec::new();
        let wh = where_sql(&known, &self.conds, &self.anys, &mut params)?;
        let t = self.table_ref();
        let _fk_guard = t.fk_guard(true)?;
        {
            for (ctbl, ccol, pcol) in t.fks_of_parent()? {
                let mut q = t.find().columns(&[pcol.as_str()]);
                for (c, o, v) in &self.conds {
                    q = q.filter(c, o, v.clone());
                }
                for g in &self.anys {
                    q = q.filter_any(g.clone());
                }
                for row in q.run()?.rows {
                    let v = row[0].clone();
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    if t.fk_is_cascade(&ctbl, &ccol, &pcol)? {
                        t.with_name(&ctbl)
                            .delete()
                            .filter(&ccol, "=", v.clone())
                            .run()?;
                        continue;
                    }
                    let used = t
                        .with_name(&ctbl)
                        .find()
                        .columns(&[ccol.as_str()])
                        .filter(&ccol, "=", v.clone())
                        .limit(1)
                        .run()?
                        .rows
                        .len();
                    if used > 0 {
                        return Err(bad(format!(
                            "foreign key: {}.{} = {:?} is still used by {}.{}",
                            self.table, pcol, v, ctbl, ccol
                        )));
                    }
                }
            }
        }
        let sql = format!("DELETE FROM {}{}", self.table, wh);
        match t.exec_sql_params(&sql, &params)? {
            ExecutionResult::Affected(n) => Ok(n),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }
}

impl Query<'_> {
    /// Number of rows that match the filters. (Do not combine with limit/offset/order.)
    pub fn count(self) -> Result<usize> {
        if self.limit.is_some() || self.offset.is_some() {
            return Err(bad("count() cannot be used with limit() or offset()"));
        }
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        let mut params: Vec<Value> = Vec::new();
        let wh = where_sql(&known, &self.conds, &self.anys, &mut params)?;
        let sql = format!("SELECT COUNT(*) FROM {}{}", self.table, wh);
        match self.exec_sql_params(&sql, &params)? {
            ExecutionResult::Rows { rows, .. } => match rows.first().and_then(|r| r.first()) {
                Some(Value::Int64(n)) if *n >= 0 => Ok(*n as usize),
                other => Err(bad(format!("unexpected COUNT result: {other:?}"))),
            },
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }

    /// `SUM`/`AVG`/`MIN`/`MAX` of one column over the matching rows. NULL if no rows.
    /// SUM/AVG: int columns only (AVG truncates). MIN/MAX: int, text, timestamp, float.
    pub fn aggregate(self, func: &str, col: &str) -> Result<Value> {
        let f = func.to_uppercase();
        if !["SUM", "AVG", "MIN", "MAX"].contains(&f.as_str()) {
            return Err(bad(format!("unknown aggregate '{}'", func)));
        }
        if self.limit.is_some() || self.offset.is_some() {
            return Err(bad("aggregate() cannot be used with limit() or offset()"));
        }
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        if !known(col) {
            return Err(bad(format!("unknown column '{}'", col)));
        }
        let mut params: Vec<Value> = Vec::new();
        let wh = where_sql(&known, &self.conds, &self.anys, &mut params)?;
        let sql = format!("SELECT {}({}) FROM {}{}", f, col, self.table, wh);
        match self.exec_sql_params(&sql, &params)? {
            ExecutionResult::Rows { rows, .. } => rows
                .first()
                .and_then(|r| r.first())
                .cloned()
                .ok_or_else(|| bad("empty aggregate result")),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }

    /// `SELECT col, COUNT(*) ... GROUP BY col`. `min_count` = HAVING COUNT(*) >= n,
    /// `by_count_desc` = biggest groups first, `top` = LIMIT. Returns (group value, count).
    pub fn group_count(
        self,
        col: &str,
        min_count: Option<i64>,
        by_count_desc: bool,
        top: Option<usize>,
    ) -> Result<Vec<(Value, i64)>> {
        if self.limit.is_some() || self.offset.is_some() {
            return Err(bad("group_count() cannot be used with limit() or offset()"));
        }
        if !valid_ident(&self.table) {
            return Err(bad(format!("bad table name: '{}'", self.table)));
        }
        let schema = self
            .server
            .get_table_schema(&self.table)?
            .ok_or_else(|| bad(format!("table '{}' not found", self.table)))?;
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        if !known(col) {
            return Err(bad(format!("unknown column '{}'", col)));
        }
        let mut params: Vec<Value> = Vec::new();
        let wh = where_sql(&known, &self.conds, &self.anys, &mut params)?;
        let mut sql = format!(
            "SELECT {col}, COUNT(*) FROM {}{} GROUP BY {col}",
            self.table, wh
        );
        if let Some(n) = min_count {
            sql.push_str(&format!(" HAVING COUNT(*) >= {}", n));
        }
        if by_count_desc {
            sql.push_str(" ORDER BY COUNT(*) DESC");
        }
        if let Some(n) = top {
            sql.push_str(&format!(" LIMIT {}", n));
        }
        match self.exec_sql_params(&sql, &params)? {
            ExecutionResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|r| match (r.first(), r.get(1)) {
                    (Some(g), Some(Value::Int64(n))) => Ok((g.clone(), *n)),
                    _ => Err(bad("unexpected GROUP BY row")),
                })
                .collect(),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }

    /// First matching row (in the table's column order), or None.
    pub fn one(self) -> Result<Option<Vec<Value>>> {
        Ok(self.limit(1).run()?.rows.into_iter().next())
    }
}

impl Table<'_> {
    /// Number of rows in the table.
    pub fn count(&self) -> Result<usize> {
        self.find().count()
    }
}

impl Server {
    /// Raw SQL / TCP / CLI path: refuse INSERT/UPDATE/DELETE on tables that have FK / CHECK /
    /// DEFAULT rules (only the Table/ORM API enforces them). Table/ORM uses the *_internal calls.
    pub(crate) fn guard_raw_dml(&self, plan: &PhysicalPlan) -> Result<()> {
        let name = match plan {
            PhysicalPlan::Insert { schema, .. }
            | PhysicalPlan::InsertMany { schema, .. }
            | PhysicalPlan::Update { schema, .. }
            | PhysicalPlan::Delete { schema, .. } => &schema.name,
            _ => return Ok(()),
        };
        if name.starts_with("mitrdb_") {
            return Ok(());
        }
        let t = self.table(name);
        let why = if !t.fks_of_child()?.is_empty() {
            "foreign key"
        } else if !t.fks_of_parent()?.is_empty() {
            "foreign key (referenced as parent)"
        } else if self.def_rows_exist(CHECK_TABLE, name)? {
            "CHECK"
        } else if self.def_rows_exist(DEFAULT_TABLE, name)? {
            "DEFAULT"
        } else {
            return Ok(());
        };
        Err(bad(format!(
            "raw SQL write refused on table '{}': it has {} rules that only the Table/ORM API enforces",
            name, why
        )))
    }

    fn def_rows_exist(&self, def_table: &str, name: &str) -> Result<bool> {
        if self.get_table_schema(def_table)?.is_none() {
            return Ok(false);
        }
        Ok(!self
            .table(def_table)
            .find()
            .columns(&["tbl"])
            .filter("tbl", "=", Value::Text(name.to_string()))
            .limit(1)
            .run()?
            .rows
            .is_empty())
    }
}
