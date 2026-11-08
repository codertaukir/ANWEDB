use mitrdb_catalog::{ColumnDef, DataType};
use mitrdb_common::{Error, Result};

use crate::Server;

/// Easy table helper: `server.table("users").sync("id int primary key, name text")?`
pub struct Table<'a> {
    server: &'a Server,
    name: String,
}

impl Server {
    pub fn table(&self, name: &str) -> Table<'_> {
        Table {
            server: self,
            name: name.to_string(),
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
        let is_pk = wanted.iter().any(|c| c.name == tokens[0] && c.is_primary_key);
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
        let is_pk = wanted.iter().any(|c| c.name == tokens[0] && c.is_primary_key);
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
                                    if w.nullable { "DROP NOT NULL" } else { "SET NOT NULL" }
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
            if existing_idx.iter().any(|i| i.unique && i.column_name == *col) {
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
            self.server.execute_sql(sql)?;
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
            .execute_sql(&format!("ALTER TABLE {} DROP COLUMN {}", self.name, name))
            .map(|_| ())
    }

    /// Rename a column, keeping data.
    pub fn rename(&self, old: &str, new: &str) -> Result<()> {
        self.check_name()?;
        if !valid_ident(old) || !valid_ident(new) {
            return Err(bad("bad column name"));
        }
        self.server
            .execute_sql(&format!(
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
            .execute_sql(&format!(
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
        if let Some(d) = default {
            let lit = match d {
                Value::Int64(n) => n.to_string(),
                Value::Text(t) => format!("'{}'", t.replace('\'', "''")),
                Value::Bool(b) => String::from(if b { "TRUE" } else { "FALSE" }),
                _ => return Err(bad("default must be an int, text or bool value")),
            };
            self.server.execute_sql(&format!(
                "UPDATE {} SET {} = {} WHERE {} IS NULL",
                self.name, name, lit, name
            ))?;
        }
        self.server
            .execute_sql(&format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                self.name, name
            ))
            .map(|_| ())
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
            .execute_sql(&format!(
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
                Err(bad(format!("NOT NULL constraint failed: {}.{}", table, col.name)))
            } else {
                Ok(())
            }
        }
        Value::Int64(_) if col.data_type == DataType::Int64 => Ok(()),
        Value::Text(_) if col.data_type == DataType::Text => Ok(()),
        Value::Bool(_) if col.data_type == DataType::Bool => Ok(()),
        Value::Bytes(_) if col.data_type == DataType::Bytes => Ok(()),
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

        let mut values = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            let v = row
                .iter()
                .find(|(n, _)| *n == col.name)
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null);
            check_value(&self.name, col, &v)?;
            values.push(v);
        }

        match self.server.execute_plan(PhysicalPlan::Insert { schema, values })? {
            ExecutionResult::Affected(n) => Ok(n),
            _ => Ok(1),
        }
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
    table: String,
    conds: Vec<(String, String, Value)>,
    cols: Option<Vec<String>>,
    order: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'a> Table<'a> {
    /// Start a query: `db.table("users").find().filter("age", ">", val(26)).run()?`
    pub fn find(&self) -> Query<'a> {
        Query {
            server: self.server,
            table: self.name.clone(),
            conds: Vec::new(),
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
        sql.push_str(&where_sql(&known, &self.conds, &mut params)?);
        if let Some((col, desc)) = &self.order {
            if !known(col) {
                return Err(bad(format!("unknown column '{}'", col)));
            }
            sql.push_str(&format!(" ORDER BY {}{}", col, if *desc { " DESC" } else { "" }));
        }
        if let Some(n) = self.limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }
        if let Some(n) = self.offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }

        match self.server.execute_sql_params(&sql, &params)? {
            ExecutionResult::Rows { columns, rows } => Ok(Rows { columns, rows }),
            other => Err(bad(format!("unexpected result: {other:?}"))),
        }
    }
}

fn where_sql(
    known: &dyn Fn(&str) -> bool,
    conds: &[(String, String, Value)],
    params: &mut Vec<Value>,
) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();
    for (col, op, v) in conds {
        if !known(col) {
            return Err(bad(format!("unknown column '{}'", col)));
        }
        let part = match (op.as_str(), v) {
            ("=", Value::Null) => format!("{} IS NULL", col),
            ("!=", Value::Null) => format!("{} IS NOT NULL", col),
            (_, Value::Null) => return Err(bad("NULL can only be used with = or !=")),
            ("=" | "!=" | ">" | "<" | ">=" | "<=", _) => {
                lit(v)?;
                params.push(v.clone());
                format!("{} {} ${}", col, op, params.len())
            }
            _ => return Err(bad(format!("unknown operator '{}'", op))),
        };
        parts.push(part);
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
    table: String,
    sets: Vec<(String, Value)>,
    conds: Vec<(String, String, Value)>,
    all: bool,
}

/// Built by `table.delete()`. Chain `filter` (or `all`), then `run()`.
pub struct Delete<'a> {
    server: &'a Server,
    table: String,
    conds: Vec<(String, String, Value)>,
    all: bool,
}

impl<'a> Table<'a> {
    /// `db.table("users").update().set("age", val(26)).filter("id", "=", val(1)).run()?`
    pub fn update(&self) -> Update<'a> {
        Update {
            server: self.server,
            table: self.name.clone(),
            sets: Vec::new(),
            conds: Vec::new(),
            all: false,
        }
    }

    /// `db.table("users").delete().filter("id", "=", val(1)).run()?`
    pub fn delete(&self) -> Delete<'a> {
        Delete {
            server: self.server,
            table: self.name.clone(),
            conds: Vec::new(),
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
        if self.conds.is_empty() && !self.all {
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
            lit(v)?;
            params.push(v.clone());
            assigns.push(format!("{} = ${}", name, params.len()));
        }

        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        let wh = where_sql(&known, &self.conds, &mut params)?;
        let sql = format!("UPDATE {} SET {}{}", self.table, assigns.join(", "), wh);
        match self.server.execute_sql_params(&sql, &params)? {
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
        if self.conds.is_empty() && !self.all {
            return Err(bad("delete without filter: add filter(...) or all()"));
        }
        let known = |c: &str| schema.columns.iter().any(|x| x.name == c);
        let mut params: Vec<Value> = Vec::new();
        let wh = where_sql(&known, &self.conds, &mut params)?;
        let sql = format!("DELETE FROM {}{}", self.table, wh);
        match self.server.execute_sql_params(&sql, &params)? {
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
        let wh = where_sql(&known, &self.conds, &mut params)?;
        let sql = format!("SELECT COUNT(*) FROM {}{}", self.table, wh);
        match self.server.execute_sql_params(&sql, &params)? {
            ExecutionResult::Rows { rows, .. } => match rows.first().and_then(|r| r.first()) {
                Some(Value::Int64(n)) if *n >= 0 => Ok(*n as usize),
                other => Err(bad(format!("unexpected COUNT result: {other:?}"))),
            },
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
