//! mitrdb-orm: struct = schema. `db.sync::<User>()`, `db.insert(&user)`,
//! `db.find(User::age.gt(18)).order_by(User::id).limit(10).run()`.

use std::marker::PhantomData;

pub use mitrdb_orm_derive::Model;

use mitrdb_server::Query;
pub use mitrdb_server::{Error, Result, Server, Value};

fn err(msg: impl Into<String>) -> Error {
    Error::TransactionError(msg.into())
}

/// A Rust type that can live in a column.
pub trait Field: Sized {
    const SQL_TYPE: &'static str;
    const NULLABLE: bool = false;
    fn to_value(&self) -> Value;
    fn from_value(v: Value) -> Result<Self>;
}

impl Field for i64 {
    const SQL_TYPE: &'static str = "int";
    fn to_value(&self) -> Value {
        Value::Int64(*self)
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Int64(x) => Ok(x),
            _ => Err(err("expected int value")),
        }
    }
}

impl Field for i32 {
    const SQL_TYPE: &'static str = "int";
    fn to_value(&self) -> Value {
        Value::Int64(i64::from(*self))
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Int64(x) => i32::try_from(x).map_err(|_| err("int value out of range for i32")),
            _ => Err(err("expected int value")),
        }
    }
}

impl Field for String {
    const SQL_TYPE: &'static str = "text";
    fn to_value(&self) -> Value {
        Value::Text(self.clone())
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Text(x) => Ok(x),
            _ => Err(err("expected text value")),
        }
    }
}

impl Field for bool {
    const SQL_TYPE: &'static str = "bool";
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Bool(x) => Ok(x),
            _ => Err(err("expected bool value")),
        }
    }
}

impl Field for Vec<u8> {
    const SQL_TYPE: &'static str = "bytes";
    fn to_value(&self) -> Value {
        Value::Bytes(self.clone())
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Bytes(x) => Ok(x),
            _ => Err(err("expected bytes value")),
        }
    }
}

impl<T: Field> Field for Option<T> {
    const SQL_TYPE: &'static str = T::SQL_TYPE;
    const NULLABLE: bool = true;
    fn to_value(&self) -> Value {
        match self {
            Some(x) => x.to_value(),
            None => Value::Null,
        }
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Null => Ok(None),
            v => T::from_value(v).map(Some),
        }
    }
}

/// f64 is stored as an order-preserving i64 (so `int` column, range filters and order_by work).
/// -0.0 is stored as 0.0. NaN is rejected by `from_value` only if it cannot round-trip; avoid NaN.
#[allow(dead_code)]
fn f64_enc(f: f64) -> i64 {
    let f = if f == 0.0 { 0.0 } else { f };
    let b = f.to_bits() as i64;
    if b < 0 { b ^ i64::MAX } else { b }
}
#[allow(dead_code)]
fn f64_dec(v: i64) -> f64 {
    f64::from_bits((if v < 0 { v ^ i64::MAX } else { v }) as u64)
}

impl Field for f64 {
    const SQL_TYPE: &'static str = "float";
    fn to_value(&self) -> Value {
        Value::Float(mitrdb_server::F64::new(*self))
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Float(x) => Ok(x.0),
            _ => Err(err("expected float value")),
        }
    }
}

/// Microseconds since UNIX epoch. Stored as a plain `int` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(d.as_micros() as i64)
    }
}

impl From<i64> for Timestamp {
    fn from(x: i64) -> Self {
        Timestamp(x)
    }
}

impl Field for Timestamp {
    const SQL_TYPE: &'static str = "timestamp";
    fn to_value(&self) -> Value {
        Value::Int64(self.0)
    }
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Int64(x) => Ok(Timestamp(x)),
            _ => Err(err("expected int value")),
        }
    }
}

/// Row with `created_at` / `updated_at` columns filled with now().
fn stamped<M: Model>(m: &M) -> Vec<(&'static str, Value)> {
    let now = Value::Int64(Timestamp::now().0);
    m.to_row()
        .into_iter()
        .map(|(n, v)| {
            if Some(n) == M::CREATED_AT || Some(n) == M::UPDATED_AT {
                (n, now.clone())
            } else {
                (n, v)
            }
        })
        .collect()
}

/// Static description of one column (macro will generate these).
#[derive(Debug, Clone, Copy)]
pub struct ColumnMeta {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub index: bool,
    pub auto: bool,
    pub references: Option<&'static str>,
    pub check: Option<&'static str>,
    pub cascade: bool,
    pub default: Option<&'static str>,
}

impl ColumnMeta {
    pub const fn of<T: Field>(name: &'static str) -> Self {
        ColumnMeta {
            name,
            sql_type: T::SQL_TYPE,
            nullable: T::NULLABLE,
            primary_key: false,
            unique: false,
            index: false,
            auto: false,
            references: None,
            check: None,
            cascade: false,
            default: None,
        }
    }
    pub const fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self
    }
    pub const fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
    pub const fn index(mut self) -> Self {
        self.index = true;
        self
    }
    /// Foreign key: `"table.column"` (parent column must be its primary key).
    pub const fn references(mut self, r: &'static str) -> Self {
        self.references = Some(r);
        self
    }
    /// CHECK conditions joined by `;`, e.g. `">= 0"` or `"like 'A%'"`.
    pub const fn check(mut self, c: &'static str) -> Self {
        self.check = Some(c);
        self
    }
    pub const fn cascade(mut self) -> Self {
        self.cascade = true;
        self
    }
    /// Default literal: `'text'`, `0`, `true`. Applied when an Option field is None.
    pub const fn default_value(mut self, d: &'static str) -> Self {
        self.default = Some(d);
        self
    }
    pub const fn auto(mut self) -> Self {
        self.auto = true;
        self
    }
}

/// Spec string for `Table::sync`, e.g. "id int primary key, email text not null unique".
pub fn spec_of(cols: &[ColumnMeta]) -> String {
    cols.iter()
        .map(|c| {
            let mut s = format!("{} {}", c.name, c.sql_type);
            if c.primary_key {
                s.push_str(" primary key");
            } else if !c.nullable {
                s.push_str(" not null");
            }
            if c.unique {
                s.push_str(" unique");
            } else if c.index {
                s.push_str(" index");
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// One result row, with column names.
pub struct RowRef<'a> {
    pub columns: &'a [String],
    pub values: &'a [Value],
}

impl RowRef<'_> {
    pub fn get<T: Field>(&self, name: &str) -> Result<T> {
        let i = self
            .columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| err(format!("column '{name}' missing in result")))?;
        T::from_value(self.values[i].clone()).map_err(|e| err(format!("column '{name}': {e:?}")))
    }
}

/// A struct that maps to a table.
pub trait Model: Sized {
    const TABLE: &'static str;
    const COLUMNS: &'static [ColumnMeta];
    /// Name of the `#[col(auto)]` column, if any.
    const AUTO_PK: Option<&'static str> = None;
    const CREATED_AT: Option<&'static str> = None;
    const UPDATED_AT: Option<&'static str> = None;
    /// Composite indexes: ("col_a,col_b", unique).
    const INDEXES: &'static [(&'static str, bool)] = &[];
    fn to_row(&self) -> Vec<(&'static str, Value)>;
    fn from_row(row: &RowRef) -> Result<Self>;
}

/// Typed column handle: `User::age` is `Col<User, Option<i32>>`.
pub struct Col<M, T> {
    pub name: &'static str,
    _p: PhantomData<fn() -> (M, T)>,
}

impl<M, T> Clone for Col<M, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M, T> Copy for Col<M, T> {}

impl<M, T> Col<M, T> {
    pub const fn new(name: &'static str) -> Self {
        Col {
            name,
            _p: PhantomData,
        }
    }
}

/// One condition, built from a column: `User::age.gt(18)`.
pub struct Cond<M> {
    col: &'static str,
    op: &'static str,
    value: Value,
    _p: PhantomData<fn() -> M>,
}

impl<M, T: Field> Col<M, T> {
    fn cond(&self, op: &'static str, v: impl Into<T>) -> Cond<M> {
        Cond {
            col: self.name,
            op,
            value: v.into().to_value(),
            _p: PhantomData,
        }
    }
    pub fn eq(&self, v: impl Into<T>) -> Cond<M> {
        self.cond("=", v)
    }
    pub fn ne(&self, v: impl Into<T>) -> Cond<M> {
        self.cond("!=", v)
    }
    pub fn gt(&self, v: impl Into<T>) -> Cond<M> {
        self.cond(">", v)
    }
    pub fn lt(&self, v: impl Into<T>) -> Cond<M> {
        self.cond("<", v)
    }
    pub fn ge(&self, v: impl Into<T>) -> Cond<M> {
        self.cond(">=", v)
    }
    pub fn le(&self, v: impl Into<T>) -> Cond<M> {
        self.cond("<=", v)
    }
}

/// Marker: text columns (`String`, `Option<String>`), the only ones that support `like`.
pub trait TextField: Field {}
impl TextField for String {}
impl TextField for Option<String> {}

impl<M, T: TextField> Col<M, T> {
    /// SQL LIKE: `%` = any run of chars, `_` = any one char. Case-sensitive.
    /// `User::name.like("a%")`
    pub fn like(&self, pattern: &str) -> Cond<M> {
        Cond {
            col: self.name,
            op: "like",
            value: Value::Text(pattern.to_string()),
            _p: PhantomData,
        }
    }
}

/// OR group: `User::age.lt(5).or(User::age.gt(60))`, `User::id.in_list([1, 2, 3])`.
pub struct AnyOf<M> {
    conds: Vec<(String, String, Value)>,
    _p: PhantomData<fn() -> M>,
}

impl<M> AnyOf<M> {
    pub fn or(mut self, c: Cond<M>) -> Self {
        self.conds
            .push((c.col.to_string(), c.op.to_string(), c.value));
        self
    }
}

impl<M> Cond<M> {
    pub fn or(self, o: Cond<M>) -> AnyOf<M> {
        AnyOf {
            conds: vec![
                (self.col.to_string(), self.op.to_string(), self.value),
                (o.col.to_string(), o.op.to_string(), o.value),
            ],
            _p: PhantomData,
        }
    }
}

impl<M, T: Field> Col<M, T> {
    /// `col IN (...)`, built as OR of equals. An empty list is an error at run().
    pub fn in_list<I: IntoIterator<Item = T>>(&self, vals: I) -> AnyOf<M> {
        AnyOf {
            conds: vals
                .into_iter()
                .map(|v| (self.name.to_string(), "=".to_string(), v.to_value()))
                .collect(),
            _p: PhantomData,
        }
    }
    pub fn is_null(&self) -> Cond<M> {
        Cond {
            col: self.name,
            op: "=",
            value: Value::Null,
            _p: PhantomData,
        }
    }
    pub fn is_not_null(&self) -> Cond<M> {
        Cond {
            col: self.name,
            op: "!=",
            value: Value::Null,
            _p: PhantomData,
        }
    }
    /// lo <= col <= hi (two conditions, ANDed).
    pub fn between(&self, lo: impl Into<T>, hi: impl Into<T>) -> (Cond<M>, Cond<M>) {
        (self.cond(">=", lo), self.cond("<=", hi))
    }
}

/// Typed query. Conditions are combined with AND.
pub struct ModelQuery<'a, M> {
    q: Query<'a>,
    _p: PhantomData<fn() -> M>,
}

impl<'a, M: Model> ModelQuery<'a, M> {
    pub fn filter_any(mut self, g: AnyOf<M>) -> Self {
        self.q = self.q.filter_any(g.conds);
        self
    }
    fn agg<T: Field>(self, f: &str, c: Col<M, T>) -> Result<Option<T>> {
        match self.q.aggregate(f, c.name)? {
            Value::Null => Ok(None),
            v => T::from_value(v).map(Some),
        }
    }
    /// `db.all::<Order>().sum(Order::qty)`; None when no rows. Int columns only. Overflow is an error.
    pub fn sum<T: Field>(self, c: Col<M, T>) -> Result<Option<T>> {
        self.agg("SUM", c)
    }
    /// Integer average (truncated). Int columns only.
    pub fn avg<T: Field>(self, c: Col<M, T>) -> Result<Option<T>> {
        self.agg("AVG", c)
    }
    /// Works on i64, i32, String, Timestamp, f64.
    pub fn min<T: Field>(self, c: Col<M, T>) -> Result<Option<T>> {
        self.agg("MIN", c)
    }
    pub fn max<T: Field>(self, c: Col<M, T>) -> Result<Option<T>> {
        self.agg("MAX", c)
    }
    /// Count rows per value of `c`: `db.all::<Like>().group_count(Like::post_id)`.
    /// Ordered by group value. Use `group_count_top` for HAVING / biggest-first / limit.
    pub fn group_count<T: Field>(self, c: Col<M, T>) -> Result<Vec<(T, i64)>> {
        self.group_count_top(c, None, false, None)
    }
    /// `min_count` = HAVING COUNT(*) >= n; `by_count_desc` = biggest groups first; `top` = LIMIT.
    pub fn group_count_top<T: Field>(
        self,
        c: Col<M, T>,
        min_count: Option<i64>,
        by_count_desc: bool,
        top: Option<usize>,
    ) -> Result<Vec<(T, i64)>> {
        self.q
            .group_count(c.name, min_count, by_count_desc, top)?
            .into_iter()
            .map(|(v, n)| T::from_value(v).map(|t| (t, n)))
            .collect()
    }
    /// Cursor/poll forward: rows with `c > last`, oldest first, at most `n`.
    /// `db.all::<Msg>().page_after(Msg::id, last_seen_id, 50)`
    pub fn page_after<T: Field>(
        self,
        c: Col<M, T>,
        last: impl Into<T>,
        n: usize,
    ) -> Result<Vec<M>> {
        self.filter(c.gt(last)).order_by(c).limit(n).run()
    }
    /// Cursor backward (older messages): rows with `c < first`, newest first, at most `n`.
    pub fn page_before<T: Field>(
        self,
        c: Col<M, T>,
        first: impl Into<T>,
        n: usize,
    ) -> Result<Vec<M>> {
        self.filter(c.lt(first)).order_by_desc(c).limit(n).run()
    }
    pub fn filter_between(self, b: (Cond<M>, Cond<M>)) -> Self {
        self.filter(b.0).filter(b.1)
    }
    pub fn filter(mut self, c: Cond<M>) -> Self {
        self.q = self.q.filter(c.col, c.op, c.value);
        self
    }
    pub fn order_by<T>(mut self, c: Col<M, T>) -> Self {
        self.q = self.q.order_by(c.name);
        self
    }
    pub fn order_by_desc<T>(mut self, c: Col<M, T>) -> Self {
        self.q = self.q.order_by_desc(c.name);
        self
    }
    pub fn limit(mut self, n: usize) -> Self {
        self.q = self.q.limit(n);
        self
    }
    pub fn offset(mut self, n: usize) -> Self {
        self.q = self.q.offset(n);
        self
    }
    pub fn run(self) -> Result<Vec<M>> {
        let rows = self.q.run()?;
        rows.rows
            .iter()
            .map(|r| {
                M::from_row(&RowRef {
                    columns: &rows.columns,
                    values: r,
                })
            })
            .collect()
    }
}

/// Extension methods on `Server`.
pub trait Orm {
    /// Create the table or bring it in line with the struct (data is kept).
    fn sync<M: Model>(&self) -> Result<Vec<String>>;
    fn insert<M: Model>(&self, m: &M) -> Result<usize>;
    /// Insert and return the generated id (needs a `#[col(auto)]` column).
    fn insert_get_id<M: Model>(&self, m: &M) -> Result<i64>;
    fn find<M: Model>(&self, cond: Cond<M>) -> ModelQuery<'_, M>;
    fn all<M: Model>(&self) -> ModelQuery<'_, M>;
}

impl Orm for Server {
    fn sync<M: Model>(&self) -> Result<Vec<String>> {
        let plan = self.table(M::TABLE).sync(&spec_of(M::COLUMNS))?;
        for c in M::COLUMNS {
            if let Some(r) = c.references {
                let (pt, pc) = r
                    .split_once('.')
                    .ok_or_else(|| err("references must look like \"table.column\""))?;
                self.table(M::TABLE)
                    .add_foreign_key_on(c.name, pt, pc, c.cascade)?;
            }
            if let Some(d) = c.default {
                let v = match d.trim() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    x => parse_check(&format!("= {x}"))?.1,
                };
                self.table(M::TABLE).set_default(c.name, v)?;
            }
            if let Some(ck) = c.check {
                for part in ck.split(';') {
                    let (op, v) = parse_check(part)?;
                    self.table(M::TABLE).add_check(c.name, op, v)?;
                }
            }
        }
        for (cols, uniq) in M::INDEXES {
            let list: Vec<&str> = cols.split(',').map(|c| c.trim()).collect();
            let name = format!(
                "{}_{}_{}",
                M::TABLE,
                list.join("_"),
                if *uniq { "uq" } else { "ix" }
            );
            self.table(M::TABLE).create_index(&name, &list, *uniq)?;
        }
        Ok(plan)
    }
    fn insert<M: Model>(&self, m: &M) -> Result<usize> {
        match M::AUTO_PK {
            None => self.table(M::TABLE).insert(&with_defaults::<M>(stamped(m))),
            Some(pk) => {
                let row: Vec<_> = with_defaults::<M>(stamped(m))
                    .into_iter()
                    .filter(|(n, _)| *n != pk)
                    .collect();
                self.table(M::TABLE).insert_auto(pk, &row).map(|(_, n)| n)
            }
        }
    }
    fn insert_get_id<M: Model>(&self, m: &M) -> Result<i64> {
        let pk = M::AUTO_PK.ok_or_else(|| err("model has no #[col(auto)] column"))?;
        let row: Vec<_> = with_defaults::<M>(stamped(m))
            .into_iter()
            .filter(|(n, _)| *n != pk)
            .collect();
        self.table(M::TABLE).insert_auto(pk, &row).map(|(id, _)| id)
    }
    fn find<M: Model>(&self, cond: Cond<M>) -> ModelQuery<'_, M> {
        self.all::<M>().filter(cond)
    }
    fn all<M: Model>(&self) -> ModelQuery<'_, M> {
        ModelQuery {
            q: self.table(M::TABLE).find(),
            _p: PhantomData,
        }
    }
}

use mitrdb_server::{Delete, Update};

/// Typed update: `db.update::<User>().set(User::age, 26).filter(User::id.eq(1)).run()`.
pub struct ModelUpdate<'a, M> {
    u: Update<'a>,
    touched: bool,
    _p: PhantomData<fn() -> M>,
}

impl<M: Model> ModelUpdate<'_, M> {
    pub fn filter_any(mut self, g: AnyOf<M>) -> Self {
        self.u = self.u.filter_any(g.conds);
        self
    }
    pub fn set<T: Field>(mut self, c: Col<M, T>, v: impl Into<T>) -> Self {
        if Some(c.name) == M::UPDATED_AT {
            self.touched = true;
        }
        self.u = self.u.set(c.name, v.into().to_value());
        self
    }
    pub fn filter(mut self, c: Cond<M>) -> Self {
        self.u = self.u.filter(c.col, c.op, c.value);
        self
    }
    /// Explicitly update every row (needed when there is no filter).
    pub fn all(mut self) -> Self {
        self.u = self.u.all();
        self
    }
    /// Returns how many rows were changed.
    pub fn run(mut self) -> Result<usize> {
        if let (Some(n), false) = (M::UPDATED_AT, self.touched) {
            self.u = self.u.set(n, Value::Int64(Timestamp::now().0));
        }
        self.u.run()
    }
}

/// Typed delete: `db.delete::<User>().filter(User::id.eq(1)).run()`.
pub struct ModelDelete<'a, M> {
    d: Delete<'a>,
    _p: PhantomData<fn() -> M>,
}

impl<M: Model> ModelDelete<'_, M> {
    pub fn filter_any(mut self, g: AnyOf<M>) -> Self {
        self.d = self.d.filter_any(g.conds);
        self
    }
    pub fn filter(mut self, c: Cond<M>) -> Self {
        self.d = self.d.filter(c.col, c.op, c.value);
        self
    }
    /// Explicitly delete every row (needed when there is no filter).
    pub fn all(mut self) -> Self {
        self.d = self.d.all();
        self
    }
    /// Returns how many rows were deleted.
    pub fn run(self) -> Result<usize> {
        self.d.run()
    }
}

/// Write-side and single-row helpers on `Server`.
pub trait OrmWrite {
    fn update<M: Model>(&self) -> ModelUpdate<'_, M>;
    fn delete<M: Model>(&self) -> ModelDelete<'_, M>;
    /// First matching row, if any.
    fn find_one<M: Model>(&self, cond: Cond<M>) -> Result<Option<M>>;
    /// Row with this primary key, if any.
    fn find_by_id<M: Model, K: Field>(&self, id: K) -> Result<Option<M>>;
}

impl OrmWrite for Server {
    fn update<M: Model>(&self) -> ModelUpdate<'_, M> {
        ModelUpdate {
            u: self.table(M::TABLE).update(),
            touched: false,
            _p: PhantomData,
        }
    }
    fn delete<M: Model>(&self) -> ModelDelete<'_, M> {
        ModelDelete {
            d: self.table(M::TABLE).delete(),
            _p: PhantomData,
        }
    }
    fn find_one<M: Model>(&self, cond: Cond<M>) -> Result<Option<M>> {
        Ok(self.find(cond).limit(1).run()?.into_iter().next())
    }
    fn find_by_id<M: Model, K: Field>(&self, id: K) -> Result<Option<M>> {
        let pk = M::COLUMNS
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| err(format!("table '{}' has no primary_key column", M::TABLE)))?;
        let rows = self
            .table(M::TABLE)
            .find()
            .filter(pk.name, "=", id.to_value())
            .limit(1)
            .run()?;
        match rows.rows.first() {
            None => Ok(None),
            Some(r) => M::from_row(&RowRef {
                columns: &rows.columns,
                values: r,
            })
            .map(Some),
        }
    }
}

impl<M: Model> ModelQuery<'_, M> {
    /// Number of matching rows. Not allowed together with limit() or offset().
    pub fn count(self) -> Result<usize> {
        self.q.count()
    }
}

/// `db.count::<User>()` and `db.count_where(User::age.gt(18))`.
/// Relation helpers built on find / in_list (no engine changes).
/// Each call is a separate query: wrap in one explicit txn if you need a
/// consistent snapshot across them.
/// Max ids per IN-style query. Engine WHERE depth limit is 128 (OR chain nests),
/// so stay well below it.
const LOAD_MANY_CHUNK: usize = 50;

pub trait OrmRelations {
    /// `post.author`: parent row whose primary key equals `fk`.
    fn belongs_to<P: Model, K: Field>(&self, fk: K) -> Result<Option<P>>;
    /// `user.posts`: child rows whose `fk_col` equals `parent_id`.
    fn has_many<C: Model, K: Field>(&self, fk_col: Col<C, K>, parent_id: K) -> Result<Vec<C>>;
    /// Batch version (avoids N+1): children grouped by FK value.
    /// Parents with no children are absent from the map.
    fn load_many<C: Model, K: Field + Clone + Eq + std::hash::Hash>(
        &self,
        fk_col: Col<C, K>,
        ids: &[K],
    ) -> Result<std::collections::HashMap<K, Vec<C>>>;
}

impl OrmRelations for Server {
    fn belongs_to<P: Model, K: Field>(&self, fk: K) -> Result<Option<P>> {
        self.find_by_id::<P, K>(fk)
    }
    fn has_many<C: Model, K: Field>(&self, fk_col: Col<C, K>, parent_id: K) -> Result<Vec<C>> {
        self.find(fk_col.eq(parent_id)).run()
    }
    fn load_many<C: Model, K: Field + Clone + Eq + std::hash::Hash>(
        &self,
        fk_col: Col<C, K>,
        ids: &[K],
    ) -> Result<std::collections::HashMap<K, Vec<C>>> {
        let mut out: std::collections::HashMap<K, Vec<C>> = std::collections::HashMap::new();
        let mut seen = std::collections::HashSet::new();
        let uniq: Vec<K> = ids
            .iter()
            .filter(|k| seen.insert((*k).clone()))
            .cloned()
            .collect();
        for chunk in uniq.chunks(LOAD_MANY_CHUNK) {
            let rows = self
                .all::<C>()
                .filter_any(fk_col.in_list(chunk.iter().cloned()))
                .run()?;
            for r in rows {
                let v = r
                    .to_row()
                    .into_iter()
                    .find(|(n, _)| *n == fk_col.name)
                    .map(|(_, v)| v)
                    .ok_or_else(|| err("fk column missing in row"))?;
                out.entry(K::from_value(v)?).or_default().push(r);
            }
        }
        Ok(out)
    }
}

pub trait OrmCount {
    fn count<M: Model>(&self) -> Result<usize>;
    fn count_where<M: Model>(&self, cond: Cond<M>) -> Result<usize>;
}

impl OrmCount for Server {
    fn count<M: Model>(&self) -> Result<usize> {
        self.all::<M>().count()
    }
    fn count_where<M: Model>(&self, cond: Cond<M>) -> Result<usize> {
        self.find(cond).count()
    }
}

/// "<op> <literal>": `>= 0`, `!= 'x'`, `like 'A%'`. Literal = int or 'text'.
fn parse_check(part: &str) -> Result<(&'static str, Value)> {
    let t = part.trim();
    let low = t.to_lowercase();
    let (op, rest): (&'static str, &str) = if low.starts_with("like ") {
        ("like", t[5..].trim())
    } else {
        let mut found = None;
        for o in [">=", "<=", "!=", ">", "<", "="] {
            if let Some(r) = t.strip_prefix(o) {
                found = Some((o, r.trim()));
                break;
            }
        }
        found.ok_or_else(|| err(format!("bad check '{t}', expected: <op> <literal>")))?
    };
    let v = if let Some(inner) = rest.strip_prefix("'").and_then(|r| r.strip_suffix("'")) {
        Value::Text(inner.replace("''", "'"))
    } else {
        Value::Int64(
            rest.parse::<i64>()
                .map_err(|_| err(format!("bad check literal '{rest}'")))?,
        )
    };
    Ok((op, v))
}

/// Option fields that are None and have a `default` are left out, so the default applies.
fn with_defaults<M: Model>(row: Vec<(&'static str, Value)>) -> Vec<(&'static str, Value)> {
    row.into_iter()
        .filter(|(n, v)| {
            !(matches!(v, Value::Null)
                && M::COLUMNS
                    .iter()
                    .any(|c| c.name == *n && c.default.is_some()))
        })
        .collect()
}
