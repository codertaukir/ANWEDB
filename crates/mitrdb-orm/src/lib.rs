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

/// Static description of one column (macro will generate these).
#[derive(Debug, Clone, Copy)]
pub struct ColumnMeta {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub index: bool,
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

/// Typed query. Conditions are combined with AND.
pub struct ModelQuery<'a, M> {
    q: Query<'a>,
    _p: PhantomData<fn() -> M>,
}

impl<'a, M: Model> ModelQuery<'a, M> {
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
    fn find<M: Model>(&self, cond: Cond<M>) -> ModelQuery<'_, M>;
    fn all<M: Model>(&self) -> ModelQuery<'_, M>;
}

impl Orm for Server {
    fn sync<M: Model>(&self) -> Result<Vec<String>> {
        self.table(M::TABLE).sync(&spec_of(M::COLUMNS))
    }
    fn insert<M: Model>(&self, m: &M) -> Result<usize> {
        self.table(M::TABLE).insert(&m.to_row())
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
