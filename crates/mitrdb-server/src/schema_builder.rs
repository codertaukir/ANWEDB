//! Mongoose-style schema builder:
//!   server.define_table("users", |t| {
//!       t.id().primary_key();
//!       t.string("name").not_null();
//!       t.integer("age");
//!       t.string("email").unique();
//!   })?;

use mitrdb_common::{Error, Result};

use crate::Server;
use crate::table::Table;

fn bad(msg: impl Into<String>) -> Error {
    Error::TransactionError(msg.into())
}

fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !s.chars().next().unwrap().is_ascii_digit()
}

/// One column in a table definition. Chain `primary_key`, `not_null`, `unique`.
pub struct ColumnBuilder {
    name: String,
    sql_type: &'static str,
    primary_key: bool,
    not_null: bool,
    unique: bool,
}

impl ColumnBuilder {
    pub fn primary_key(&mut self) -> &mut Self {
        self.primary_key = true;
        self
    }
    pub fn not_null(&mut self) -> &mut Self {
        self.not_null = true;
        self
    }
    pub fn unique(&mut self) -> &mut Self {
        self.unique = true;
        self
    }
    fn to_spec(&self) -> String {
        let mut s = format!("{} {}", self.name, self.sql_type);
        if self.primary_key {
            s.push_str(" primary key");
        } else {
            if self.not_null {
                s.push_str(" not null");
            }
            if self.unique {
                s.push_str(" unique");
            }
        }
        s
    }
}

/// Collects column definitions inside `define_table`.
#[derive(Default)]
pub struct TableBuilder {
    cols: Vec<ColumnBuilder>,
}

impl TableBuilder {
    fn add(&mut self, name: &str, sql_type: &'static str) -> &mut ColumnBuilder {
        self.cols.push(ColumnBuilder {
            name: name.to_string(),
            sql_type,
            primary_key: false,
            not_null: false,
            unique: false,
        });
        self.cols.last_mut().unwrap()
    }
    /// Integer column named "id", already primary key.
    pub fn id(&mut self) -> &mut ColumnBuilder {
        let c = self.add("id", "int");
        c.primary_key();
        c
    }
    pub fn integer(&mut self, name: &str) -> &mut ColumnBuilder {
        self.add(name, "int")
    }
    pub fn string(&mut self, name: &str) -> &mut ColumnBuilder {
        self.add(name, "text")
    }
    pub fn boolean(&mut self, name: &str) -> &mut ColumnBuilder {
        self.add(name, "bool")
    }
    pub fn bytes(&mut self, name: &str) -> &mut ColumnBuilder {
        self.add(name, "bytes")
    }
    fn build_spec(&self) -> Result<String> {
        if self.cols.is_empty() {
            return Err(bad("define_table needs at least one column"));
        }
        for c in &self.cols {
            if !valid_ident(&c.name) {
                return Err(bad(format!("bad column name: '{}'", c.name)));
            }
        }
        let parts: Vec<String> = self.cols.iter().map(|c| c.to_spec()).collect();
        Ok(parts.join(", "))
    }
}

impl Server {
    /// Create the table, or bring an existing one in line with this definition (data is kept).
    pub fn define_table<F>(&self, name: &str, build: F) -> Result<Table<'_>>
    where
        F: FnOnce(&mut TableBuilder),
    {
        let mut b = TableBuilder::default();
        build(&mut b);
        let spec = b.build_spec()?;
        let table = self.table(name);
        table.sync(&spec)?;
        Ok(table)
    }
}
