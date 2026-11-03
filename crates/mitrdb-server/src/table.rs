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

impl Table<'_> {
    fn check_name(&self) -> Result<()> {
        if valid_ident(&self.name) {
            Ok(())
        } else {
            Err(bad(format!("bad table name: '{}'", self.name)))
        }
    }

    /// Make the table match `spec`, keeping data. Returns the SQL it ran.
    pub fn sync(&self, spec: &str) -> Result<Vec<String>> {
        self.check_name()?;
        let wanted = parse_columns(spec)?;
        let mut plan: Vec<String> = Vec::new();

        match self.server.get_table_schema(&self.name)? {
            None => {
                let cols: Vec<String> = wanted.iter().map(col_sql).collect();
                plan.push(format!("CREATE TABLE {} ({})", self.name, cols.join(", ")));
            }
            Some(schema) => {
                for old in &schema.columns {
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
                            if old.data_type != w.data_type
                                || old.is_primary_key != w.is_primary_key
                                || old.nullable != w.nullable
                            {
                                return Err(bad(format!(
                                    "column '{}' changed (type/primary key/not null); this is not supported yet",
                                    w.name
                                )));
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

        for sql in &plan {
            self.server.execute_sql(sql)?;
        }
        Ok(plan)
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
}
