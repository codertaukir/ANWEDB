# Known limitations

**Types**
- There is no native Timestamp or Float column type. `Timestamp` and `f64` are mapped onto an `int` column
  by the ORM. For `f64`: raw SQL `SUM`/`AVG`/`SELECT` show encoded integers, NaN must not be stored,
  `-0.0` is stored as `0.0`, and CHECK must not be used on `f64` columns.
- No default values in the schema. Use Rust `Default` (`..Default::default()`).

**Constraints**
- Foreign keys and CHECK are enforced by the Table/ORM API only. Raw `execute_sql` and raw SQL sent over TCP bypass them (pinned by `crates/mitrdb-orm/tests/tcp_fk.rs`).
- The foreign-key lock works inside one process only. Two processes on the same database cannot enforce FKs safely.
- Foreign keys are RESTRICT only (no cascade). The parent column must be a primary key of the same type; `bytes` is not supported.
- CHECK supports `column op literal` (int or text literal). NULL passes. Existing rows are not verified when a check is added.

**Queries**
- LIKE is case-sensitive, has no escape character, works on text only, and always does a full scan plus filter.

**Transactions**
- DDL (CREATE/ALTER/DROP) is not allowed inside a transaction.
- `ROLLBACK TO` can fail for writes that use page-slice undo; the transaction is then aborted.

**Internal tables**
- `mitrdb_seq`, `mitrdb_fk`, `mitrdb_check` are reserved. Do not use the `mitrdb_` prefix for your own tables.
