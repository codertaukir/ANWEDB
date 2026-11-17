# Known limitations

**Size**
- A row must fit in one 4096-byte page. The limit is per ROW (all columns added up) and counts bytes, not characters. With an `id int` primary key and one text column: 4019 ASCII bytes fit, 4020 fail. Hindi (3 bytes per character) fits 1339 characters.
- An oversized row is refused with `OUT_OF_SPACE` (`Error::OutOfSpace`) and nothing is half-written. The table stays usable.
- `FileStore` has no size limit. Files live on disk, outside the database, WAL and transactions. Saving a file and inserting its row are not atomic: save the file first, insert the row second, delete the file if the insert fails. Orphan files after a crash are not swept. `FileStore` and `TextStore` are in-process only (no upload/download over TCP).
- One TCP frame is at most 1 MB (requests and responses).

**Types**
- Native `TIMESTAMP` (int microseconds) and `FLOAT`. Float: NaN is rejected, `-0.0` is stored as `0.0`. Do not use CHECK on float columns (not tested).
- Integer `AVG` truncates; float `SUM`/`AVG` are real f64.
- `DEFAULT` is available through `set_default` / `#[col(default = ..)]`. ORM `Default` applies only when an `Option` field is `None`.
- Tables created before native TIMESTAMP / FLOAT (commit `aa65c8a`) stored them in an `int` column and now read wrong. There is no migration system by design: drop the table and `sync()` again. To keep data, `legacy_migrate_probe.rs` has a tested recipe (add shadow column, decode with the old formula, drop, rename).

**Constraints**
- FK, CHECK and DEFAULT are enforced by the Table/ORM API only. Raw `execute_sql` and raw SQL over TCP bypass them (pinned by `crates/mitrdb-orm/tests/tcp_fk.rs`).
- The FK lock works inside one process only. Two processes on the same database cannot enforce FKs safely.
- Foreign keys: RESTRICT or `ON DELETE CASCADE`. Cascade delete is not atomic. The parent column must be a primary key of the same type; `bytes` is not supported.
- CHECK supports `column op literal` (int or text). NULL passes. Existing rows are not verified when a check is added.
- Update/delete through the ORM needs a `filter` or `filter_any`. An empty OR group (`in_list([])`) does not count as a filter, so the whole table is protected.

**Queries**
- LIKE is case-sensitive, has no escape character and works on text only. A literal prefix (`'ab%'`, `'ab_c%'`) on an indexed column uses an index range scan; a pattern starting with `%` or `_`, or an unindexed column, does a full scan plus filter.
- `LIMIT $n` / `OFFSET $n` are not supported in SQL (ORM `limit`/`offset` take `usize`).
- Composite index: at most 4 columns; unique constraints and leftmost-prefix equality only (no range or ORDER BY on a prefix).
- `WHERE` nesting depth is at most 128. `in_list` builds an OR chain, so `load_many` uses chunks of 50 ids.
- `page_after` / `page_before` on a NON-UNIQUE column skip rows that share the cursor value (measured: 35 of 50 rows seen with page size 7). Use the primary key (or another unique column) as cursor: 50 of 50, also with a filter.
- `belongs_to`, `has_many`, `load_many` are plain queries (no JOIN eager loading). Separate calls are not one snapshot unless wrapped in an explicit transaction.

**Transactions**
- DDL (CREATE/ALTER/DROP) is not allowed inside a transaction.
- `ROLLBACK TO` can fail for writes that use page-slice undo; the transaction is then aborted.

**Schema changes**
- `sync()` refuses a changed column type. `sync_allow_type_change` applies it through a shadow column with strict casts (first bad row aborts), refuses primary key and indexed columns, and is not safe against concurrent writers.
- Engine `ALTER COLUMN TYPE`: int, text, bool, bytes, and int to timestamp work. int to float is refused.

**Network**
- Protocol: 4-byte big-endian length + payload. First frame `MITRDB/1`, then optional `AUTH <token>`, then plain SQL.
- Auth failures are rate limited: 5 per 60 s per IP.
- Clients must set `TCP_NODELAY` and send length + payload in ONE write. Measured: 41163 us per statement without, 431 us with nodelay, 95 us in-process (small sample).

**Internal tables**
- `mitrdb_seq`, `mitrdb_fk`, `mitrdb_check`, `mitrdb_default` are reserved. Do not use the `mitrdb_` prefix for your own tables.

**Not the database's job**
- Password hashing, sessions and WebSocket real-time belong to the app layer.
