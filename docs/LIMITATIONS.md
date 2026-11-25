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
- FK, CHECK and DEFAULT are enforced by the Table/ORM API only. Raw SQL writes (INSERT/UPDATE/DELETE) through `execute_sql`, `execute_sql_params`, `execute_in_session`, `execute_plan*`, the CLI and TCP are REFUSED on a table that has FK, CHECK or DEFAULT rules, or that is the parent of an FK (error contains `refused`). Only the Table/ORM API may write to such tables. SELECT and DDL are not affected; tables without rules accept raw SQL as before. Pinned by `raw_guard.rs` and `tcp_fk.rs`.
- The FK lock works inside one process only. Two processes on the same database cannot enforce FKs safely.
- Foreign keys: RESTRICT or `ON DELETE CASCADE`. Cascade delete is not atomic. The parent column must be a primary key of the same type; `bytes` is not supported.
- CHECK supports `column op literal` (int or text). NULL passes. Existing rows are not verified when a check is added.
- The raw SQL guard only checks INSERT/UPDATE/DELETE and reads the rule tables (`mitrdb_fk`, `mitrdb_check`, `mitrdb_default`) per statement; its cost is not measured. To write to a table with rules from SQL, use the Table/ORM API. Engine-level enforcement of these rules is not implemented.
- Update/delete through the ORM needs a `filter` or `filter_any`. An empty OR group (`in_list([])`) does not count as a filter, so the whole table is protected.

**Queries**
- LIKE is case-sensitive, has no escape character and works on text only. A literal prefix (`'ab%'`, `'ab_c%'`) on an indexed column uses an index range scan; a pattern starting with `%` or `_`, or an unindexed column, does a full scan plus filter.
- `LIMIT $n` / `OFFSET $n` are not supported in SQL (ORM `limit`/`offset` take `usize`).
- Composite index: at most 4 columns; unique constraints and leftmost-prefix equality only (no range or ORDER BY on a prefix).
- `WHERE` nesting depth is at most 128. `in_list` builds an OR chain, so `load_many` uses chunks of 50 ids.
- `page_after` / `page_before` on a NON-UNIQUE column skip rows that share the cursor value (measured: 35 of 50 rows seen with page size 7). Use the primary key (or another unique column) as cursor: 50 of 50, also with a filter.
- `JOIN`: both tables are read fully into memory and `WHERE` is applied after the join (no pushdown), so very large joins use a lot of memory. Equality `ON` conditions use a hash join; other conditions use a nested loop (O(left x right)). In a JOIN query, an `ORDER BY` column must also appear in the `SELECT` list.
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

## Default session and auto ids
- The default session (`execute_sql`, Table/ORM API) runs plain autocommit statements without holding the session mutex, so threads run in parallel. A `BEGIN` on one thread does not capture a statement running at the same time on another thread (that statement runs in its own autocommit transaction). For several threads with explicit transactions, use one session per thread (`create_session` / `execute_in_session`).
- Auto ids are never reused, also inside an explicit transaction: the id counter is committed on its own, so a ROLLBACK leaves a gap (auto_txn_rollback.rs). Ids can have gaps.
- Many threads inserting auto-id rows into one table contend on one counter row. Measured (tmpfs, one run): 1 thread about 0.6 ms per insert, 4 threads 4-7 ms.
- `insert_auto` claims its id from the `mitrdb_seq` counter in its own autocommit and retries a counter conflict up to 1000 times (yield + capped sleep backoff), also inside an explicit transaction. The row insert itself runs in the caller's transaction; a write conflict on that insert is not retried by `insert_auto`. Tested: ROLLBACK/COMMIT inside an explicit txn (`insert_auto_txn.rs`, one run) and `auto_concurrent` (30 runs, autocommit). A conflict on the row insert inside an explicit txn is not tested.

## Durability / crash testing scope
All results are observations from a small number of runs, not guarantees. Every power-loss drill used one QEMU VM, hard `virsh destroy`, `cache=directsync`, ext4, 4 writer threads (DDL drill: 1 thread), intent and ack logs synced with `sync_data()`. Each cut counts as one run. After every cut the drill also ran 1000-2000 more inserts to check the database still accepts writes.

- **INSERT only** (`powerloss_drill`): 10 cuts, one table, `lost=0 phantom=0 dup=0`.
- **INSERT/UPDATE/DELETE** (`powerloss_soak`): 4 cuts with a checkpointer thread, one table, `lost=0 phantom=0`.
- **Long soak** (`powerloss_soak`): about 100 minutes, I/U/D, 4 threads, checkpointer every 3 s, WAL rotated through 5 segments, 65,141 ids checked, 1 cut, `lost=0 phantom=0`.
- **Multi-table transaction** (`powerloss_txn`): 4 sessions, `BEGIN..COMMIT` over 2 tables plus counter `UPDATE n = n + 1`, 4 cuts, `torn=0 lost=0 phantom=0 counter_bad=0`.
- **DDL** (`powerloss_ddl`): CREATE TABLE, INSERT, CREATE INDEX, ALTER ADD COLUMN, DROP TABLE, single thread, one table per iteration, 4 cuts, `bad=0 phantom=0`. Concurrent DDL, DDL together with DML and ALTER DROP/RENAME COLUMN are untested.
- **Not covered:** other filesystems or hardware; a cut during a checkpoint is not proven (only cuts close to one); backup during power loss.
- **Backup** (`backup_kill_drill`, host): 200k base rows, 4 writer threads doing INSERT/UPDATE/DELETE, backups in a loop, process `kill -9`, then restore and verify of recent backups: `bad=0` in the final runs. Process-crash level only, not power loss. Earlier: 1M rows with 4 insert threads, backup 2.79 s, restore 2.16 s, 1M/1M restored, `bad=0` (`backup_big_drill`, one run).
- **Disk full** (`diskfull_drill`, 8 MB tmpfs): 9,654 acknowledged inserts, then a clean `No space left on device`; after restart `lost=0 phantom=0 dup=0`. Small database, process-crash level.
- **TCP + TLS** (`tcp_tls_drill`, host, loopback, `--max-connections 256`): 100 and 200 clients, one run each, server `kill -9`, `lost=0 phantom=0 dup=0`. Wrong token refused. With `--max-connections 20` and 30 attempts, 19 connected (one slot held by the probe), 11 refused, the server stayed alive and accepted new connections after release. Not tested: the default limit with 200 clients, slow clients, a real network. This is not production-scale network validation.
- **Directory lock:** one database directory can be opened by one process (`<db>.lock`, flock). Not guaranteed on network filesystems.
- **Torn WAL tail rule:** in the last segment, a CRC-failed record is truncated only if everything after it is zeros; corruption in the middle stays an error.

**Performance observations (measured, single runs, real disk btrfs unless noted; not guarantees)**
- 1M rows: insert about 15.8k rows/s, full scan 3.89 s, point lookup about 297 us, `CREATE INDEX` 20 s, reopen 1.49 s (after the open-time checkpoint; one intermediate run took 14 s, cause not found).
- 10M rows, 4 threads: insert about 20.2k rows/s, peak RSS 4,216 MB (about 420 B/row), `COUNT(*)` 99.95 s, reopen 19.58 s. Without a memory cap this test hit OOM; a `MemoryMax=4G` run passed, whether the cap was enforced is not established.
- `COUNT(*)` on large tables is a known bottleneck. Buffer pool 4096 to 65536 frames: 1.37 s to 0.98 s in one run only, not a proven general gain.
- After recovery, WAL growth was about 7 KB per insert in one observed run (full page images; reason not investigated). Large DELETEs write about 6 KB of WAL per row.
- `write_mark_real` A/B was inconclusive (noise); no speed-up is claimed.
- tmpfs and real-disk numbers are not comparable; see the README benchmarks for conditions.
