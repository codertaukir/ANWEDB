# Known limitations

**Size**
- **Streaming BLOB (large payloads ka sahi rasta):** default max **128 MiB** (`DEFAULT_MAX_BLOB_BYTES`), per-server configurable via `Server::set_max_blob_bytes(n)` / read via `Server::max_blob_bytes()`. Configured max exceed karne pe `OUT_OF_SPACE` (`Error::OutOfSpace`) aata hai aur writer failed rehta hai — kuch half-written commit nahi hota. `Server::begin_blob_write_default(session)` server ke configured max pe cap karta hai; `begin_blob_write(session, max_len)` explicit per-call max leta hai.
- **SQL literal / inline row path bounded hai:** SQL me `TEXT`/`BYTES` literal inline Tuple + overflow path se jata hai, capped at **256 KiB + 1 byte** (`MAX_OVERFLOW_PAYLOAD_BYTES`) per row payload. Isse bada SQL literal fast fail hota hai `OUT_OF_SPACE` se — usse bada payload likhna hai to streaming BLOB API (`begin_blob_write_default` / `write_blob_chunk` / `finish_blob_write`) use karo. Fields up to 2,048 bytes stay inline in the B+Tree leaf; limit bytes me ginta hai, characters me nahi.
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
- `LIMIT $n` / `OFFSET $n` are supported in SQL via `execute_sql_params` and `execute_sql_params_in_session` (must bind non-negative `Int64` values; ORM `limit`/`offset` take `usize`).
- Composite index: at most 4 columns; unique constraints and leftmost-prefix equality only (no range or ORDER BY on a prefix).
- `WHERE` nesting depth is at most 128. `in_list` builds an OR chain, so `load_many` uses chunks of 50 ids.
- `page_after` / `page_before` on a NON-UNIQUE column skip rows that share the cursor value (measured: 35 of 50 rows seen with page size 7). Use the primary key (or another unique column) as cursor: 50 of 50, also with a filter.
- `JOIN`: equality `ON` conditions probe the right table's primary key or secondary index directly when the outer side has `<= 256` distinct keys (and short-circuit immediately when the outer side is empty), falling back to an in-memory hash join for larger outer inputs; non-equality `ON` conditions use a nested loop. `WHERE` is applied after the join. In a JOIN query, an `ORDER BY` column must also appear in the `SELECT` list.
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
- The default session (`execute_sql`, `Table`, ORM) runs plain autocommit statements without holding the session mutex, so threads run in parallel. A `BEGIN` on one thread does not capture a statement running at the same time on another thread (that statement runs in its own autocommit transaction). For concurrent request handlers with explicit transactions (e.g., Axum), use one session per task (`create_session` / `execute_in_session` / `execute_sql_params_in_session`) along with `Server::session_table(session_id, name)` or `Server::session_orm(session_id)`.
- Auto ids are never reused, also inside an explicit transaction: the id counter is committed on its own, so a ROLLBACK leaves a gap (auto_txn_rollback.rs). Ids can have gaps.
- Many threads inserting auto-id rows into one table contend on one counter row. Measured (tmpfs, one run): 1 thread about 0.6 ms per insert, 4 threads 4-7 ms.
- `insert_auto` claims its id from the `mitrdb_seq` counter in its own autocommit and retries a counter conflict up to 1000 times (yield + capped sleep backoff), also inside an explicit transaction. The row insert runs in the caller's transaction; a `Duplicate primary key` or retryable conflict on it also re-enters the same loop (full max(pk) scan, then retry). Inside an explicit transaction, a conflict that poisons the transaction may make the retry fail too (not tested). Tested: ROLLBACK/COMMIT inside an explicit txn (`insert_auto_txn.rs`, one run) and `auto_concurrent` (30 runs, autocommit).

## Durability / crash testing scope
All results are observations from a small number of runs, not guarantees. Every power-loss drill used one QEMU VM, hard `virsh destroy`, `cache=directsync`, ext4, 4 writer threads (DDL drill: 1 thread), intent and ack logs synced with `sync_data()`. Each cut counts as one run. After every cut the drill also ran 1000-2000 more inserts to check the database still accepts writes.

- **INSERT only** (`powerloss_drill`): 10 cuts, one table, `lost=0 phantom=0 dup=0`.
- **INSERT/UPDATE/DELETE** (`powerloss_soak`): 4 cuts with a checkpointer thread, one table, `lost=0 phantom=0`.
- **Long soak** (`powerloss_soak`): about 100 minutes, I/U/D, 4 threads, checkpointer every 3 s, WAL rotated through 5 segments, 65,141 ids checked, 1 cut, `lost=0 phantom=0`.
- **Multi-table transaction** (`powerloss_txn`): 4 sessions, `BEGIN..COMMIT` over 2 tables plus counter `UPDATE n = n + 1`, 4 cuts, `torn=0 lost=0 phantom=0 counter_bad=0`.
- **DDL** (`powerloss_ddl`): CREATE TABLE, INSERT, CREATE INDEX, ALTER ADD COLUMN, DROP TABLE, single thread, one table per iteration, 4 cuts, `bad=0 phantom=0`. Concurrent DDL + DML sessions are covered at the process level in `concurrent_ddl_dml.rs`; ALTER DROP/RENAME COLUMN under power loss is untested.
- **Process `SIGKILL` during active checkpoint & 16 KB overflow writes** (`crash_recovery_concurrent.rs`, host): concurrent writers inserting 16 KB overflow rows alongside explicit uncommitted transactions and continuous `checkpoint()` calls killed with `SIGKILL` recover with `lost=0 phantom=0` and all uncommitted transactions rolled back.
- **Not covered:** other filesystems or hardware; a VM hardware power cut (`virsh destroy`) mid-checkpoint (only cuts close to one were drilled on VM); backup during power loss.
- **Backup** (`backup_kill_drill`, host): 200k base rows, 4 writer threads doing INSERT/UPDATE/DELETE, backups in a loop, process `kill -9`, then restore and verify of recent backups: `bad=0` in the final runs. Process-crash level only, not power loss. Earlier: 1M rows with 4 insert threads, backup 2.79 s, restore 2.16 s, 1M/1M restored, `bad=0` (`backup_big_drill`, one run).
- **Disk full** (`diskfull_drill`, 8 MB tmpfs): 9,654 acknowledged inserts, then a clean `No space left on device`; after restart `lost=0 phantom=0 dup=0`. Small database, process-crash level.
- **TCP + TLS** (`tcp_tls_drill` & `network_limits.rs`, host, loopback): 100 and 200 clients with `--max-connections 256` under server `kill -9` (`lost=0 phantom=0 dup=0`), plus default connection limit (`128`) under 220 concurrent clients, oversized frame rejection, slow-client timeouts, and auth IP rate limiting. Real-network WAN/packet-loss behavior is not tested.
- **Directory lock:** one database directory can be opened by one process (`<db>.lock`, flock). Not guaranteed on network filesystems.
- **Torn WAL tail rule:** in the last segment, a CRC-failed record is truncated only if everything after it is zeros; corruption in the middle stays an error.

**Performance observations (measured, single runs, real disk btrfs unless noted; not guarantees)**
- 1M rows: insert about 15.8k rows/s, full scan 3.89 s, point lookup about 297 us, `CREATE INDEX` 20 s, reopen 1.49 s (after the open-time checkpoint; one intermediate run took 14 s, cause not found).
- 10M rows, 4 threads: insert about 20.2k rows/s, peak RSS 4,216 MB (about 420 B/row), `COUNT(*)` 99.95 s, reopen 19.58 s. Without a memory cap this test hit OOM; a `MemoryMax=4G` run passed, whether the cap was enforced is not established.
- `COUNT(*)` on large tables is a known bottleneck. Buffer pool 4096 to 65536 frames: 1.37 s to 0.98 s in one run only, not a proven general gain.
- After recovery, WAL growth was about 7 KB per insert in one observed run (full page images; reason not investigated). Large DELETEs write about 6 KB of WAL per row.
- `write_mark_real` A/B was inconclusive (noise); no speed-up is claimed.
- tmpfs and real-disk numbers are not comparable; see the README benchmarks for conditions.
