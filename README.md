# MITRDB

[![CI](https://github.com/codertaukir/MITRDB/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/codertaukir/MITRDB/actions/workflows/ci.yml)

A transactional database engine written in Rust.

MITRDB is built from the storage layer up: a B+Tree storage engine, a write-ahead log with
crash recovery, MVCC transactions with serializable range locking, and online backup.
It is currently a **single-node** engine in **early development**.

> **Status:** early development. APIs, on-disk formats and behaviour may change.
> Not production ready.

---

## Features

**Storage**
- B+Tree storage engine with variable-length keys and values, leaf/internal splits, merges and rebalancing
- Fixed-size 4 KB pages with CRC32C checksums (torn or corrupted pages are detected and rejected)
- Linked 4 KB overflow page chains supporting `TEXT` and `BYTES` payloads up to 16 KB (with an inline fast path up to 2,048 bytes per field)
- Buffer pool with clock eviction and pinned-frame protection
- Deferred page freeing, so freed pages are only reused after the owning transaction finalizes

**Durability and recovery**
- Write-ahead logging (redo and undo records) with CRC32C-protected records
- Group commit: concurrent commits share a single fsync, and a commit returns only after its log record is durable
- Segmented WAL with rotation and checkpoint-driven reclamation of obsolete segments
- ARIES-style crash recovery: redo of committed work, undo of uncommitted work, idempotent on repeated restarts
- Torn-tail detection and truncation on startup

**Transactions**
- MVCC with snapshot visibility
- Serializable isolation with range locking (gap and next-key locks that prevent phantoms)
- Deadlock detection and lock upgrades
- Background-safe vacuum that prunes old versions and tombstones without resurrecting deleted records

**Operations**
- Online backup with a manifest and per-file checksums, validated on restore
- WAL pinning so checkpoints cannot reclaim segments needed by an in-progress backup
- Built-in metrics and an optional per-phase insert profiler

**Performance**
- Hardware-accelerated CRC32C on x86_64 (SSE4.2), with a portable slicing-by-8 fallback; both produce identical output

## Architecture

The project is a Cargo workspace. Each crate has one responsibility:

| Crate | Responsibility |
|---|---|
| `mitrdb-common` | Shared types, constants and the error type |
| `mitrdb-observe` | Metrics and the insert profiler |
| `mitrdb-storage` | Pages, disk manager, buffer pool, B+Tree, WAL, recovery, backup |
| `mitrdb-txn` | MVCC, lock manager, range locks, transactions, vacuum |
| `mitrdb-catalog` | Table and schema metadata |
| `mitrdb-query` | Query parsing and planning |
| `mitrdb-exec` | Query execution |
| `mitrdb-server` | Server and benchmark examples |

## Getting started

### Requirements
- A recent stable Rust toolchain (the code uses let-chains, so a current release is needed)
- Linux only. Other platforms are not supported.

### Build
```bash
cargo build --release
```

### Run the tests
```bash
cargo test --workspace
```

The suites cover crash recovery (including torn tails and crashes during checkpoints), WAL
segmentation, online backup and restore, MVCC visibility, vacuum, group commit concurrency
and serializable range locking.

### Run the benchmark
```bash
cargo run --release -p mitrdb-server --example perf_bench -- \
    /tmp/bench.db /tmp/bench.wal 10000 10 batch 1 4096
```

Arguments: database path, WAL path, rows to insert, measured scans, transaction mode,
warm-up scans, buffer pool frames.

To see where insert time goes, enable the per-phase profiler:
```bash
MITRDB_PROFILE_INSERT=1 cargo run --release -p mitrdb-server --example perf_bench -- \
    /tmp/bench.db /tmp/bench.wal 10000 10 batch 1 4096
```
Leave it off when measuring throughput, because profiling adds overhead.

## Usage (Rust)

### Tables

```rust
let (db, _report) = Server::open("data.db", "data.wal", 1024)?;

// Create the table, or add new nullable columns. Safe to run again.
db.table("users").sync("id int primary key, email text not null unique, age int")?;

db.table("users").insert(&[("id", val(1)), ("email", val("a@b.c")), ("age", val(30))])?;

let rows = db.table("users").find().filter("age", ">", val(17)).order_by_desc("age").limit(10).run()?;
db.table("users").delete().filter("id", "=", val(1)).run()?;
```

### Schema builder

Mongoose-style schema in Rust code, no migration files. Safe to run on every start: it creates the table or adds new nullable columns, and keeps your data.

```rust
let users = db.define_table("users", |t| {
    t.id().primary_key();
    t.string("name").not_null();
    t.integer("age");
    t.string("email").unique();
    t.boolean("active");
    t.bytes("avatar_hash");
})?;

users.insert(&[("id", val(1)), ("name", val("Asha")), ("email", val("a@b.c"))])?;
users.drop_column("avatar_hash")?;   // logical drop, see Limits
```

### Parameters (no SQL injection)

Use `$1, $2, ...` placeholders with `execute_sql_params`. Values are bound, never spliced into the SQL text:

```rust
db.execute_sql_params("INSERT INTO users VALUES ($1, $2)", &[val(1), val("O'Brien")])?;
db.execute_sql_params("UPDATE users SET age = age + $1 WHERE id = $2", &[val(1), val(7)])?;
```

Placeholders work for values in INSERT, UPDATE (`SET col = $n`, `col = col +/- $n`), WHERE comparisons, and `LIMIT $n` / `OFFSET $n` (non-negative integers). Missing, extra, or invalid parameters are errors. Plain `execute_sql` rejects `$n`. `Table::find/count/update/delete` already bind their values this way. Use `execute_sql_params_in_session(session_id, sql, params)` to run parameterized queries inside explicit per-request sessions.

### Big files

`TEXT` and `BYTES` fields (and total row payloads) up to **16 KB (16,384 bytes)** are stored natively in the database via transactional, CRC32C-verified overflow page chains. Bigger data (photos, videos, multi-megabyte blobs) goes to disk with `FileStore`; the DB keeps only the file name in a normal `text` column.

```rust
let files = FileStore::open("data/files")?;
let saved = files.save(&mut reader)?;        // any std::io::Read, written in 1 MB chunks
// store saved.name in a text column
let mut file = files.open_read(&saved.name)?;
files.delete(&saved.name)?;
```

Files get random 32-character names, are written to a temp file and renamed, and use owner-only permissions. `FileStore` does not limit file size, does not encrypt, and does not delete a file when a row is deleted: that is the caller's job.

### ORM (struct = schema)

The `mitrdb-orm` crate turns a Rust struct into the table schema. No migration files.

```rust
use mitrdb_orm::*;

#[derive(Model, Debug)]
struct User {
    #[col(primary_key, auto)] id: i64,
    #[col(unique)] email: String,
    age: Option<i32>,
    #[col(check = ">= 0")] score: i64,
    #[col(created_at)] created_at: Timestamp,
    #[col(updated_at)] updated_at: Timestamp,
}

db.sync::<User>()?;
let id = db.insert_get_id(&user)?;
let adults = db.find(User::age.gt(18)).order_by(User::id).run()?;
db.update::<User>().set(User::age, 31).filter(User::id.eq(id)).run()?;
```

Supports auto-increment ids, `created_at`/`updated_at`, `like`, foreign keys (`references = "table.col"`), CHECK, and savepoints.
See [docs/ORM.md](docs/ORM.md), [docs/LIMITATIONS.md](docs/LIMITATIONS.md) and
`cargo run --release -p mitrdb-orm --example quickstart`.

## Limits

Know these before you use MITRDB.

**Data**
- Engine types: `INT`, `TEXT`, `BOOL`, `BYTES`, `TIMESTAMP`, `FLOAT` (see [docs/LIMITATIONS.md](docs/LIMITATIONS.md)).
- Individual `TEXT` / `BYTES` values and rows up to **16 KB (16,384 bytes)** are supported natively via WAL-logged, CRC32C-verified 4 KB overflow page chains (fields <= 2,048 bytes stay inline on B+Tree leaf pages). Larger payloads return a clean `OUT_OF_SPACE` error; for multi-megabyte blobs, use `FileStore` (`files.rs`).
- A `UNIQUE` column allows many `NULL` values; real duplicate values are rejected. All `NULL` rows share one index entry, so concurrent writes of `NULL` into the same unique column run one after another (from code reading, not measured).

**SQL**
- `CREATE TABLE IF NOT EXISTS` and `CREATE [UNIQUE] INDEX IF NOT EXISTS` do nothing (0 rows affected) when the name already exists. The old table or index is not changed.
- `JOIN`, `INNER JOIN`, `LEFT JOIN` work. Equality conditions in `ON` use index-assisted lookups on the right table's primary key or secondary index when the outer side is selective (`<= 256` distinct keys), falling back to an in-memory hash join; other conditions (`<`, `!=`, ...) use a nested loop. `WHERE` is applied after the join, so very large unselective joins can still use substantial memory.
- In a `JOIN` query, an `ORDER BY` column must also appear in the `SELECT` list (otherwise an error is returned).
- Large deletes write a lot of WAL. One `DELETE FROM t WHERE id >= 0` over 10k, 20k and 40k rows (table with an INT primary key, an INT and a TEXT column, no secondary index) took about 157, 139 and 117 us per row, wrote about 6 KB of WAL per row, and raised the process peak memory by about 45, 59 and 82 MB (2-5 KB per row). Per-row time and memory fall as the row count grows, so do not extrapolate. Measured on a release build with the files on tmpfs, one run per size; real disks with slower fsync were not measured. Delete in batches (for example a few thousand rows per statement) for big tables. This is a known limitation, not yet optimized.
- Indexes on columns with few distinct values (many rows per value) slow inserts: inserting 100k rows with 1000 distinct indexed values ran about 3x slower and wrote about 9x more WAL than the same insert without the index (measured, one machine).
- A write that fills the WAL's disk fails with an error and the transaction is marked for recovery; nothing is silently lost. Keep enough free disk space for the WAL.
- `LIMIT $n` / `OFFSET $n`, `DEFAULT`, and composite indexes (max 4 columns, leftmost-prefix equality) are supported.
- `LIKE` (`%`, `_`) works on text columns: case-sensitive, no escape. A literal prefix (`'ab%'`) on an indexed column uses an index range scan; a leading `%` / `_` or an unindexed column does a full scan plus filter.
- `SAVEPOINT x`, `ROLLBACK TO x`, `RELEASE x` work inside `BEGIN ... COMMIT`.
- Foreign keys (RESTRICT or ON DELETE CASCADE), CHECK and DEFAULT are enforced by the Table/ORM API only; raw SQL INSERT/UPDATE/DELETE on such tables (and on FK parent tables) is refused, SELECT still works. Details in [docs/LIMITATIONS.md](docs/LIMITATIONS.md).
- One index value can match many rows (tested with thousands): the row list is stored in chunks. Updating or deleting one row of a very large group rewrites part of that group's list, so it gets slower as the group grows (not measured).
- `ALTER TABLE t DROP [COLUMN] c` (and `table.drop_column("c")`) is a **logical drop**: the column is renamed internally to `__dropped_<pos>` and hidden from `SELECT *`, rows are not rewritten, so the data stays on disk and no space is reclaimed. You cannot drop a primary key column, an indexed column, or the last live column. Column names that start with `__dropped_` are reserved and rejected.
- `INSERT` can write many rows: `INSERT INTO t VALUES (1,'a'), (2,'b')` (up to 1000 rows). With auto-commit it is all or nothing. Inside an explicit transaction, rows before a failing row stay in that transaction. `INSERT INTO t (b, a) VALUES (...)` also works; columns you leave out become NULL.
- `UPDATE ... SET col = col + N` and `col = col - N` work on `INT` columns (`N` is a whole number). `NULL` stays `NULL`, overflow gives an error and changes nothing. Other expressions (for example `a = b + 1`, `col * 2`) are not supported.
- `sync()` creates tables and adds nullable columns without losing data. It also switches a column between `NOT NULL` and nullable: making it `NOT NULL` fails with the first offending primary key if any row holds `NULL`, and the column stays nullable. It does not remove columns or change the primary key. A changed column type is refused unless you call `sync_allow_type_change()` (see below). If you remove `unique` from a column, `sync()` drops that unique index (rows are kept, but duplicates are allowed from then on).
- `ALTER TABLE t ALTER COLUMN c DROP NOT NULL`, `... SET NOT NULL` and `... TYPE <type>` work, with `table.set_nullable("c")`, `table.set_not_null("c", default)` and `table.change_type("c", "text")` as Rust calls. `SET NOT NULL` scans the table first. With a default, `set_not_null` fills the `NULL` rows in its own transaction and then sets the flag; these two steps are not atomic. A type change rewrites every row through a hidden shadow column: casts are strict (`INT`<->`TEXT`, `INT`<->`BOOL` for 0/1 only, `TEXT`<->`BYTES` for valid UTF-8), and the first bad row aborts the whole change. The changed column moves to the end of the column list (this changes the order for positional `INSERT`). Primary key and indexed columns are refused. These changes are not safe against concurrent writers. Failure injection was tested for page I/O errors only, not for WAL append or fsync errors.
- `WHERE` nesting depth is limited to 128.
- DDL (`CREATE`/`DROP`/`ALTER`) is not transactional: it takes effect at once and cannot be rolled back, so it is rejected inside `BEGIN ... COMMIT`.

**Network**
- One frame is at most 1 MB.
- There is one shared token. There are no users or roles.
- 5 failed logins in 60 seconds from one IP rate-limit that IP.
- Slow or silent connections are closed (handshake and frame timeouts).
- Binding to a non-local address requires TLS and a token. On localhost the default has no auth.
- When the database runs on a separate server, always use TLS and a token.

**Files**
- `.db`, `.wal` and backup files are set to owner-only (`0600`, backup folder `0700`) when opened or created. A small internal free-list file may still be created with the default permissions, so also run the server with `umask 077` (systemd: `UMask=0077`).
- Backup is a Rust call (`server.backup(path)`), not an SQL command. It refuses an existing destination folder.
- There is no `CREATE DATABASE`. MITRDB is an embedded single-node engine: one database file is one database (`Server::open(path)`).

## Benchmarks

All numbers are single runs on one development machine (4 cores, Linux). Real-disk numbers are on btrfs, where one fsync takes about 12 ms (measured with `dd oflag=dsync`). Run-to-run spread on real disk is large, so treat them as indicative only.

| Workload | Storage | Result |
|---|---|---|
| Insert, one explicit transaction, row by row (`perf_bench batch`) | tmpfs | about 50,000 rows/s |
| Same | real disk | about 36,000 - 40,000 rows/s |
| Autocommit, 1000 rows per `INSERT ... VALUES`, 1M rows | real disk | about 16,000 - 28,000 rows/s (3 runs) |
| Autocommit, 1 row per statement, 1 thread | real disk | about 77 rows/s (fsync-bound) |
| Autocommit, 1 row per statement, 8 threads | real disk | about 300 rows/s (group commit) |
| Autocommit, 100 rows per statement, 1 to 8 threads (default session) | real disk | about 5,500 to 14,000 rows/s |

Other measured points (1M rows, about 50 bytes per row, real disk): full scan 3.9 s, point lookup about 300 us, `CREATE INDEX` 20 s, peak memory about 960 MB, reopen about 1.5 s after the open-time checkpoint.

Reproduce with `./scripts/bench.sh` and the examples in `crates/mitrdb-server/examples`.

## Verified checks

Every item was run a small number of times on one machine; counts are listed. This is
test evidence for a pilot, not a guarantee. See docs/LIMITATIONS.md and docs/PILOT.md.

**Power loss** (QEMU VM, hard `virsh destroy`, `cache=directsync`, ext4, 4 writer threads;
each cut verified: acked rows present, no phantom rows, no duplicates, DB accepts new
writes after recovery):
- INSERT only: 10 cuts
- INSERT/UPDATE/DELETE soak: 4 cuts, plus one ~100 min soak (WAL rotated through 5 segments) with 1 cut
- Multi-table `BEGIN..COMMIT` + counter `UPDATE n = n + 1`: 4 cuts, atomicity held
- DDL loop (CREATE TABLE, INSERT, CREATE INDEX, ALTER ADD COLUMN, DROP TABLE), single thread: 4 cuts

**Process crash (`kill -9`, host):** online backup with concurrent writers (each backup
restored and verified); TCP + TLS with 100 and 200 clients (wrong token refused,
connection limit refused cleanly).

**Earlier process-crash checks (one run each):** disk full on an 8 MB tmpfs fails inserts
with a clean `No space left on device` error, and after restart every acknowledged row is
present (lost=0, phantom=0, duplicates=0); 1M rows insert, scan, point lookups and index
creation give correct results; a 1M-row database backed up in 2.8 s under 4 inserting
threads and restored with no missing, corrupt or duplicate rows.

**Bugs found and fixed by these drills:** recovered page frame overwritten after
recovery, no exclusive directory lock (two processes corrupted the WAL), torn WAL tail
rejected instead of truncated, rotated WAL segments created world-readable.

**Not tested:** other filesystems/hardware, concurrent DDL, DDL together with DML,
default connection limit with 200 clients, runs longer than ~100 min.

## Design notes

- **Write-ahead rule.** A page is never written to disk before the log records describing
  its changes are durable.
- **Group commit.** One thread becomes the flush leader, writes and fsyncs a batch of
  pending records outside the WAL mutex, then wakes the waiting committers. Operations that
  do WAL I/O under the mutex (flush-all, checkpoint, backup) wait for any active leader
  first, so two writers never touch the same segment at once.
- **Checksums.** Pages, WAL records, checkpoint metadata and backup files are all CRC32C
  protected, and corruption fails closed rather than being silently accepted.

## Roadmap

- [ ] Faster inserts: in-place leaf updates with smaller WAL records
- [ ] Reduce per-row lock bookkeeping
- [x] Non-unique indexes in `sync()` / `define_table`
- [x] Prepared statements and `$n` parameter binding (`execute_sql_params`)
- [ ] Space reclaim for dropped columns (table rewrite)
- [ ] Containerized, Kubernetes-friendly deployment (StatefulSet with persistent volumes)

Out of scope: NoSQL/document store, replication, consensus, sharding. MITRDB stays a single-node, Linux-only SQL engine.

## Contributing

The project is in early development and the internals are still changing quickly. Issues and discussion are welcome.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
