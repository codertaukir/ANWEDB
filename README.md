# ANWEDB

A transactional database engine written in Rust.

ANWEDB is built from the storage layer up: a B+Tree storage engine, a write-ahead log with
crash recovery, MVCC transactions with serializable range locking, and online backup.
It is currently a **single-node** engine in **early development**.

> **Status:** early development. APIs, on-disk formats and behaviour may change.
> Not production ready.

---

## Features

**Storage**
- B+Tree storage engine with variable-length keys and values, leaf/internal splits, merges and rebalancing
- Fixed-size pages with CRC32C checksums (torn or corrupted pages are detected and rejected)
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
- Linux is the primary development platform

### Build
```bash
cargo build --release
```

### Run the tests
```bash
cargo test -p mitrdb-txn
cargo test -p mitrdb-storage
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

### Big files

A DB value holds up to about 3900 bytes. Bigger data (photos, videos) goes to disk with `FileStore`; the DB keeps only the file name in a normal `text` column.

```rust
let files = FileStore::open("data/files")?;
let saved = files.save(&mut reader)?;        // any std::io::Read, written in 1 MB chunks
// store saved.name in a text column
let mut file = files.open_read(&saved.name)?;
files.delete(&saved.name)?;
```

Files get random 32-character names, are written to a temp file and renamed, and use owner-only permissions. `FileStore` does not limit file size, does not encrypt, and does not delete a file when a row is deleted: that is the caller's job.

## Limits

Know these before you use ANWEDB.

**Data**
- Types: `INT`, `TEXT`, `BOOL`, `BYTES`.
- A value can be about 3900 bytes. Bigger values give a clean `OUT_OF_SPACE` error. Store big files outside the database and keep only the path or name in a text column.
- A `UNIQUE` column allows many `NULL` values; real duplicate values are rejected. All `NULL` rows share one index entry, so concurrent writes of `NULL` into the same unique column run one after another (from code reading, not measured).

**SQL**
- Not supported yet: `DROP COLUMN`, `DEFAULT`, `IF NOT EXISTS`, `$n` parameters, composite indexes, joins.
- `INSERT` writes one row at a time.
- `UPDATE ... SET col = col + N` and `col = col - N` work on `INT` columns (`N` is a whole number). `NULL` stays `NULL`, overflow gives an error and changes nothing. Other expressions (for example `a = b + 1`, `col * 2`) are not supported.
- `sync()` creates tables and adds nullable columns without losing data. It does not remove columns, change types, or remove `UNIQUE`.
- `WHERE` nesting depth is limited to 128.

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

## Benchmarks

Single process, single thread, 10,000 rows inserted in one transaction (batch mode),
16 MB buffer pool, durable commit with fsync:

| | Durable insert throughput |
|---|---|
| Result on a development machine | roughly 25,000 rows/sec |

This is a single-thread baseline, not a concurrency benchmark. Numbers vary between
machines and between runs, so treat them as indicative only.

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
- [ ] Replication (leader and followers, using the WAL)
- [ ] Consensus for automatic leader election
- [ ] Containerized, Kubernetes-friendly deployment (StatefulSet with persistent volumes)
- [ ] Sharding

## Contributing

The project is in early development and the internals are still changing quickly. Issues
and discussion are welcome once the repository is public.

## License

Not yet decided. Until a license is added, all rights are reserved.
