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
