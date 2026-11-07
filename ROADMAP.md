# MITRDB Roadmap

Status tracker. `[x]` = done and measured/tested, `[ ]` = pending.
"measured" = terminal output se confirm, "andaza" = abhi measure nahi hua.

## Done
- [x] B+Tree, MVCC + Serializable, WAL + crash recovery + group commit, checkpoint + vacuum, backup/restore, indexes, SQL, TCP + TLS, metrics, CI
- [x] Read-only commit ka WAL fsync skip (7c0f366)
- [x] Batch free_pages: N=5000 delete fsync 249 -> 9, delete 3.4-3.8s -> ~0.4s (38066d1)
- [x] DELETE ko PK range par narrow karna: autocommit delete 594-6045us (N ke saath badhta) -> 57-88us flat, tmpfs. Bulk delete A/B noise (N=10000, 3 rounds) (1e20f9d)
- [x] Delete probes (delete_probe, delete_autocommit_probe) + delete_pk_fastpath tests (417d5bd)
- [x] Explicit txn statement atomicity: failed DML statement ab sirf apne writes undo karta hai, txn zinda rehta hai (`write_mark` / `rollback_to_mark`, WAL-logged undo). stmt_atomicity 3 tests + txn/exec/server/storage crash suites pass. Insert A/B 3 rounds x 5 runs: noise (c83ca6b)

## P0: Production se pehle
- [ ] Commit error par txn leak (I/O error par hi; statement atomicity se alag). Commit limitation: write beech mein fail + explicit COMMIT par bina fsync commit ho sakta hai; `ctx.last_lsn` update nahi hota (durability)
- [ ] Explicit-txn bench: perf_bench autocommit hai, `write_mark` ka mutex measure nahi hua (earlier.contains branch aur LockTimeout ke baad statement rollback ke tests ho chuke, stmt_atomicity.rs)
- [ ] WAL growth / auto-checkpoint: non-test code mein auto-checkpoint nahi. Measured: 2000 delete + 2000 reinsert ke baad t.wal 13.4MB
- [ ] `exec_update` ka `scan_unlocked` (O(N) autocommit UPDATE ... WHERE id = k). Code-confirmed, latency measure nahi hua
- [ ] Delete memory: N=10000 bulk delete par VmHWM 153MB (measured). Streaming ya beech mein flush; storage core hai, crash tests + A/B zaroori

## Performance
- [ ] Non-PK indexed column par delete/update (`WHERE g = 5`) abhi full scan (andaza: sirf PK ke liye ranges lagi)
- [ ] Delete par ~6KB WAL/row (leaf shift): batch delete per leaf
- [ ] Index write amplification: pehle index wala bench variant (perf_bench mein index nahi)
- [ ] btrfs par autocommit ~13ms WAL fdatasync floor; sirf concurrent commits se kam hoga
- [ ] (parked) Freelist save on reuse: measured sirf 3-4% fsync, low priority

## Features
- [ ] P0: column type / nullable / PK change ka explicit API (pehle design)
- [ ] P1: query planner (JOIN mein WHERE pushdown nahi)
- [ ] P1: concurrent read/write
- [ ] P2: Rust API docs, SQL REPL, backup/restore CLI, better errors, release packaging

## Housekeeping
- [ ] README measured limits mein naya delete number (57-88us autocommit)
- [ ] CI badge + repo naam ANWEDB -> MITRDB
- [ ] free_pages_batch crash-injection test (blank pages likhe, freelist save se pehle process mare)
- [ ] ~/mitrdb_ab/delprev worktree saaf karna
- [ ] Repo fmt-clean karna (alag commit, logic ke saath nahi)

## Rules (short)
Perf girni nahi chahiye (A/B 3 rounds, 3% se kam = noise). Hot path (`exec_insert`, `allocate_page`) par A/B zaroori. Har step ek commit, push maintainer ke kehne par.
