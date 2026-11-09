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
- [x] Failed COMMIT (serializable conflict / WAL append fail) txn ko leak nahi karta: `commit_or_abort` ab abort karta hai, locks chhootte hain. stmt_atomicity 7 tests (earlier.contains branch, LockTimeout ke baad rollback, failed commit); negative control: fix hatane par LockTimeout se FAIL (e558cab)
- [x] UPDATE ko PK range par narrow karna: autocommit `UPDATE ... WHERE id = k` 2.9ms / 11.8ms / 37.5ms (N=1k/4k/16k, N ke saath badhta) -> 65 / 50 / 61us flat, tmpfs. update_pk_fastpath 7 tests + update_autocommit_probe (e7247a4)
- [x] Auto-checkpoint: Server har 256 statements par WAL bytes (segments * max segment size) dekhta hai, 256MB se upar checkpoint chalata hai (floor backoff, ek waqt par ek). 1MB segments par 2 churn ke baad 22 -> 1 segment; chalte txn ke saath sahi (pinned, commit ke baad count aur reopen sahi). ckpt_reclaim 6 tests (6th: concurrent, 6906488); insert A/B noise
- [x] split_leaf ab old_next leaf ko write se PEHLE padhta hai: failed split ke WAL records chain se bahar nahi jaate, tree aadha nahi badalta. Probe measured: pehle committed keys 10/12/14 gayab. btree_split_atomicity; negative control FAIL (prev_lsn 48 vs 46). Insert A/B 3 rounds x 5 runs: noise
- [x] commit(): transient WAL flush failure ab 5 baar retry hota hai (10..160ms backoff, sirf error branch). commit_flush_retry; negative control: transient test FAIL bina fix. Insert A/B: noise (wahi binary khud se 5.0% alag, new prev2 se 0.1%)
- [x] `resolve_poisoned_commit(txn)`: poisoned commit ka commit LSN yaad rehta hai; WAL theek hone par dobara flush + commit pura (commit_ts, locks release), abort kabhi nahi. commit() ka tail `finish_commit` helper mein. poisoned_commit_resolve 2 tests; Insert A/B: noise (new +0.8%, identical-binary control -0.1%)
- [x] Poisoned commit sweep: server har statement se pehle (sirf poisoned count > 0 par) resolve_poisoned_commits chalata hai; WAL theek hote hi atke locks chhoot jaate hain. txn-level + server-level test (next WAL segment path block karke), negative control: HEAD par server test FAIL. Insert A/B: noise (new -0.4%, identical-binary control +0.3%)
- [x] Leaf split se pehle poora split chain padha jaata hai (ab `plan_split` mein, btree.rs): split_internal child-read failure par ab koi WAL record/page write nahi, keys gayab nahi. btree_split_atomicity2; negative control FAIL bina fix. Insert A/B: noise (new -2.9%, identical-binary control -3.0%)
- [x] new_page failure atomicity: leaf split / parent full / root split / split_internal mein new_page fail par ab tree aadha nahi badalta. `plan_split` poori chain padhke exact new-page count nikalta hai, `alloc_split_pages` saare pages kisi write se PEHLE allocate karta hai, fail par free. Probe v2 measured: pehle committed keys 2-3 / 8-10 / 23-31 gayab (k=1/2/3). btree_newpage_fail_atomicity pass; HEAD par 1705 injected failures, 0 lost, 0 phantom (75800ba, 9f6b5d8)
- [x] Btree write_node atomicity (f974100): step 3 (LSN stamp) gap closed. BufferPoolManager::with_page se zero-copy read inspection add kiya; ab payload copy + LSN stamp + CRC update ek single atomic mutate_page mein tabhi hoti hai jab WAL durable append ho chuka ho. btree_multiwrite_atomicity_probe (fault-injection k=0..6 sweep) verified: Fail-at 4 par k20=PRESENT, 0 keys lost
- [x] Explicit-txn bench (uncontended): perf_bench ka `batch` mode pehle se explicit txn hai (N=10000, tmpfs: 53.3k rows/s, per-insert p50 15us, 2 WAL syncs; autocommit 34.5k rows/s, p50 25us, 10002 syncs). write_mark_probe: `write_mark` 48-72 ns/call single thread (6339cad)
- [x] write_mark contended worst-case microbench (write_mark_contended, c477f1c): T=1 median 40.6-44.3 ns/call, T=2 161-218 ns, T=4 bimodal 283-1618 ns
- [x] write_mark asli put ke saath (write_mark_real, 66ecb5b): mark vs nomark median ops/s T=1 +0.9%, T=2 +3.6%, T=4 +0.4% (noise ke andar). put ~58-64us/op T=1, write_mark ~44-58ns (~0.1%)
- [x] Poisoned commit ka WalCorrupted label theek: group-commit leader ka flush error ab apna variant (Io) rakhta hai, WAL_CORRUPTED nahi dikhta. poisoned_commit_label (815d4e9)
- [x] Poisoned commit ke baaki tukde (band, fix nahi, by design / measured limits): retry window measured ~312ms; blocked WAL + poisoned commit connect-to-PONG 5.4ms med; sweep statement path aur network idle loop (862c787) par verified
- [x] Auto-checkpoint error ab metrics mein: `checkpoint_errors` counter (failed auto-checkpoint par badhta hai, statement fail nahi hota; 5564c46)
- [x] Auto-checkpoint floor backoff ki ginti measure hui (ckpt_floor_backoff, 00605ea): pinned WAL par fix ke saath 7 checkpoints, final_segments=17 (bound segs/2+2)
- [x] Auto-checkpoint error par log nahi: by design, `checkpoint_errors` counter hi interface hai (5564c46)
- [x] Bulk delete memory: WAL arena beech mein flush (har 64 rows par, pending > 16MB). N=20000 VmHWM delta 259MB -> 59MB (measured; 9a5a857)
- [x] Bulk UPDATE memory: WAL arena beech mein flush (exec_update put loop, har 64 rows par, pending > 16MB). `set name` N=20000 VmHWM delta 143MB -> 72MB (measured; 786939f)
- [x] Bulk delete/update memory phase breakdown (mem_probe, 4dad829): update_g retained ~950 B/row, peak ~1.9KB/row. WAL arena Vec capacity constant block identified
- [x] WAL arena capacity flush ke baad wapas milti hai (`drain_pending` mein shrink, cap > 4MB aur len < cap/4): test mein cap 32MB -> 0MB (4c465c5)
- [x] Arena shrink ke baad mem_probe + bulk tests dobara: constant block free (~31MB drop), peak memory digit-to-digit same
- [x] WAL pressure limit 16/12/8MiB mem_probe: 16MiB optimal decision (`WAL_PRESSURE_LIMIT_BYTES`, 4497ef6)
- [x] Non-PK indexed column par delete/update point predicates (`= k`, `IS NULL`, OR): `narrow_dml_entries` secondary index point lookup. Speedup ~30x (82bb365)
- [x] Non-PK indexed column par delete/update range predicates (`g > 990`, `u >= k`): `narrow_dml_entries` range collection + cap 4096 fallback. Speedup ~11x (72775a3)
- [x] Index write amplification, UPDATE ka kaaran (396f2ac): `exec_update` ab index-entry lock skip karta hai jab index key aur PK dono unchanged hon
- [x] P0: nullable / NOT NULL / column type change explicit API + `sync()` wiring (`d77d4c3`, `d67e425`, `ef89dfe`, `a068428`)
- [x] README: `sync()` NOT NULL/type handling aur ALTER COLUMN limits documented (a078848)
- [x] README: delete limit naye numbers se update (2822eef)
- [x] ~/mitrdb_ab/delprev worktree hataya
- [x] `.bak*` files source dirs se clean kiye

## P0: Production Readiness & Memory Safety
- [x] `insert_many` par WAL pressure hook: `PhysicalPlan::InsertMany` mein har 64 rows par 16MB pressure check (`relieve_wal_pressure`); `wal_pressure_insert_flush` crash recovery test + `insert_many_wal_pressure_test` rollback test verified (bf0fc73)
- [x] Flush payload single-copy: `collect_flush_work` (`wal.rs`) group-commit leader path refactored to ping-pong arena hand-off; duplicate heap vector allocation eliminated.
- [x] Bulk DML transient memory profiling: measured per-structure breakdown (~136B write_set, ~136B LockManager, ~248B MVCC store; ~520B/row total transient heap across 5k rows).
- [ ] Btree multi-write remaining edge cases (low priority): `free_btree_page`, delete merge/borrow/rebalance_internal error-path isolation aur buffer pool new_page conflict free-page leak guard.

## Performance
- [ ] Batch delete per leaf: single-row leaf shift se ~6KB WAL/row banta hai; per-leaf batch shift implement karke write amplification ghatana.
- [ ] Index write amplification (wide ranges): `u >= 0` par `ta_i` ka +20% slowdown measure karna aur DELETE lock loop verify karna.
- [ ] btrfs autocommit fsync floor: ~13ms fdatasync floor ko concurrent group commit aggregation se mitigate karna.
- [ ] (parked) Freelist reuse save: 3-4% fsync reduction (low priority).

## Features & DDL
- [ ] ALTER limitations address karna: column position order maintain karna aur `set_not_null(default)` ko atomic banana.
- [ ] Concurrent DDL locking: ALTER table execution ke dauran concurrent writers ko actively lock karna (sirf point-in-time check nahi).
- [ ] PK change support: new key ke mutabiq table tree aur secondary indexes rebuild karna.
- [ ] Indexed column type change: secondary indexes drop karke rebuild ke saath type cast support karna.
- [ ] DDL crash fault-injection: ALTER TYPE ke WAL append failure aur fsync failure paths par test coverage add karna.

## P1: Architecture & Concurrency
- [ ] Concurrent B-Tree (read/write scaling): MvccMutex contention hatane ke liye concurrent B-tree design doc aur implementation (T=4 put scaling fix).
- [ ] Query Planner: JOIN execution mein WHERE predicate pushdown support.

## P2 & Housekeeping
- [ ] `define_table` builder mein type change ka opt-in variant add karna.
- [ ] `free_pages_batch` crash-injection test: blank pages write ke baad freelist persist se pehle crash scenario.
- [ ] Workspace `cargo fmt` clean run (isolated commit).
- [ ] CI badge aur repository branding update (ANWEDB -> MITRDB).
- [ ] Documentation: Rust API docs, SQL REPL guide, backup/restore CLI tooling, enhanced error context.

## Rules (short)
Perf girni nahi chahiye (A/B 3 rounds, 3% se kam = noise). Hot path (`exec_insert`, `allocate_page`) par A/B zaroori. Har step ek commit, push maintainer ke kehne par.
