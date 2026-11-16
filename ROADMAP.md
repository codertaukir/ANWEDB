# MITRDB Roadmap

Status tracker. `[x]` = done and measured/tested, `[ ]` = pending.
"measured" = terminal output se confirm, "andaza" = abhi measure nahi hua.

## Done
- [x] Crash recovery ARIES local CLR anomaly & split reconciliation (19ebf03): local CLR updates se page LSN advance hone par contiguous backward undo slices ko stale samajhne ka bug fix kiya (`generated_clrs` tracking). Page splits/churn se skipped uncommitted records ko capture karne ke liye `SkippedUndo` mechanism add kiya aur `Server::open` mein `catalog.all_table_trees()` ke zariye post-recovery B+Tree key reconciliation implement ki. `ckpt_reclaim` aur `v1_group_f_server_tests` 100% pass, insert throughput ~52k rows/sec maintain raha.
- [x] B+Tree, MVCC + Serializable, WAL + crash recovery + group commit, checkpoint + vacuum, backup/restore, indexes, SQL, TCP + TLS, metrics, CI
- [x] Read-only commit ka WAL fsync skip (7c0f366)
- [x] Batch free_pages: N=5000 delete fsync 249 -> 9, delete 3.4-3.8s -> ~0.4s (38066d1)
- [x] DELETE ko PK range par narrow karna: autocommit delete 594-6045us (N ke saath badhta) -> 57-88us flat, tmpfs. Bulk delete A/B noise (N=10000, 3 rounds) (1e20f9d)
- [x] Delete probes (delete_probe, delete_autocommit_probe) + delete_pk_fastpath tests (417d5bd)
- [x] Explicit txn statement atomicity: failed DML statement ab sirf apne writes undo karta hai, txn zinda rehta hai (`write_mark` / `rollback_to_mark`, WAL-logged undo). stmt_atomicity 3 tests + txn/exec/server/storage crash suites pass. Insert A/B 3 rounds x 5 runs: noise (c83ca6b)
- [x] Failed COMMIT (serializable conflict / WAL append fail) txn ko leak nahi karta: `commit_or_abort` ab abort karta hai, locks chhootte hain. stmt_atomicity 7 tests (earlier.contains branch, LockTimeout kel baad rollback, failed commit); negative control: fix hatane par LockTimeout se FAIL (e558cab)
- [x] UPDATE ko PK range par narrow karna: autocommit `UPDATE ... WHERE id = k` 2.9ms / 11.8ms / 37.5ms (N=1k/4k/16k, N ke saath badhta) -> 65 / 50 / 61us flat, tmpfs. update_pk_fastpath 7 tests + update_autocommit_probe (e7247a4)
- [x] Auto-checkpoint: Server har 256 statements par WAL bytes (segments * max segment size) dekhta hai, 256MB se upar checkpoint chalata hai (floor backoff, ek waqt par ek). 1MB segments par 2 churn ke baad 22 -> 1 segment; chalte txn ke saath sahi (pinned, commit ke baad count aur reopen sahi). ckpt_reclaim 6 tests (6th: concurrent, 6906488); insert A/B noise
- [x] split_leaf ab old_next leaf ko write se PEHLE padhta hai: failed split ke WAL records chain se bahar nahi jaate, tree aadha nahi badalta. Probe measured: pehle committed keys 10/12/14 gayab. btree_split_atomicity; negative control FAIL (prev_lsn 48 vs 46). Insert A/B 3 rounds x 5 runs: noise
- [x] commit(): transient WAL flush failure ab 5 baar retry hota hai (10..160ms backoff, sirf error branch). commit_flush_retry; negative control: transient test FAIL bina fix. Insert A/B: noise (wahi binary khud se 5.0% alag, new prev2 se 0.1%)
- [x] `resolve_poisoned_commit(txn)`: poisoned commit ka commit LSN yaad rehta hai; WAL theek hone par dobara flush + commit pura (commit_ts, locks release), abort kabhi nahi. commit() ka tail `finish_commit` helper mein. poisoned_commit_resolve 2 tests; Insert A/B: noise (new +0.8%, identical-binary control -0.1%)
- [x] Poisoned commit sweep: server har statement se pehle (sirf poisoned count > 0 par) resolve_poisoned_commits chalata hai; WAL theek hote hi atke locks chhoot jaate hain. txn-level + server-level test (next WAL segment path block karke), negative control: HEAD par server test FAIL. Insert A/B: noise (new -0.4%, identical-binary control +0.3%)
- [x] Leaf split se pehle poora split chain padha jaate hai (ab `plan_split` mein, btree.rs): split_internal child-read failure par ab koi WAL record/page write nahi, keys gayab nahi. btree_split_atomicity2; negative control FAIL bina fix. Insert A/B: noise (new -2.9%, identical-binary control -3.0%)
- [x] new_page failure atomicity: leaf split / parent full / root split / split_internal mein new_page fail par ab tree aadha nahi badalta. `plan_split` poori chain padhke exact new-page count nikalta hai, `alloc_split_pages` saare pages kisi write se PEHLE allocate karta hai, fail par free. Probe v2 measured: pehle committed keys 2-3 / 8-10 / 23-31 gayab (k=1/2/3). btree_newpage_fail_atomicity pass; HEAD par 1705 injected failures, 0 lost, 0 phantom (75800ba, 9f6b5d8)
- [x] Btree write_node atomicity (f974100): step 3 (LSN stamp) gap closed. BufferPoolManager::with_page se zero-copy read inspection add kiya; ab payload copy + LSN stamp + CRC update ek single atomic mutate_page mein tabhi hoti hai jab WAL durable append ho chuka ho. btree_multiwrite_atomicity_probe (fault-injection k=0..6 sweep) verified: Fail-at 4 par k20=PRESENT, 0 keys lost
- [x] Explicit-txn bench (uncontended): perf_bench ka `batch` mode pehle se explicit txn hai (N=10000, tmpfs: 53.3k rows/s, per-insert p50 15us, 2 WAL syncs; autocommit 34.5k rows/s, p50 25us, 10002 syncs). write_mark_probe: `write_mark` 48-72 ns/call single thread (6339cad)
- [x] write_mark contended worst-case microbench (write_mark_contended, c477f1c): T=1 median 40.6-44.3 ns/call, T=2 161-218 ns, T=4 bimodal 283-1618 ns
- [x] write_mark asli put ke saath (write_mark_real, 66ecb5b): mark vs nomark median ops/s T=1 +0.9%, T=2 +3.6%, T=4 +0.4% (noise ke andar). put ~58-64us/op T=1, write_mark ~44-58ns (~0.1%)
- [x] Poisoned commit ka WalCorrupted label theek: group-commit leader ka flush error ab apna variant (Io) rakhta hai, WAL_CORRUPTED nahi dikhta. poisoned_commit_label (815d4e9)
- [x] Auto-checkpoint error ab metrics mein: `checkpoint_errors` counter (failed auto-checkpoint par badhta hai, statement fail nahi hota; 5564c46)
- [x] Auto-checkpoint floor backoff ki ginti measure hui (ckpt_floor_backoff, 00605ea): pinned WAL par fix ke saath 7 checkpoints, final_segments=17 (bound segs/2+2)
- [x] Bulk delete memory: WAL arena beech mein flush (har 64 rows par, pending > 16MB). N=20000 VmHWM delta 259MB -> 59MB (measured; 9a5a857)
- [x] Bulk UPDATE memory: WAL arena beech mein flush (exec_update put loop, har 64 rows par, pending > 16MB). `set name` N=20000 VmHWM delta 143MB -> 72MB (measured; 786939f)
- [x] WAL arena capacity flush ke baad wapas milti hai (`drain_pending` mein shrink, cap > 4MB aur len < cap/4): test mein cap 32MB -> 0MB (4c465c5)
- [x] Non-PK indexed column par delete/update point predicates (`= k`, `IS NULL`, OR): `narrow_dml_entries` secondary index point lookup. Speedup ~30x (82bb365)
- [x] Non-PK indexed column par delete/update range predicates (`g > 990`, `u >= k`): `narrow_dml_entries` range collection + cap 4096 fallback. Speedup ~11x (72775a3)
- [x] Index write amplification, UPDATE ka kaaran (396f2ac): `exec_update` ab index-entry lock skip karta hai jab index key aur PK dono unchanged hon
- [x] P0: nullable / NOT NULL / column type change explicit API + `sync()` wiring (`d77d4c3`, `d67e425`, `ef89dfe`, `a068428`)
- [x] Batch delete per leaf (4042419, fdc2679): `delete_batch_logged` (btree.rs) + `rebalance_leaf` isolation, `delete_batch` (mitrdb-txn), aur `exec_delete` wiring. N=5000 delete_probe: 257us/row, 81 page writes, 0 evictions; delete_autocommit_probe: N=2000 avg=741us, p50=718us, 32 page writes, 0 evictions
- [x] High-concurrency B+Tree insert scaling, WAL zero-copy batching & hint_epoch safety (`e95efea`..`87daf71`): 64-shard `txn_table`, `mimalloc` global allocator (`b299f41`), 47-byte packed WAL header + batch txn tracking (`57bb563`), and thread-local WAL scratch buffers (`06c56c5`, `ce051b2`).
- [x] Concurrent DDL locking & isolation (4875019): DDL statements par `DdlTableLockGuard` exclusive table lock acquire karta hai; DML execution par `acquire_table_shared` integrate kiya. `schema_persist` test suite + poora workspace tests green (100% passed).

- [x] Concurrent insert vs scan: leaf split ab leaf latch ke andar fresh read karta hai aur right leaf pehle likhta hai (a686985): load mein ckpt_reclaim concurrent test 94-97/100 -> 100/100 (lost update + `kind byte: 0` zero-page read). buffer_pool update_page_data single write guard, page_lsn monotonic (1869337). Insert A/B batch tmpfs 6 round x 2: median OLD 49.5k vs NEW 49.5k rows/s, noise

- [x] range_scan_directed: candidate key set bulk build, raw_values HashMap, extend_keys_in_range redundant clone skip: full-scan p50 (N=20000, 10 rounds A/B interleaved) 72.1ms -> 58.2ms, insert noise (53.4k vs 53.4k rows/s). txn/exec/server tests + ckpt_reclaim 100/100

## P0: Production Readiness & Memory Safety
- [x] B+Tree free_btree_page pin safety (caf1b77): Page ko WAL append tak pinned rakha taaki WAL append ke dauran eviction race na bane; WAL failure par safe unpin aur clean error return. `ckpt_reclaim` aur recovery suites pass.
- [x] `insert_many` par WAL pressure hook: `PhysicalPlan::InsertMany` mein har 64 rows par 16MB pressure check (`relieve_wal_pressure`); `wal_pressure_insert_flush` crash recovery test + `insert_many_wal_pressure_test` rollback test verified (bf0fc73)
- [x] Flush payload single-copy: `collect_flush_work` (`wal.rs`) group-commit leader path refactored to ping-pong arena hand-off; duplicate heap vector allocation eliminated.
- [x] Bulk DML transient memory profiling: measured per-structure breakdown (~136B write_set, ~136B LockManager, ~248B MVCC store; ~520B/row total transient heap across 5k rows).
- [x] Btree multi-write remaining edge cases: `free_btree_page`, delete merge/borrow/rebalance_internal error-path isolation aur buffer pool new_page conflict free-page leak guard completely verified across 1700+ injected fault points.

## P1: Architecture & Concurrency
- [x] Query Planner & Join Execution Optimization (5fa0485, cea2550): Planner predicate pushdown & zero-allocation hash join probe completed and tested.
- [x] WalManager lock: measured, no actionable gain. Lock wait ~12% + hold ~8% of TxnPut (write_mark_real 4T), but WAL is only ~2.4% CPU in perf. txn_trackers cheap-hasher A/B (12 runs): median 355.9k vs 359.4k ops/s, noise. Atomic next_lsn alone is unsafe (arena must stay in LSN order); lock-free reservation not attempted (recovery risk, gain bounded by ~20% of put time)

## P2 & Housekeeping
- [x] `free_pages_batch` crash-injection test (dedb3e2): blank pages write ke baad stale/missing freelist crash scenario par disk rebuild recovery verified; page leaks eliminated aur allocator reuse validated.
- [x] B+Tree self-deadlock fix (78b3bfc): `try_mutate_leaf_in_place` ab next leaf ka first key leaf frame write lock lene se PEHLE padhta hai (pehle lock ke andar doosra page pin hota tha, buffer pool pressure mein `find_victim_frame_sharded` apne hi write-locked frame par `.read()` pe atak jata tha). `btree_newpage_fail_atomicity` hang theek (pre-existing, ee9920b par bhi tha). Verified: sab fault-injection targets pass, workspace 330 passed, ckpt_reclaim 100/100 under load, write_mark_real 4T A/B median old 73.7k vs new 74.6k ops/s (+1.1%, noise ke andar; bench hint path pe zyada chalta hai, non-hint path ka alag A/B nahi kiya).

## ORM + SQL features (session: ORM completion)
- [x] mitrdb-orm + derive(Model): struct-as-schema, typed queries, update/delete/count (d982715, 2cf6b66)
- [x] Auto-increment id: `#[col(primary_key, auto)]`, `insert_get_id`, `Table::insert_auto` (4fbc675)
- [x] Timestamp (i64 micros), `#[col(created_at)]`/`#[col(updated_at)]`, f64 as order-preserving int (aa65c8a). Caveat: f64 par raw SQL SUM/AVG galat, NaN mat daalo
- [x] SQL `LIKE` (`%`, `_`, case-sensitive, text only, full scan + filter) (d6263f8); ORM `Col::like` (38e317a)
- [x] Auto id kabhi reuse nahi hoti: `mitrdb_seq` counter table, CAS update (52d3cab)
- [x] Perf A/B (write_mark_real 4T, 10 round): OLD 2cf6b66 248.5k vs NEW 245.0k ops/s (-1.4%, noise). Purana 73k vs 368k fark = noisy machine, engine regression nahi
- [ ] Baaki: default values (Rust `Default` se kaam chalta hai), foreign key, CHECK, savepoints, native Timestamp/Float DataType, non-hint path A/B
- [x] SAVEPOINT / ROLLBACK TO / RELEASE: `TransactionContext.savepoints`, `write_mark` + `rollback_to_mark` par based, COMMIT/ROLLBACK par khud saaf (c36e383)
- [x] Foreign key (RESTRICT): `#[col(references = "table.col")]`, `Table::add_foreign_key`, defs `mitrdb_fk` mein, catalog format untouched. Limit: sirf Table/ORM API enforce karta hai, raw SQL bypass, check+write atomic nahi (353fbac)
- [x] CHECK: `#[col(check = ">= 0")]`, `Table::add_check`, defs `mitrdb_check` mein, insert/update par, NULL pass, purani rows verify nahi. f64 column par mat lagao (55b1a5f)
- [x] Perf A/B #2 (write_mark_real 4T, 12 round): OLD 2cf6b66 82.1k vs NEW 83.9k ops/s (+2.2%, noise). Absolute numbers machine load se badalte hain, sirf relative dekho
- [x] Non-hint path A/B (perf_bench batch 20000 rows, 12 round): OLD 78.5k vs NEW 78.3k rows/s (-0.2%, noise)
- [x] Concurrency bugs mile aur fix hue: internal table create race (mitrdb_seq/fk/check "already exists"), FK check+write race (1500 rounds mein 21 orphans -> 0, process-wide FK lock). Limit: lock sirf ek process ke andar, raw SQL bypass (4859919 aur ye commit)
- [x] Docs: README ORM section, docs/ORM.md, docs/LIMITATIONS.md, orm quickstart example (03d6af8, 2bb3f9b)
- [ ] Baaki (optional): native Timestamp/Float DataType, DEFAULT in schema, FK cascade, engine-level FK/CHECK (raw SQL enforce), cross-process FK safety

## Session 2: done (local commits, push baaki ho to pehle push karo)
- [x] Native TIMESTAMP (tag 5, int micros) (d4e3501)
- [x] Native FLOAT (tag 6, order-preserving key, NaN reject), ORM f64 native (5ce9153)
- [x] Composite (multi-column) index, unique + non-unique, max 4 cols (5579daa)
- [x] ORM unique_together / index (5611b94)
- [x] DEFAULT (mitrdb_default) + FK ON DELETE CASCADE (0815634)
- [x] ORM OR groups, in_list, between, is_null (e78b52b)
- [x] Chat app end-to-end test (6efc3e5)
- [x] Planner: composite index leftmost-prefix equality (a4ef1a6)
- [x] Insert cost measure: plain 120us vs FK+CHECK 159us (+32%), cache zaroori nahi, mat banao

## Baaki (is order mein karo, har step ke baad sirf cargo check; full test last mein)
- [ ] 1. MIN/MAX Timestamp+Float, ORM sum/avg/min/max (patch_agg.py + tests/agg.rs, commit baaki)
- [ ] 2. Float SUM/AVG (compute_aggregates + AccumulatorSum ke Float arm; output type DataType::Float)
- [ ] 3. ORM group_count / GROUP BY helper
- [ ] 4. LIMIT $n / OFFSET $n parameter
- [ ] 5. Cursor pagination + polling helper (id > last_seen, id < before)
- [ ] 6. ORM relations / joins (post.author, user.posts)
- [ ] 7. TCP path se FK / auto-id / cascade test (network.rs, multi-client)
- [ ] 8. Purane int-encoded Timestamp/f64 ORM tables ka migration (sync_allow_type_change ya naya column)
- [ ] 9. Optional: engine-level FK/CHECK/DEFAULT (raw SQL par bhi enforce, catalog format badlega, sabse risky)
- [ ] 10. Optional: LIKE ke liye index prefix scan (abhi full scan)
- [ ] 11. Value limit ~3900 bytes: lambi post/message ke liye FileStore ya chunk helper

## Final gate (sabse last, is order mein)
- [ ] F1. cargo test --workspace --release (pehle 348+, ab zyada) 0 failed
- [ ] F2. fault-injection loop (handoff wali for c in mitrdb-storage mitrdb-txn mitrdb-server ...)
- [ ] F3. perf A/B: write_mark_real 4T aur perf_bench batch, OLD 2cf6b66 vs NEW, 12 round
- [ ] F4. clippy clean: cargo clippy --workspace --release
- [ ] F5. Docs update: README Limits (Timestamp/Float native, DEFAULT, LIKE, composite), docs/ORM.md (types, default, on_delete, unique_together, OR/IN, aggregates), docs/LIMITATIONS.md (purani lines hatao)
- [ ] F6. ROADMAP [x] + git push origin anwedb-clean

## Known limits (jaan-boojhkar)
- FK/CHECK/DEFAULT sirf Table/ORM API se, raw SQL bypass
- FK lock sirf ek process ke andar; cascade atomic nahi
- AVG integer truncate; f64 par CHECK mat lagao
- App layer ka kaam: password hashing, sessions, WebSocket real-time
