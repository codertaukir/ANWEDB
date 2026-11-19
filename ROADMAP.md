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
- [x] Baaki (closed): default values, foreign key, CHECK, savepoints, native Timestamp/Float, non-hint path A/B: sab ho chuka (Session 2 aur Session 3 dekho)
- [x] SAVEPOINT / ROLLBACK TO / RELEASE: `TransactionContext.savepoints`, `write_mark` + `rollback_to_mark` par based, COMMIT/ROLLBACK par khud saaf (c36e383)
- [x] Foreign key (RESTRICT): `#[col(references = "table.col")]`, `Table::add_foreign_key`, defs `mitrdb_fk` mein, catalog format untouched. Limit: sirf Table/ORM API enforce karta hai, raw SQL bypass, check+write atomic nahi (353fbac) [UPDATE Session 3: raw SQL bypass ab band, raw DML refused]
- [x] CHECK: `#[col(check = ">= 0")]`, `Table::add_check`, defs `mitrdb_check` mein, insert/update par, NULL pass, purani rows verify nahi. f64 column par mat lagao (55b1a5f)
- [x] Perf A/B #2 (write_mark_real 4T, 12 round): OLD 2cf6b66 82.1k vs NEW 83.9k ops/s (+2.2%, noise). Absolute numbers machine load se badalte hain, sirf relative dekho
- [x] Non-hint path A/B (perf_bench batch 20000 rows, 12 round): OLD 78.5k vs NEW 78.3k rows/s (-0.2%, noise)
- [x] Concurrency bugs mile aur fix hue: internal table create race (mitrdb_seq/fk/check "already exists"), FK check+write race (1500 rounds mein 21 orphans -> 0, process-wide FK lock). Limit: lock sirf ek process ke andar, raw SQL bypass (4859919 aur ye commit) [UPDATE: raw SQL bypass ab band, Session 3]
- [x] Docs: README ORM section, docs/ORM.md, docs/LIMITATIONS.md, orm quickstart example (03d6af8, 2bb3f9b)
- [x] Baaki (closed): native Timestamp/Float, DEFAULT, FK cascade done (Session 2). Engine-level FK/CHECK skipped (step 9), raw SQL DML reject se band (Session 3). Cross-process FK safety khula: Known limits

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
- [x] Delete/update guard ab OR groups ginta hai: `filter_any(...)` akela ab refuse nahi hota (pehle `or_in_between_null` fail tha), khali OR group (`in_list([])`) ab bhi filter nahi ginta, poori table safe. empty_group_guard test + or_in green (63166bf)

## Session 3: done (origin 76e068d)
- [x] F5 docs: README, docs/LIMITATIONS.md, docs/ORM.md refreshed; 3 unverified lines pinned: row limit in bytes (4019 ASCII bytes / 1339 Hindi chars, value_limit_probe2.rs), oversize error `OUT_OF_SPACE` (Table::insert), `page_after`/`page_before` on non-unique column skips rows (35 of 50, page_nonunique.rs)
- [x] F3 closed as inconclusive (noise), F1-F6 all done (a1c3b17, 309baea)
- [x] Step 10: LIKE literal-prefix index range scan (3777be7): `like_prefix_range` + Like arm in `extract_column_ranges` (query/lib.rs). Range is a superset, every caller re-applies the filter (verified in code). like_prefix.rs 14 patterns (incl. Hindi), like_explain.rs pins plans (`ab%`, `ab_c%` IndexRangeScan; `%bc`, `_bc`, unindexed column SeqScan). Probe N=20000: prefix 131 us vs full scan 76733 us (different queries, order of magnitude only)
- [x] Raw SQL bypass closed (76e068d): `guard_raw_dml` (server/table.rs) refuses raw INSERT/UPDATE/DELETE on tables with FK / CHECK / DEFAULT rules and on FK parents. Public entry points guarded: execute_sql, execute_sql_params, execute_in_session, execute_plan, execute_plan_in_session (also CLI and TCP). Table/ORM use `*_internal` calls. raw_guard.rs + tcp_fk.rs (now asserts rejection). Gate: clippy 0, 372 passed / 0 failed / 17 ignored
- [x] Guard cost + insert A/B measured (Session 4): see below. Result: noise
- [ ] Open (not checked): guard covers public Server entry points only; any internal path calling the executor directly would skip it

## Session 4: production checks (measured, real disk btrfs, not tmpfs)
- [x] Raw insert A/B, pre-guard 3777be7 vs HEAD, 12 rounds alternating, taskset 0-3, TMPDIR on /home (btrfs): batch median 54688 vs 55226 rows/s (+1.0%), spread 47.4k-59.9k vs 49.1k-58.6k; autocommit median 13045 vs 13145 us (+0.8%), spread 12.0k-15.7k vs 12.3k-14.6k. Difference is far smaller than spread = noise, no guard cost visible. Autocommit ~13 ms/stmt on real disk (fsync), vs 95 us on tmpfs
- [x] Soak v2 (examples/soak.rs): 30 cycles of kill -9, 4 threads raw INSERT/UPDATE/DELETE, intent+ack log, real disk. Every cycle lost=0 phantom=0, rows grew to 34210, stderr empty. Scope: process crash only, NOT power loss / fsync lie. (Soak v1 showed 29 failures: harness bug, ack written after SQL, kill -9 between them. Not an engine bug, replaced by v2)
- [x] Backup/restore drill (examples/backup_drill.rs, restore_drill.rs): backup of 1000 rows -> Server::restore into new dir -> rows=1000 content_ok=true; restore into an existing dir refused; backup with 1 flipped byte in t.db: validate and restore both fail with `Database file CRC mismatch` (corruption detected)
- [x] FK soak (crates/mitrdb-orm/examples/soak_fk.rs): ORM insert parent+child (FK RESTRICT) and child delete, 4 threads, 60 min of kill -9 cycles, real disk: lost=0 phantom=0 orphan=0 every cycle (process crash only)
- [ ] Open: power loss, disk full, TCP path under kill -9
- [ ] Open: backup drill was a small DB (1000 rows, no concurrent writers during backup)
- [x] Thread scaling (examples/scale_sessions.rs, one session per thread, 4 cores, 3 rounds x 5 s, median): real disk btrfs autocommit 1 row/stmt 77/80/155/275 rows/s (1/2/4/8 threads, group commit works); autocommit 100 rows/stmt 5438/7320/12126/14106; explicit txn per thread 35132/29926/22628/21765. tmpfs (no fsync): single ~33-35k flat, multi100 61k to 47k, txn 45.9k/46.1k/39.7k/40.2k. errors=0 everywhere except one tmpfs run where /tmp (4 GB) filled up (cause: disk full, not an engine bug; exact error text not captured)
- [x] Note: `execute_sql` and the Table/ORM API use one default session (one mutex): multi-threaded use of them is serialized. Real concurrency needs one session per thread (`create_session` / `execute_in_session`, what TCP does). Earlier 4-thread numbers on the default session were mutex-bound and fsync-noisy, not a regression (A/B 161b1d8 vs HEAD: no systematic gap)
- [ ] Open: why tmpfs CPU scaling is flat (no profile done); disk-full behaviour (errors seen, data integrity after full disk not checked); TCP 100-1000 connections; datasets above ~50k rows (1M/10M rows not tested)

## Baaki (is order mein karo, har step ke baad sirf cargo check; full test last mein)
- [x] 1. MIN/MAX Timestamp+Float, ORM sum/avg/min/max (69396e6)
- [x] 2. Float SUM/AVG, plain + GROUP BY (25ed178)
- [x] 3. ORM group_count / group_count_top (4db5a58)
- [x] 4. LIMIT $n / OFFSET $n: SKIPPED jaan-boojhkar (ORM limit/offset usize hain, injection nahi; raw SQL ke liye Select ka type badalna padta, risk zyada)
- [x] 5. Cursor pagination: ORM page_after / page_before (52ff9d8)
- [x] 6. ORM relations v1: belongs_to / has_many / load_many (do-query helpers, chunk 50 kyunki WHERE depth limit 128; real JOIN eager loading nahi, ek call ke queries ke beech snapshot consistent nahi) (b23e37a)
- [x] 7. TCP path test (tcp_fk.rs): raw TCP INSERT FK bypass kar deta hai (limit test se pinned), 4 client x 250 raw INSERT/UPDATE exact. Latency probe (tcp_latency.rs, 300 stmts): client nodelay OFF 41ms/stmt (Nagle+delayed-ACK, client artifact), nodelay ON 431us, in-process 95us. TCP clients ko set_nodelay(true) + length+payload ek write mein chahiye [UPDATE Session 3: ye bypass ab band, raw TCP INSERT FK table pe refused, tcp_fk.rs rejection assert karta hai]
- [x] 8. SKIPPED jaan-boojhkar: migration system nahi chahiye (design: sync(), no migration files). Purane int-encoded tables ko drop + sync() se dobara banao. Agar data bachana ho: legacy_migrate_probe.rs mein verified recipe hai (ADD COLUMN shadow, f64_dec, DROP, RENAME; ALTER TYPE timestamp seedha chalta hai)
- [x] 9. SKIPPED jaan-boojhkar (engine-level enforce, catalog format badlega). Bypass reject se band: `guard_raw_dml` (server/table.rs), Table/ORM `*_internal` calls use karta hai. raw_guard.rs + tcp_fk.rs pinned
- [x] 10. LIKE literal-prefix index range scan: `like_prefix_range` + Like arm in `extract_column_ranges` (query/lib.rs). Range is a superset, every caller re-applies the filter (verified in code: exec_index_range_scan, narrow_dml_entries via exec_update/exec_delete, IndexOrderedScan, PK path). like_prefix.rs 14 patterns (incl. Hindi) correct, like_explain.rs pins plans (`ab%`, `ab_c%` IndexRangeScan; `%bc`, `_bc`, unindexed SeqScan). Probe N=20000: prefix 131 us vs full scan 76733 us (different queries, order of magnitude only)
- [x] 11. Value limit: row ~4000 bytes total (measured, value_limit_probe.rs). ORM TextStore (put/get/delete/put_and): long text in FileStore, row keeps body_ref; 140 KB Hindi round-trip, failed insert leaves no orphan file (9c9725a). Limit: not atomic with WAL, crash orphans not swept, in-process only

## Limits and numbers (source for README + docs/LIMITATIONS.md, English)
[verified] = read in code or measured by a test. [unverified] = reasoning only, check before publishing.

### Size limits
- [verified] A row must fit in one page: PAGE_SIZE = 4096. Measured (value_limit_probe.rs): one text column 4000 bytes ok, 4050 fails; three columns 3900 bytes total ok, 4500 total fails. The limit is per ROW (all columns added up), not per value. Exact threshold (table `id int primary key` + one text column, value_limit_probe2.rs): 4019 bytes ok, 4020 fails. The three-column exact threshold is not pinned.
- [verified] Larger values are refused with an error, nothing half-written is left (big_value.rs: 8 KB, 100 KB, 1 MB, 10 MB).
- [verified] The limit counts bytes, not characters (value_limit_probe2.rs, Hindi 3 bytes per character in UTF-8): 1339 Hindi characters (4017 bytes) ok, 1340 (4020 bytes) fails. Same table as above, so about 1339 Hindi characters fit in one column.
- [verified] An oversized row is refused by `Table::insert` with `Error::OutOfSpace` (Display `OUT_OF_SPACE`), same for 4100, 5000 and 100000 bytes. The table stays usable after the failure. How the error looks over raw SQL / TCP was not checked.
- [verified] FileStore (files.rs) has no size limit: files are streamed in 1 MB chunks (CHUNK_BYTES is a buffer size, not a cap). Files are stored on disk (dir mode 0700, file mode 0600, random 32-hex names), outside the database, WAL and transactions. Saving a file and inserting its row are NOT atomic: save the file first, insert the row second, delete the file if the insert fails. Orphan files after a crash are not swept yet (only `cleanup_tmp` removes old `.tmp` files).
- [verified] FileStore is in-process only. There is no file upload or download over TCP.
- [verified] TCP: one frame is at most 1 MB by default (`max_frame_bytes`), for requests and responses.

### TCP / network
- [verified] Protocol: 4-byte big-endian length + payload. First frame `MITRDB/1`, then optional `AUTH <token>` (token + TLS are optional), then plain SQL text. Errors reply with `"status":"error"`.
- [verified] Auth failures are rate limited: 5 failures per 60 s per IP.
- [verified] Raw SQL INSERT/UPDATE/DELETE over TCP on a table with FK / CHECK / DEFAULT rules (or an FK parent) is refused (pinned by tcp_fk.rs, raw_guard.rs).
- [verified] Clients must set TCP_NODELAY and send length + payload in ONE write. A client without nodelay and with two writes measured 41163 us per statement, with nodelay 431 us, in-process execute_sql 95 us (tcp_latency.rs, 300 autocommit inserts, one connection). Small sample; whether /tmp was tmpfs was not checked.
- [verified] Multi-client raw writes are exact (4 clients x 250 INSERT + UPDATE over TCP, tcp_fk.rs).

### Query limits
- [verified] WHERE nesting depth is at most 128 (`MAX_WHERE_DEPTH`). `in_list` builds an OR chain, so `load_many` uses chunks of 50 ids.
- [verified] LIKE `%` and `_`, case-sensitive, text only, no escape. A pattern with a literal prefix (`'ab%'`, `'ab_c%'`) on an indexed column uses an index range scan on that prefix; a pattern that starts with `%` or `_`, or a column without an index, does a full scan plus filter.
- [verified] `LIMIT $n` / `OFFSET $n` are not supported in SQL (ORM limit/offset are usize values).
- [verified] Composite index: at most 4 columns. It helps unique constraints and leftmost-prefix equality only (no range or ORDER BY on a prefix).
- [verified] DDL is not allowed inside a transaction.
- [verified] Integer AVG truncates; float SUM/AVG are real f64.
- [verified] `page_after` / `page_before` on a NON-UNIQUE column skip rows (page_nonunique.rs: 50 rows, 10 distinct values x 5 rows, page size 7, cursor = last row's value: only 35 of 50 rows seen, forward and backward). The rest of the rows that share the cursor value are skipped. Cursor on the primary key (or any unique column) is safe: 50 of 50, also with a filter.

### Constraints and integrity
- [verified] FK, CHECK and DEFAULT are enforced by the Table/ORM API only. Raw SQL writes (INSERT/UPDATE/DELETE) through `execute_sql`, `execute_sql_params`, `execute_in_session`, `execute_plan*`, the CLI and TCP are REFUSED on a table that has FK, CHECK or DEFAULT rules, or that is the parent of an FK (error contains `refused`). Only the Table/ORM API may write to such tables. SELECT and DDL are not affected; tables without rules accept raw SQL as before. The guard checks INSERT/UPDATE/DELETE only.
- [verified] The FK lock works inside one process only. Cascade delete is not atomic.
- [verified] FK: RESTRICT or ON DELETE CASCADE. The parent column must be a primary key of the same type.
- [verified] CHECK: `column op literal` (int or text), NULL passes, existing rows are not verified when a check is added.
- [verified] ORM `Default` applies only when an Option field is None. Schema DEFAULT exists through `set_default` / `#[col(default = ..)]`.
- [verified] Float: NaN is rejected, -0.0 is stored as 0.0.
- [verified] Auto id is never reused (`mitrdb_seq` counter table, compare-and-swap). The `mitrdb_` table prefix is reserved (mitrdb_seq, mitrdb_fk, mitrdb_check, mitrdb_default).

### Schema changes
- [verified] `sync()` refuses a changed column type. `sync_allow_type_change` applies it through a shadow column with strict casts (first bad row aborts), refuses primary key and indexed columns, and is not safe against concurrent writers.
- [verified] Engine ALTER TYPE: int, text, bool, bytes, and int to timestamp work. int to float is refused.
- [verified] ORM tables created before native TIMESTAMP / FLOAT (commit aa65c8a) stored them in an `int` column and now read wrong. There is no migration system by design: drop the table and `sync()` again. If data must be kept, legacy_migrate_probe.rs has a tested recipe (add shadow column, decode with the old formula, drop, rename).

### Relations
- [verified] `belongs_to`, `has_many`, `load_many` are plain queries (no JOIN eager loading). Separate calls are not one snapshot unless wrapped in an explicit transaction.

### Not the database's job
- Password hashing, sessions and WebSocket real-time belong to the app layer.

### Performance numbers (relative only, machine load changes absolutes)
- [verified] Batch insert about 50k rows/s (explicit txn, tmpfs, 4 cores). Autocommit insert in-process 95 us per statement (small sample).
- [verified] Perf A/B after native types / composite / DEFAULT / cascade (OLD 2cf6b66 vs 63166bf, 12 rounds, alternating order): perf_bench batch median 75668 vs 75644 rows/s (-0.03%), flat. write_mark_real 4T is inconclusive (median -12.6%, but spread 54k-444k ops/s is larger than the difference).

## Final gate (sabse last, is order mein)
- [x] F1. cargo test --workspace --release --no-fail-fast: 372 passed, 0 failed, 17 ignored (measured, gate before 76e068d; pehle 366 @63166bf, 368 @a1c3b17, 371 @3777be7)
- [x] F2. fault-injection loop (mitrdb-storage, mitrdb-txn, mitrdb-server, har test file alag, timeout 120s): 0 failures (measured)
- [x] F3. DONE: inconclusive (noise), regression nahi maana. perf A/B: write_mark_real 4T aur perf_bench batch, OLD 2cf6b66 vs NEW, 12 round, alternate order, taskset 0-3, sha256 alag (63166bf)
  - perf_bench batch (measured): median OLD 75668 vs NEW 75644 rows/s (-0.03%), spread 71.5k-76.2k. Flat.
  - write_mark_real 4T (measured): median OLD 148763 vs NEW 129950 ops/s (-12.6%), par spread OLD 79k-201k, NEW 54k-444k. Inconclusive (noise), regression nahi maana. Andaza: WAL lock contention se variance. Tight number ke liye T=1/T=2 + 30 round baaki.
- [x] F4. clippy clean: cargo clippy --workspace --release: 7 warnings (6 needless_borrow in exec, 1 bool_comparison in query) fixed, now 0 warnings (measured)
- [x] F5. DONE (docs updated, 3 unverified lines pinned by value_limit_probe2.rs + page_nonunique.rs; old note: copy the 'Limits and numbers' section above into README + docs/LIMITATIONS.md first; drop the [verified]/[unverified] tags only after checking the unverified lines) (LIMITATIONS.md stale lines 7, 12, 23: DEFAULT, cascade, mitrdb_default reserved) Docs update: README Limits (Timestamp/Float native, DEFAULT, LIKE, composite), docs/ORM.md (types, default, on_delete, unique_together, OR/IN, aggregates), docs/LIMITATIONS.md (purani lines hatao)
- [x] F6. ROADMAP [x] + git push origin anwedb-clean

## Known limits (jaan-boojhkar)
- FK/CHECK/DEFAULT sirf Table/ORM API se; raw SQL DML un tables pe refused (bypass band)
- FK lock sirf ek process ke andar; cascade atomic nahi
- AVG integer truncate; f64 par CHECK mat lagao
- App layer ka kaam: password hashing, sessions, WebSocket real-time
- Raw SQL guard: sirf INSERT/UPDATE/DELETE check hota hai (SELECT/DDL nahi); lookup cost measure nahi hua
- Step 9 (engine-level FK/CHECK/DEFAULT) skipped: raw SQL enforce nahi hota, sirf reject

## Session 5: default-session concurrency
- [x] Default-session autocommit lock-free (`run_default_plan`, lib.rs): plain autocommit statement ab default_session mutex ke bina local txn slot pe chalta hai; BEGIN/COMMIT/ROLLBACK/SAVEPOINT/Checkpoint aur explicit txn purane path pe. default_session_concurrency 3 tests. Real disk btrfs, 4 cores, median: multi100 1/2/4/8 threads 5460/5400/5438/5171 -> 5511/7288/11940/14005 rows/s; single 79/74/77/74 -> 77/80/138/296 rows/s. Single-thread raw insert A/B 8 rounds alternating: batch 36160 vs 35277 rows/s, autocommit 12944 vs 13051 us = noise.
- [x] insert_auto: WriteConflict ab CAS mismatch ki tarah retry hota hai (1000 attempts, yield + capped sleep backoff). auto_concurrent 30x loop: pass=30 fail=0. Claimed id burn ho sakti hai (gaps possible, reuse nahi).
- [x] CI: clippy approx_constant (legacy_migrate_probe), collapsible_if (2 examples), deny.toml Unicode-3.0 allow. Gate: 376 passed, 0 failed, 17 ignored.
- [ ] Open: insert_auto explicit txn ke andar WriteConflict pe retry nahi kar sakta (txn poisoned), test nahi hua.
- [ ] Known limit: default session par ek thread ka BEGIN aur doosre thread ka concurrent autocommit statement alag txn slot pe chal sakte hain. Multi-thread explicit txn ke liye create_session use karo.

## Session 6: auto-id correctness + perf findings (2026-11-18)
- [x] Probe (auto_txn_probe.rs) ne bug pakda: explicit txn ROLLBACK pe auto id reuse hoti thi (counter update outer txn ke andar tha; probe: a=1, b=2, rollback ke baad d=2). Autocommit mein reuse nahi hota tha, isliye auto_reuse pass tha
- [x] Fix: `ForceAutocommit` (thread-local guard, lib.rs `run_default_plan`): `insert_auto` ka seq counter read/claim outer txn se alag autocommit mein chalta hai. Row insert aur `max_pk` read outer txn mein hi rehte hain. Ab rollback pe gap banta hai, reuse nahi. auto_txn_rollback (assert d > b), auto_reuse, auto_concurrent: pass (ek-ek run). Local, abhi COMMIT NAHI hua
- [x] Disk floor measured: `dd oflag=dsync` 300 x 4 KB = 3.66 s = ~12.2 ms/write (btrfs /home). Autocommit 13 ms/stmt isi floor ke paas hai, engine ka overhead nahi. WAL commit path pehle se `sync_data` use karta hai (sync_all sirf recovery/backup/checkpoint mein)
- [x] insert_auto thread scaling (auto_scale_probe.rs, tmpfs, 500 inserts/thread, ek run): NEW 1/2/4T = 605/1779/7536 us per insert; OLD 41e53a0 = 597/1593/4028 us. Contention pehle se maujood tha (3 alag statements: counter read, max_pk, CAS update), mutex hatane se naya nahi bana. NEW 4T ka 1.9x gap ek run hai, noise ya regression pakka nahi. Backoff badalne se farak nahi (21.4 s vs 20.4 s), wapas liya
- [ ] Insert_auto block reservation (ek claim mein 32 ids, per-table cache): 4T contention kam karne ke liye. Design change, alag commit, A/B ke saath. Tradeoff: crash/restart pe gap 32 tak, "kabhi reuse nahi" guarantee wahi
- [ ] Benchmark A/B: HEAD bace4d0 vs working tree (ForceAutocommit ka thread-local read), 12 round alternating, tmpfs aur real disk. auto_scale_probe 4T 5 baar dono pe
- [x] Last batch: clippy --all-targets, auto_concurrent 30x, full gate, commit (auto_txn_rollback.rs + auto_scale_probe.rs `#[ignore]` ke saath), push, CI
- [x] docs/LIMITATIONS.md: (1) default-session BEGIN vs concurrent autocommit statement, (2) auto id rollback pe gap, reuse nahi, (3) hot counter pe multi-thread contention (1T 0.6 ms, 4T 4-7 ms)
- [ ] README: roadmap stale (Prepared statements / `$n` binding abhi unchecked hai), benchmark mein tmpfs (~50k) aur real disk btrfs (~36k rows/s) alag likhna

### Open (production checks, kuch bhi measure nahi hua)
- [ ] Disk full ke baad data integrity (sirf errors dekhe, integrity check nahi)
- [ ] TCP path under kill -9
- [ ] Power loss / fsync lie (sirf process crash tested)
- [ ] 1M / 10M rows (50k ke upar test nahi)
- [ ] Backup drill bada DB + concurrent writers ke saath
- [ ] guard_raw_dml sirf public entry points cover karta hai, internal path skip ho sakta hai
- [ ] insert_auto explicit txn ke andar WriteConflict pe retry: untested (txn poisoned ho sakta hai)
- [ ] tmpfs CPU scaling flat kyun, perf record nahi hua
- [ ] Delete 6 KB WAL per row; kam distinct values wale index pe 3x slow insert
- [ ] 2 unused dependency warnings (mitrdb-catalog -> mitrdb-txn, mitrdb-observe -> mitrdb-common)
- [x] CI debug build pe auto_concurrent fail (LockTimeout; local debug 2/5 aur 5/15 fail, release 30x ne cover nahi kiya). LOCK_TIMEOUT retry kaafi nahi tha (`too many conflicts`). Fix: insert_auto per-table in-process mutex (0854a4f). Release auto_scale_probe 1/2/4T = 759/1018/2347 us (pehle 605/1779/7536, ek run). CI se verify: green (0854a4f)
- [x] insert_auto max(pk) full scan hataya (8357ae9): scan sirf pehli claim ya duplicate pk ke baad. auto_growth_probe (release, tmpfs, ek run): 0 rows 1726 us -> 96 us, 12k rows 36448 us -> 101 us, 18k rows 105 us (flat). Pehle O(n) per insert tha, bulk auto-id insert O(n^2)
- [ ] insert_auto explicit txn mein mutex + row lock stall (untested)
- [ ] Benchmark A/B bace4d0 vs HEAD (ForceAutocommit thread-local), ab bhi baaki
- [ ] insert_auto per-table mutex (0854a4f) ab shayad zaroori nahi (scan hat gaya). Real disk pe group commit rokta hai. Hatane se pehle auto_concurrent debug build (CI) pe check karna
- [ ] Debug auto_concurrent ab bhi ~20 s: scan hatne ke baad dobara time dekhna

## Session 7: speed profiling (2026-11-19)
- [x] Per-phase profile (perf_bench batch 20000 rows, tmpfs, ek run, phases overlap): PlanExecution 469 ms, InsertExecution 352, TxnPut 156, BtreeMutation 79, TxnCommit 48, WalAppend 26, WalFlush 11, B+Tree splits ~40. Lock: LockMutexWait 75 (60000 calls = 3 per row), LockBookkeeping 60 (max 12.8 ms spike), LockAcquire 35. MVCC/Txn mutex wait ~2 ms
- [ ] Lock manager per-row cost kam karna (3 mutex per row, bookkeeping spike), phir A/B 12 round alternating
- [x] read_key_visible_value: 64 txn-shard lock ab sirf tab jab version chain khali na ho (logically sahi, lock order same). raw_ins_probe A/B 6 round: HEAD 40548 vs PATCH 42677 rows/s (+5.2%), lekin PATCH spread 35.3k-47.9k, HEAD ke range ke andar = inconclusive, speed-up claim nahi. mitrdb-txn tests pass
- [ ] Insert ka asli bottleneck abhi pakka nahi: perf profile mein scans mile hue the. Scans band karke dobara profile lena
- [x] Perf try (revert kiya): insert ke duplicate-PK check se Shared lock hataya (get_already_locked). raw_ins_probe A/B 6 round: HEAD 51762 vs PATCH 50598 rows/s (-2.3%), no win, revert
- [ ] Insert perf profile (perf, 5701 samples): acquire_batch_with_rollback 15.6%, memcmp 10.2%, SipHash ~6.5%, malloc/free ~12%. Kaaran (code se): acquire_exclusive_keys slow path (global mutex + 64-shard drain), active_slow_locks sirf line 692 par reset. Fix idea: acquire_exclusive_keys ko sharded fast path, ya FxHash. Asar measure nahi hua
- [x] Disk-full drill (examples/diskfull_drill.rs, 8 MB tmpfs, ek run): fill 9654 acked INSERT, phir 50 errors `IO_ERROR: No space left on device` (clean, no panic). Restart ke baad (disk full aur space free karke dono): rows=9654 acked=9654 lost=0 phantom=0 dup=0 bad_payload=0, space free hone ke baad insert OK. Scope: process crash (exit bina Drop), power loss nahi, chhota DB. (Pehla verify galat tha: id parse bug in drill, DB nahi)
- [ ] Disk-full: DDL/checkpoint/backup ke dauran full disk, aur bade DB pe, test nahi hua
- [x] 1M rows drill (examples/million_drill.rs, real disk btrfs, ek run, 1000 rows/INSERT autocommit, 3 cols ~50 B/row, DB 136 MB): insert 63.2 s = 15.8k rows/s, flat (13.7k-17.4k per 100k). Reopen 17.45 s. Full scan 1M rows 3.89 s ok. Point lookup 297 us avg (1000/1000 ok). CREATE INDEX 20 s, index lookup 0.256 s. Peak RSS 960 MB. No panic/error
- [ ] Reopen 17.45 s pe 136 MB DB: shak WAL recovery (checkpoint ke baad reopen time measure nahi hua)
- [ ] Peak memory ~960 B/row (scan poora memory mein?): 10M rows test nahi hua
- [x] Reopen fix measured (6f26e22): million_drill 1M rows, real disk btrfs. Pehle reopen 17.45 s (WAL 294 MB). Ab: reopen 1.49 s / second 1.38 s (WAL 28 MB before drop). Insert 3 runs 15.8k / 21.7k / 28.3k rows/s (btrfs spread bada) = regression nahi, speed-up claim nahi. Ek beech ka run 14.0 s reopen aaya (WAL size naapa nahi, kaaran pakka nahi)
- [ ] Reopen 14 s outlier dobara aaye to wal_mb_before_drop ke saath dekhna
