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
- [x] Leaf split se pehle poora split chain padha jaata hai (preflight_split_reads): split_internal child-read failure par ab koi WAL record/page write nahi, keys gayab nahi. btree_split_atomicity2; negative control FAIL bina fix. Insert A/B: noise (new -2.9%, identical-binary control -3.0%)

## P0: Production se pehle
- [ ] Btree multi-write atomicity ki baaki windows: new_page fail (split ke beech), write_node step 3 (LSN stamp), free_btree_page, delete merge/borrow/rebalance_internal paths, root split ka read_node(left/right), transient I/O jab pages pool se evict ho jayein. split_leaf aur split_internal ke read-failure windows fixed (persistent unreadable page). General fix: error par prev_lsn ctx tak + op-level undo. Chhota facet: sirf failed write ho to has_writes false, commit bina fsync
- [ ] Explicit-txn bench: perf_bench autocommit hai, `write_mark` ka mutex measure nahi hua (earlier.contains branch aur LockTimeout ke baad statement rollback ke tests ho chuke, stmt_atomicity.rs)
- [x] Poisoned commit ka WalCorrupted label theek: group-commit leader ka flush error ab apna variant (Io) rakhta hai, WAL_CORRUPTED nahi dikhta. poisoned_commit_label; negative control: HEAD par test FAIL (815d4e9). Error branch, A/B nahi
- [ ] Poisoned commit ke baaki tukde: retry window worst case ~310ms (LockTimeout 50ms se zyada); client ko Err milta hai par commit baad mein ho sakta hai (in-doubt, by design); sweep sirf agle statement par chalta hai (idle server par nahi); explicit COMMIT par session slot take() ho chuka hota hai (tid kho jata hai, sweep use resolve karta hai)
- [x] Auto-checkpoint error ab metrics mein: `checkpoint_errors` counter (failed auto-checkpoint par badhta hai, statement fail nahi hota). ckpt_error_metric: metadata tmp path block karke 8 errors gine gaye, statements chalte rahe, heal ke baad checkpoint ok; negative control (field bina increment) FAIL (5564c46). Auto-checkpoint concurrent test (4 sessions) pehle se pass
- [ ] Auto-checkpoint: floor backoff ki checkpoint-ginti measure nahi hui; auto-checkpoint error par log nahi (library mein koi logging macro nahi, sirf counter)
- [x] Bulk delete memory: WAL arena beech mein flush (har 64 rows par, pending > 16MB). N=20000 VmHWM delta 259MB -> 59MB (measured); crash test wal_pressure_flush, A/B delete probe noise ke andar (9a5a857)
- [x] Bulk UPDATE memory: WAL arena beech mein flush (exec_update put loop, har 64 rows par, pending > 16MB). `set name` N=20000 VmHWM delta 143MB -> 72MB (measured); bulk_update_memory FAIL on HEAD (143MB), wal_pressure_update_flush crash test; A/B update probe noise ke andaza mein (set name 30/34 vs 34us; set g +9.6% phir +3.8%, same-binary control +3.8%, hook g mein chalta hi nahi) (786939f)
- [ ] Bulk delete/update mein arena ke baad bhi memory N ke saath badhti hai: delete N=20000 delta 59MB (arena <= 16MB, baaki ~40MB andaza), update `set name` 35/50/72MB (N=5k/10k/20k), `set g` 10/20/41MB (WAL sirf 1MB, ~2KB/row, hook trigger nahi hota). Kaun si structure (entries, to_delete, matched_rows, planned_updates, lock_targets, MVCC chains, undo write_set) measure nahi hua. insert_many par WAL pressure hook abhi nahi (andaza, probe-first)

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
- [ ] 14 `.bak*` files (btree/buffer_pool/disk/recovery/wal) source dir se hatao (git-ignored, par grep ko gandha karti hain)

## Rules (short)
Perf girni nahi chahiye (A/B 3 rounds, 3% se kam = noise). Hot path (`exec_insert`, `allocate_page`) par A/B zaroori. Har step ek commit, push maintainer ke kehne par.
