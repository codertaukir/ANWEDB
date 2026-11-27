# MITRDB pilot rules (Mittr)

Status: PILOT only. Experimental, single-node. PostgreSQL / YugabyteDB ka replacement nahi.
CI: run #180 (commit 7eca40d, branch main): success (audit, test, deny). Baad ke commits docs-only hain, unka CI alag dekho.

Test hua (sab ek VM / ek disk config: QEMU, virsh destroy, directsync, ext4, 4 threads; har cut ek run):
- Power-loss INSERT-only: 10 cuts. I/U/D soak: 4 cuts + ~100 min soak 1 cut (WAL 5 segments, 65141 ids). Multi-table txn + counter: 4 cuts. DDL loop (single thread): 4 cuts. Sab mein lost=0 phantom=0 (txn: torn=0; DDL: bad=0).
- Host, process kill -9 (power-loss nahi): backup + concurrent I/U/D writers (har backup restore+verify, bad=0); TCP+TLS 100 aur 200 clients (loopback).
Ye test evidence hai, guarantee nahi.

## 1. Sirf non-critical data
- Mittr ka wo data jo kho jaye to chalega (test users, logs, cache-type). Messages/keys ka akela source mat banao.
- Source of truth ke liye purana DB (Postgres/Yugabyte) chalta rahe, MITRDB shadow/pilot rahe.

## 2. Regular backup + restore verify
Backup server ke andar hi lena hai (process bahar se DB nahi khol sakta, dir lock hai):

    let m = server.backup(format!("{root}/bk/{ts}"))?;      // ts = unix secs, har ghante
    Server::validate_backup(format!("{root}/bk/{ts}"))?;

Restore check (hafte mein ek baar, alag dir mein; destination pehle se EXIST nahi hona chahiye):

    Server::restore(format!("{root}/bk/{ts}"), format!("{root}/rst/{ts}"))?;
    // phir restored .db/.wal Server::open se kholo, row counts dekho, dir delete karo

- Aakhri 24 backups rakho, purane delete karo.
- Backup ek hi process se, ek hi time par (backup_lock hai).

## 3. Bade table par COUNT(*) nahi
- 10M rows par COUNT(*) ~100 s laga (measure hua). 1M par ~4 s full scan.
- Chhoti counter table rakho, row insert/delete ke saath usi explicit txn (BEGIN..COMMIT) mein update karo.
- Counter `UPDATE c SET n = n + 1 WHERE k = 1`: autocommit, ROLLBACK, COMMIT, reopen test pass (counter_update.rs, ek run).
- Batch INSERT (VALUES ...) ya explicit txn use karo, ek-ek row autocommit nahi.

## 4. Ek DB dir = ek process
- Server::open <db>.lock par exclusive lock leta hai. Dusra open "already open" error dega.
- Mittr server mein ek hi MITRDB instance, ek hi process. Rolling deploy mein purana band hone ke baad naya start karo.
- Lock network filesystem (NFS) par guaranteed nahi, local disk hi use karo.

## Abhi untested
- Doosra filesystem / hardware / disk config; power-loss ke dauran backup
- Concurrent DDL, DDL + DML saath, ALTER DROP/RENAME COLUMN
- TLS: default connection limit (128) par 200 clients, slow clients, real network (drill sirf loopback par, --max-connections 256)
- ~100 min se lamba soak
- insert_auto explicit txn ke andar WriteConflict retry (sirf basic ROLLBACK/COMMIT test hua)
