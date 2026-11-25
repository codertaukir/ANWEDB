# MITRDB pilot rules (Mittr)

Status: PILOT only. Production-ready nahi. Scope: ek VM, ek disk config (ext4, directsync),
4 threads, ek table; INSERT-only 10 power-cuts, I/U/D 4 power-cuts, backup+kill -9 kuch runs. CI status confirm nahi.

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
TCP+TLS: 100 aur 200 clients ek-ek run pass (loopback, kill -9, max-connections 256); default limit 128 par 200 clients, slow clients, real network untested. DDL/multi-table txn ke saath power-cut, doosri disk/filesystem,
(insert_auto explicit txn ke andar: ek test pass; multi-table txn+counter power-loss: 4 cuts pass), lamba soak (ghanton).
