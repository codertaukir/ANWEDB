use std::path::PathBuf;
use mitrdb_server::Server;

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("mitrdb_a8_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&p); std::fs::create_dir_all(&p).unwrap(); p
}
// offset-independent pattern — chunk ko bina poora blob banaye regenerate kar sakte hain
fn chunk_at(offset: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| {
        let o = offset + i as u64;
        ((o.wrapping_mul(2654435761) >> 24) as u8) ^ ((o >> 8) as u8)
    }).collect()
}
const TOTAL: u64 = 5_000_000;
const CHUNK: usize = 64 * 1024;

#[test]
fn backup_restore_multimb_blob_chain_streaming() {
    let dir = temp_dir("stream");
    let db = dir.join("t.db"); let wal = dir.join("t.wal");
    let backup_dir = dir.join("backup"); let restored_dir = dir.join("restored");

    // 1) Write 5MB blob in 64KB chunks — never hold full blob
    let desc = {
        let (s, _) = Server::open(&db, &wal, 64).unwrap();
        let sess = s.create_session();
        s.begin_blob_write(sess, u64::MAX).unwrap();
        let mut off = 0u64;
        while off < TOTAL {
            let n = CHUNK.min((TOTAL - off) as usize);
            s.write_blob_chunk(sess, &chunk_at(off, n)).unwrap();
            off += n as u64;
        }
        let d = s.finish_blob_write(sess).unwrap();
        assert_eq!(d.total_len, TOTAL);
        s.execute_in_session(sess, "COMMIT").unwrap();
        s.close_session(sess).unwrap();
        // 2) Backup + validate while server still open (online backup)
        let manifest = s.backup(&backup_dir).unwrap();
        let validated = Server::validate_backup(&backup_dir).unwrap();
        assert_eq!(validated, manifest);
        d
    }; // server dropped here — no clean-shutdown assumption

    // 3) Restore to fresh dir
    let rep = Server::restore(&backup_dir, &restored_dir).unwrap();
    let _ = rep;

    // 4) Reopen restored DB, validate chain streaming — 64KB at a time
    let (rs, _) = Server::open(restored_dir.join("t.db"), restored_dir.join("t.wal"), 64).unwrap();
    let sess = rs.create_session();
    rs.open_blob_read(sess, desc).unwrap();
    let mut off = 0u64;
    let mut checked: u64 = 0;
    while off < TOTAL {
        let n = CHUNK.min((TOTAL - off) as usize);
        let got = rs.read_blob_range(sess, off, n).unwrap();
        assert_eq!(got.len(), n, "short read at offset {off}");
        assert_eq!(got, chunk_at(off, n), "chain corrupted at offset {off} after restore");
        checked += n as u64;
        off += n as u64;
    }
    assert_eq!(checked, TOTAL);
    // boundary: last byte + one-past-end
    assert_eq!(rs.read_blob_range(sess, TOTAL - 1, 1).unwrap(), chunk_at(TOTAL - 1, 1));
    assert!(rs.read_blob_range(sess, TOTAL, 1).unwrap().is_empty());
    rs.close_session(sess).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
