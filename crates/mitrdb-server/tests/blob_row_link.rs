use std::path::PathBuf;
use mitrdb_server::{Server, Value, val};
use mitrdb_storage::{OverflowDescriptorV2, PAGE_TYPE_OVERFLOW, Page};
const PAGE_SIZE: usize = 4096;
fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("mitrdb_a9_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&p); std::fs::create_dir_all(&p).unwrap(); p
}
fn count_overflow(db_path: &std::path::Path) -> usize {
    let bytes = std::fs::read(db_path).unwrap();
    bytes.as_chunks::<PAGE_SIZE>().0.iter().enumerate()
        .filter(|(pid, chunk)| *pid != 0 && Page::from_bytes(**chunk).map(|p| p.page_type() == PAGE_TYPE_OVERFLOW).unwrap_or(false))
        .count()
}
fn pattern(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
fn write_blob(s: &Server, payload: &Vec<u8>) -> OverflowDescriptorV2 {
    let sess = s.create_session();
    s.begin_blob_write(sess, u64::MAX).unwrap();
    for c in payload.chunks(64*1024) { s.write_blob_chunk(sess, c).unwrap(); }
    let d = s.finish_blob_write(sess).unwrap();
    s.execute_in_session(sess, "COMMIT").unwrap();
    s.close_session(sess).unwrap();
    d
}
fn to_ref(d: OverflowDescriptorV2) -> Value {
    Value::BlobRef(d.head_page_id, d.total_len, d.payload_crc32c)
}
fn to_desc(v: &Value) -> OverflowDescriptorV2 {
    if let Value::BlobRef(h, t, c) = v { OverflowDescriptorV2 { head_page_id: *h, total_len: *t, payload_crc32c: *c } } else { panic!("expected BlobRef, got {v:?}") }
}
#[test]
fn row_blob_link_insert_select_update_delete_vacuum() {
    let dir = temp_dir("link");
    let db = dir.join("t.db"); let wal = dir.join("t.wal");
    let (s, _) = Server::open(&db, &wal, 64).unwrap();
    let t = s.table("docs");
    t.sync("id int primary key, blob bytes").unwrap();
    // INSERT BlobRef
    let payload1 = pattern(300_000, 11);
    let d1 = write_blob(&s, &payload1);
    t.insert(&[("id", val(1)), ("blob", to_ref(d1))]).unwrap();
    // SELECT returns same BlobRef, chain readable
    let r = t.find().filter("id", "=", val(1)).run().unwrap();
    assert_eq!(r.rows.len(), 1);
    let got_desc = to_desc(&r.rows[0][1]);
    assert_eq!(got_desc, d1);
    let sess = s.create_session();
    s.open_blob_read(sess, got_desc).unwrap();
    let got = s.read_blob_range(sess, 0, 5000).unwrap();
    assert_eq!(got, &payload1[..5000]);
    s.close_session(sess).unwrap();
    s.checkpoint().unwrap();
    let ovf_after_insert = count_overflow(&db);
    assert!(ovf_after_insert > 0, "inserted blob should use overflow pages");
    // UPDATE to new blob — old chain must become dead
    let payload2 = pattern(100_000, 22);
    let d2 = write_blob(&s, &payload2);
    assert_ne!(d1.head_page_id, d2.head_page_id);
    let n = t.update().set("blob", to_ref(d2)).filter("id", "=", val(1)).run().unwrap();
    assert_eq!(n, 1);
    let r2 = t.find().filter("id", "=", val(1)).run().unwrap();
    assert_eq!(to_desc(&r2.rows[0][1]), d2);
    // DELETE + vacuum must free both chains (old via UPDATE dead version, new via DELETE)
    let n = t.delete().filter("id", "=", val(1)).run().unwrap();
    assert_eq!(n, 1);
    let _ = s.vacuum();
    s.checkpoint().unwrap();
    assert_eq!(count_overflow(&db), 0, "DELETE + vacuum must free all blob overflow pages");
    // reuse proof — new blob still works after free
    let payload3 = pattern(50_000, 33);
    let d3 = write_blob(&s, &payload3);
    t.insert(&[("id", val(2)), ("blob", to_ref(d3))]).unwrap();
    let r3 = t.find().filter("id", "=", val(2)).run().unwrap();
    assert_eq!(to_desc(&r3.rows[0][1]), d3);
    let _ = std::fs::remove_dir_all(dir);
}
#[test]
fn blobref_codec_roundtrip_tag9() {
    use mitrdb_exec::Tuple;
    let vals = vec![Value::Int64(1), Value::BlobRef(42, 12345, 0xdeadbeef), Value::Text("x".into())];
    let buf = Tuple::serialize(&vals);
    // tag 9 must be at expected position: count(2) + Int64(1+8) = 11 -> tag at 11
    assert_eq!(buf[11], 9, "BlobRef must serialize as tag 9");
    let back = Tuple::deserialize(&buf).unwrap();
    assert_eq!(back, vals);
}
