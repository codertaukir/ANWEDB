use mitrdb_server::Server;
use std::path::PathBuf;
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (bk, dest) = (PathBuf::from(&a[1]), PathBuf::from(&a[2]));
    println!("RD validate => {:?}", Server::validate_backup(&bk).map(|m| m.backup_id));
    let rep = Server::restore(&bk, &dest);
    println!("RD restore => {:?}", rep);
    for e in std::fs::read_dir(&dest).unwrap() { println!("RD file {:?}", e.unwrap().file_name()); }
    let s = Server::open(dest.join("t.db"), dest.join("t.wal"), 64).unwrap().0;
    let rows = s.table("bk").find().columns(&["id", "v"]).run().unwrap().rows;
    let ok = rows.len() == 1000 && rows.iter().all(|r| {
        let (i, v) = (format!("{:?}", r[0]), format!("{:?}", r[1]));
        v.contains(&format!("row{}", i.trim_start_matches("Int64(").trim_end_matches(')')))
    });
    println!("RD rows={} content_ok={}", rows.len(), ok);
    // restore into existing dir must be refused
    println!("RD restore_existing_refused={}", Server::restore(&bk, &dest).is_err());
}
