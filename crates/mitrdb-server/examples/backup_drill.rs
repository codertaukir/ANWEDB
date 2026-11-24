use mitrdb_server::Server;
use std::path::PathBuf;
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (dir, bk) = (PathBuf::from(&a[1]), PathBuf::from(&a[2]));
    std::fs::create_dir_all(&dir).unwrap();
    let s = Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0;
    s.execute_sql("CREATE TABLE bk (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..1000 {
        s.execute_sql(&format!("INSERT INTO bk VALUES ({i}, 'row{i}')"))
            .unwrap();
    }
    println!("BK backup => {:?}", s.backup(&bk));
}
