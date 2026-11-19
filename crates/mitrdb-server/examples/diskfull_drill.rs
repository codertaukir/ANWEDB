use mitrdb_server::Server;
use std::collections::HashSet;
use std::io::Write;

fn open(dir: &str) -> Server {
    Server::open(format!("{dir}/t.db"), format!("{dir}/t.wal"), 256).unwrap().0
}
fn pid(s: &str) -> i64 {
    let a = s.find('(').unwrap() + 1;
    let b = s.rfind(')').unwrap();
    s[a..b].trim().parse().unwrap()
}
fn load(path: &str) -> HashSet<i64> {
    std::fs::read_to_string(path).unwrap_or_default().lines().filter_map(|l| l.parse().ok()).collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, dir, log) = (a[1].as_str(), a[2].as_str(), a[3].as_str());
    if mode == "fill" {
        let s = open(dir);
        s.execute_sql("CREATE TABLE IF NOT EXISTS d (id INT PRIMARY KEY, p TEXT)").unwrap();
        let mut intent = std::fs::File::create(format!("{log}.intent")).unwrap();
        let mut ack = std::fs::File::create(format!("{log}.ack")).unwrap();
        let (mut id, mut errs, mut first_err, mut acked) = (0i64, 0, String::new(), 0u64);
        while errs < 50 && id < 5_000_000 {
            writeln!(intent, "{id}").unwrap();
            match s.execute_sql(&format!("INSERT INTO d VALUES ({id}, 'payload-{id}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')")) {
                Ok(_) => { writeln!(ack, "{id}").unwrap(); acked += 1; }
                Err(e) => { if errs == 0 { first_err = e.to_string(); } errs += 1; }
            }
            id += 1;
        }
        println!("FILL acked={acked} attempted={id} errors={errs} first_err={first_err}");
        // exit without dropping Server (crash-like)
        std::process::exit(0);
    } else {
        let s = open(dir);
        let acked = load(&format!("{log}.ack"));
        let attempted = load(&format!("{log}.intent"));
        let rows = s.table("d").find().columns(&["id", "p"]).run().unwrap().rows;
        let mut present = HashSet::new();
        let mut bad_payload = 0;
        for r in &rows {
            let id = pid(&format!("{:?}", r[0]));
            if id >= 0 && !format!("{:?}", r[1]).contains(&format!("payload-{id}-")) { bad_payload += 1; }
            present.insert(id);
        }
        let lost = acked.difference(&present).count();
        let phantom = present.iter().filter(|i| **i >= 0 && !attempted.contains(i)).count();
        let dup = rows.len() - present.len();
        let uid = -1 - (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() % 1_000_000_000) as i64;
        let ok_after = s.execute_sql(&format!("INSERT INTO d VALUES ({uid}, 'payload-{uid}-after')")).is_ok();
        println!("VERIFY rows={} acked={} lost={lost} phantom={phantom} dup={dup} bad_payload={bad_payload} insert_after_ok={ok_after}", rows.len(), acked.len());
    }
}
