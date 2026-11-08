//! Probe: uncontended cost of TxnManager::write_mark (the per-statement mark taken in explicit txns).
//! Use fresh DB/WAL paths.
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use mitrdb_server::Server;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("Usage: write_mark_probe <fresh-db-path> <fresh-wal-path>".into());
    }
    if Path::new(&args[0]).exists() || Path::new(&args[1]).exists() {
        return Err("Refusing to use an existing DB/WAL path".into());
    }
    let (server, _) = Server::open(&args[0], &args[1], 256).map_err(|e| format!("open: {e:?}"))?;
    let tm = server.txn_manager();
    let tid = tm.begin().map_err(|e| format!("begin: {e:?}"))?;
    const M: u64 = 1_000_000;
    for _ in 0..100_000 {
        black_box(tm.write_mark(black_box(tid)).map_err(|e| format!("mark: {e:?}"))?);
    }
    for round in 0..5 {
        let t = Instant::now();
        for _ in 0..M {
            black_box(tm.write_mark(black_box(tid)).map_err(|e| format!("mark: {e:?}"))?);
        }
        println!(
            "round {round}: {:.1} ns/call",
            t.elapsed().as_nanos() as f64 / M as f64
        );
    }
    let _ = tm.abort(tid);
    Ok(())
}
