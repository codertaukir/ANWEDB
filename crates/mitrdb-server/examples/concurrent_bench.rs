//! Concurrent writer/reader benchmark for MITRDB.
//!
//! Use fresh DB/WAL paths. Each worker uses an independent SQL session.
use std::env;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use mitrdb_server::Server;

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 || args.len() > 5 {
        return Err("Usage: cargo run --release -p mitrdb-server --example concurrent_bench -- <fresh-db-path> <fresh-wal-path> [rows=1000] [writers=4] [readers=2]".into());
    }
    let db_path = &args[0];
    let wal_path = &args[1];
    let rows: usize = args
        .get(2)
        .map_or(Ok(1_000), |v| v.parse())
        .map_err(|e| format!("invalid rows: {e}"))?;
    let writers: usize = args
        .get(3)
        .map_or(Ok(4), |v| v.parse())
        .map_err(|e| format!("invalid writers: {e}"))?;
    let readers: usize = args
        .get(4)
        .map_or(Ok(2), |v| v.parse())
        .map_err(|e| format!("invalid readers: {e}"))?;
    if rows == 0 || writers == 0 {
        return Err("rows and writers must be greater than zero; readers may be zero".into());
    }
    if Path::new(db_path).exists() || Path::new(wal_path).exists() {
        return Err("Refusing to use an existing DB/WAL path. Choose fresh paths so existing data is untouched.".into());
    }

    let (server, _recovery) =
        Server::open(db_path, wal_path, 256).map_err(|e| format!("open database: {e:?}"))?;
    server
        .execute_sql("CREATE TABLE bench (id INT PRIMARY KEY, value INT);")
        .map_err(|e| format!("create table: {e:?}"))?;
    let server = Arc::new(server);
    let worker_count = writers + readers;
    let barrier = Arc::new(Barrier::new(worker_count));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(worker_count);

    for worker in 0..writers {
        let server = Arc::clone(&server);
        let barrier = Arc::clone(&barrier);
        let session = server.create_session();
        handles.push(thread::spawn(move || -> Result<usize, String> {
            barrier.wait();
            let mut inserted = 0;
            // Disjoint, deterministic primary keys prevent benchmark-induced key conflicts.
            for id in (worker + 1..=rows).step_by(writers) {
                let sql = format!(
                    "INSERT INTO bench VALUES ({id}, {});",
                    id.saturating_mul(10)
                );
                server
                    .execute_in_session(session, &sql)
                    .map_err(|e| format!("writer {worker}, row {id}: {e:?}"))?;
                inserted += 1;
            }
            server
                .close_session(session)
                .map_err(|e| format!("close writer session {worker}: {e:?}"))?;
            Ok(inserted)
        }));
    }

    for worker in 0..readers {
        let server = Arc::clone(&server);
        let barrier = Arc::clone(&barrier);
        let session = server.create_session();
        handles.push(thread::spawn(move || -> Result<usize, String> {
            barrier.wait();
            let mut scans = 0;
            // Readers overlap the write phase. Empty/partial results are expected during insertion.
            for _ in 0..10 {
                server
                    .execute_in_session(session, "SELECT * FROM bench;")
                    .map_err(|e| format!("reader {worker}: {e:?}"))?;
                scans += 1;
            }
            server
                .close_session(session)
                .map_err(|e| format!("close reader session {worker}: {e:?}"))?;
            Ok(scans)
        }));
    }

    let mut inserted = 0usize;
    let mut scans = 0usize;
    for (index, handle) in handles.into_iter().enumerate() {
        let count = handle
            .join()
            .map_err(|_| format!("worker {index} panicked"))??;
        if index < writers {
            inserted += count;
        } else {
            scans += count;
        }
    }
    let elapsed = start.elapsed();
    // Final independent read verifies the total committed row count without timing assumptions.
    let verify_session = server.create_session();
    let verification = server
        .execute_in_session(verify_session, "SELECT * FROM bench;")
        .map_err(|e| format!("verification scan: {e:?}"))?;
    server
        .close_session(verify_session)
        .map_err(|e| format!("close verification session: {e:?}"))?;

    println!("MITRDB concurrent benchmark");
    println!("  target rows: {rows}; committed writer operations: {inserted}");
    println!("  writer threads: {writers}; reader threads: {readers}; reader scans: {scans}");
    println!(
        "  elapsed (concurrent phase): {:.3}s",
        elapsed.as_secs_f64()
    );
    println!(
        "  writer throughput: {:.2} rows/sec",
        inserted as f64 / elapsed.as_secs_f64()
    );
    let verified_rows = match &verification {
        mitrdb_exec::ExecutionResult::Rows { rows, .. } => rows,
        other => {
            return Err(format!(
                "verification query returned unexpected result: {other:?}"
            ));
        }
    };
    if inserted != rows || verified_rows.len() != rows {
        return Err(format!(
            "verification failed: expected {rows} committed rows, writers reported {inserted}, query returned {}",
            verified_rows.len()
        ));
    }
    println!(
        "  final verification: PASS ({}/{} rows present)",
        verified_rows.len(),
        rows
    );
    println!(
        "  note: readers=0 disables reader threads; timing includes overlapping reader scans when enabled. Run several times on identical hardware/settings."
    );
    println!("DB path: {db_path}; WAL path: {wal_path}");
    Ok(())
}
