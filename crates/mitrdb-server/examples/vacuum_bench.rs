use std::env;
use std::path::Path;
use std::time::Instant;

use mitrdb_server::Server;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("test_vac.db");
    let wal_path = args.get(2).map(|s| s.as_str()).unwrap_or("test_vac.wal");
    let total_rows: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5_000);
    let delete_rows: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(2_000);
    let pool_size: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(4096);

    if Path::new(db_path).exists() || Path::new(wal_path).exists() {
        return Err("Database or WAL path already exists. Use fresh paths.".into());
    }

    println!("============================================================");
    println!("             MITRDB VACUUM PERFORMANCE BENCHMARK            ");
    println!("============================================================");
    println!(" Rows: {total_rows} | Delete: {delete_rows} | Buffer Pool: {pool_size} frames");

    let (server, _) = Server::open(db_path, wal_path, pool_size)?;
    server.start()?;

    server.execute_sql("CREATE TABLE bench_vac (id INT PRIMARY KEY, val INT);")?;

    // 1. Insert Phase
    println!("\n[Stage 1] Inserting {total_rows} rows in batch...");
    let t_insert_start = Instant::now();
    server.execute_sql("BEGIN;")?;
    for id in 1..=total_rows {
        server.execute_sql(&format!(
            "INSERT INTO bench_vac VALUES ({id}, {});",
            id * 10
        ))?;
    }
    server.execute_sql("COMMIT;")?;
    let t_insert_elapsed = t_insert_start.elapsed();
    println!(
        "  -> Completed in {:.3}s ({:.2} rows/sec)",
        t_insert_elapsed.as_secs_f64(),
        total_rows as f64 / t_insert_elapsed.as_secs_f64()
    );

    // Baseline Clean Scan
    let t_base_scan = Instant::now();
    let _ = server.execute_sql("SELECT * FROM bench_vac;")?;
    let base_scan_elapsed = t_base_scan.elapsed();
    println!(
        "  -> Baseline scan latency (clean): {:.3}ms",
        base_scan_elapsed.as_secs_f64() * 1000.0
    );

    // 2. Churn / Delete Phase (Generating dead tuples)
    println!("\n[Stage 2] Deleting {delete_rows} rows (generating dead tombstones)...");
    let t_del_start = Instant::now();
    server.execute_sql("BEGIN;")?;
    for id in 1..=delete_rows {
        server.execute_sql(&format!("DELETE FROM bench_vac WHERE id = {id};"))?;
    }
    server.execute_sql("COMMIT;")?;
    let t_del_elapsed = t_del_start.elapsed();
    println!(
        "  -> Deleted in {:.3}s ({:.2} deletes/sec)",
        t_del_elapsed.as_secs_f64(),
        delete_rows as f64 / t_del_elapsed.as_secs_f64()
    );

    // Bloated Scan
    let t_bloat_scan = Instant::now();
    let _ = server.execute_sql("SELECT * FROM bench_vac;")?;
    let bloat_scan_elapsed = t_bloat_scan.elapsed();
    println!(
        "  -> Bloated scan latency (with dead tuples): {:.3}ms (+{:.1}%)",
        bloat_scan_elapsed.as_secs_f64() * 1000.0,
        ((bloat_scan_elapsed.as_secs_f64() - base_scan_elapsed.as_secs_f64())
            / base_scan_elapsed.as_secs_f64())
            * 100.0
    );

    // 3. VACUUM Phase
    println!("\n[Stage 3] Executing server.vacuum()...");
    let m_before = server.metrics_snapshot();
    let t_vac_start = Instant::now();
    let rep = server.vacuum();
    let t_vac_elapsed = t_vac_start.elapsed();
    let m_after = server.metrics_snapshot();

    println!(
        "  -> VACUUM completed in {:.3}ms",
        t_vac_elapsed.as_secs_f64() * 1000.0
    );
    println!("  -> Versions pruned: {}", rep.versions_pruned);
    println!(
        "  -> Active MVCC keys remaining: {}",
        server.txn_manager().mvcc_key_count()
    );
    println!(
        "  -> Active MVCC versions remaining: {}",
        server.txn_manager().mvcc_version_count()
    );

    // 4. Post-Vacuum Scan Phase
    let t_post_scan = Instant::now();
    let _ = server.execute_sql("SELECT * FROM bench_vac;")?;
    let post_scan_elapsed = t_post_scan.elapsed();
    println!(
        "\n[Stage 4] Post-Vacuum scan latency: {:.3}ms",
        post_scan_elapsed.as_secs_f64() * 1000.0
    );

    println!("\n================== BENCHMARK SUMMARY ==================");
    println!(
        "  Baseline Scan (Clean):       {:.3}ms",
        base_scan_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  Bloated Scan (Dead Tuples):  {:.3}ms",
        bloat_scan_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  VACUUM Execution Time:       {:.3}ms",
        t_vac_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  Post-VACUUM Scan:            {:.3}ms",
        post_scan_elapsed.as_secs_f64() * 1000.0
    );
    println!("  Versions Cleaned:            {}", rep.versions_pruned);
    println!(
        "  Buffer Pool Hits:            {}",
        m_after.buffer_hits.saturating_sub(m_before.buffer_hits)
    );
    println!("=======================================================");

    Ok(())
}
