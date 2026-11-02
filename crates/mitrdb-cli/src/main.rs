use std::net::SocketAddr;
use std::sync::Arc;

use mitrdb_server::{Server, TcpServerConfig};

use zeroize::Zeroizing;
fn split_statements(script: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    let mut brace_depth = 0usize;
    let mut bracket_depth = 0usize;

    for ch in script.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' && (in_single || in_double) {
            current.push(ch);
            escaped = true;
            continue;
        }

        if ch == '\'' && !in_double {
            in_single = !in_single;
            current.push(ch);
            continue;
        }

        if ch == '"' && !in_single {
            in_double = !in_double;
            current.push(ch);
            continue;
        }

        if !in_single && !in_double {
            match ch {
                '{' => {
                    brace_depth += 1;
                    current.push(ch);
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                    current.push(ch);
                }
                '[' => {
                    bracket_depth += 1;
                    current.push(ch);
                }
                ']' => {
                    bracket_depth = bracket_depth.saturating_sub(1);
                    current.push(ch);
                }
                ';' if brace_depth == 0 && bracket_depth == 0 => {
                    let trimmed = current.trim();
                    if !trimmed.is_empty() {
                        stmts.push(trimmed.to_string());
                    }
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        } else {
            current.push(ch);
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        stmts.push(trimmed.to_string());
    }

    stmts
}

fn print_usage() {
    println!(
        "MITRDB CLI\n\nUsage:\n  mitrdb-cli --db <path> --wal <path> [--sql <script>]\n  MITRDB_AUTH_TOKEN=<secret> mitrdb-cli --listen <127.0.0.1:7432> --db <path> --wal <path> --tls-cert <cert.pem> --tls-key <key.pem> [--max-connections <n>] [--max-frame-bytes <n>]\n\nMITRDB_AUTH_TOKEN must be at least 32 bytes. Remote binds require BOTH TLS and authentication. TLS is optional on loopback for local development."
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut db_path: Option<String> = None;
    let mut wal_path: Option<String> = None;
    let mut sql_script: Option<String> = None;
    let mut listen_addr: Option<SocketAddr> = None;
    let mut max_connections: Option<usize> = None;
    let mut max_frame_bytes: Option<usize> = None;
    let mut tls_cert_path: Option<std::path::PathBuf> = None;
    let mut tls_key_path: Option<std::path::PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print_usage();
                return;
            }
            "--db" if i + 1 < args.len() => {
                db_path = Some(args[i + 1].clone());
                i += 2;
            }
            "--wal" if i + 1 < args.len() => {
                wal_path = Some(args[i + 1].clone());
                i += 2;
            }
            "--sql" if i + 1 < args.len() => {
                sql_script = Some(args[i + 1].clone());
                i += 2;
            }
            "--listen" if i + 1 < args.len() => {
                match args[i + 1].parse::<SocketAddr>() {
                    Ok(addr) => listen_addr = Some(addr),
                    Err(err) => {
                        eprintln!("invalid --listen address: {err}");
                        std::process::exit(2);
                    }
                }
                i += 2;
            }
            "--max-connections" if i + 1 < args.len() => {
                match args[i + 1].parse::<usize>() {
                    Ok(value) => max_connections = Some(value),
                    Err(err) => {
                        eprintln!("invalid --max-connections value: {err}");
                        std::process::exit(2);
                    }
                }
                i += 2;
            }
            "--tls-cert" if i + 1 < args.len() => {
                tls_cert_path = Some(args[i + 1].clone().into());
                i += 2;
            }
            "--tls-key" if i + 1 < args.len() => {
                tls_key_path = Some(args[i + 1].clone().into());
                i += 2;
            }
            "--max-frame-bytes" if i + 1 < args.len() => {
                match args[i + 1].parse::<usize>() {
                    Ok(value) => max_frame_bytes = Some(value),
                    Err(err) => {
                        eprintln!("invalid --max-frame-bytes value: {err}");
                        std::process::exit(2);
                    }
                }
                i += 2;
            }
            unknown => {
                eprintln!("unknown or incomplete argument: {unknown}");
                eprintln!("Use --help for usage.");
                std::process::exit(2);
            }
        }
    }

    if db_path.is_some() != wal_path.is_some() {
        eprintln!("--db and --wal must be provided together");
        std::process::exit(2);
    }

    if let Some(bind_addr) = listen_addr {
        if db_path.is_none() || wal_path.is_none() {
            eprintln!("--listen requires both --db and --wal for persistent database mode");
            std::process::exit(2);
        }
        if sql_script.is_some() {
            eprintln!(
                "--sql cannot be combined with --listen; use a network client after the server starts"
            );
            std::process::exit(2);
        }

        let server = if let (Some(db), Some(wal)) = (db_path, wal_path) {
            let (server, report) = match Server::open(&db, &wal, 64) {
                Ok(res) => res,
                Err(err) => {
                    eprintln!("mitrdb-cli open error: {err}");
                    std::process::exit(1);
                }
            };
            println!(
                "mitrdb-cli v0.1.0 (recovered: {} redone, {} undone)",
                report.redone_records, report.undone_records
            );
            server
        } else {
            Server::new()
        };

        let auth_token = match std::env::var("MITRDB_AUTH_TOKEN") {
            Ok(token) if token.len() >= 32 => Some(Zeroizing::new(token)),
            Ok(_) => {
                eprintln!("MITRDB_AUTH_TOKEN must be at least 32 bytes; refusing to start");
                std::process::exit(2);
            }
            Err(_) => {
                eprintln!(
                    "MITRDB_AUTH_TOKEN is required for TCP server mode; remote binds require both TLS and authentication"
                );
                std::process::exit(2);
            }
        };
        if tls_cert_path.is_some() != tls_key_path.is_some() {
            eprintln!("--tls-cert and --tls-key must be provided together");
            std::process::exit(2);
        }
        let mut config = TcpServerConfig {
            bind_addr,
            auth_token,
            tls_cert_path,
            tls_key_path,
            ..TcpServerConfig::default()
        };
        if let Some(value) = max_connections {
            config.max_connections = value;
        }
        if let Some(value) = max_frame_bytes {
            config.max_frame_bytes = value;
        }

        let tls_enabled = config.tls_cert_path.is_some();
        let handle = match Arc::new(server).bind_tcp(config) {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("mitrdb-cli server error: {err}");
                std::process::exit(1);
            }
        };
        println!(
            "mitrdb-cli v0.1.0 listening on {} (TLS: {})",
            handle.local_addr(),
            if tls_enabled { "enabled" } else { "disabled" }
        );
        if let Err(err) = handle.wait() {
            eprintln!("mitrdb-cli server stopped with error: {err}");
            std::process::exit(1);
        }
        return;
    }

    if max_connections.is_some() || max_frame_bytes.is_some() {
        eprintln!("--max-connections and --max-frame-bytes require --listen");
        std::process::exit(2);
    }

    if let (Some(db), Some(wal)) = (db_path, wal_path) {
        let (server, report) = match Server::open(&db, &wal, 64) {
            Ok(res) => res,
            Err(err) => {
                eprintln!("mitrdb-cli open error: {}", err);
                std::process::exit(1);
            }
        };
        if let Some(script) = sql_script {
            for stmt in split_statements(&script) {
                match server.execute_sql(&stmt) {
                    Ok(res) => println!("{:?}", res),
                    Err(err) => {
                        eprintln!("SQL error: {}", err);
                        std::process::exit(1);
                    }
                }
            }
        } else {
            println!(
                "mitrdb-cli v0.1.0 (recovered: {} redone, {} undone)",
                report.redone_records, report.undone_records
            );
        }
    } else {
        if sql_script.is_some() {
            eprintln!("--sql requires both --db and --wal");
            std::process::exit(2);
        }
        let server = Server::new();
        if let Err(err) = server.start() {
            eprintln!("mitrdb-cli error: {}", err);
            std::process::exit(1);
        }
        println!("mitrdb-cli v0.1.0");
    }
}
