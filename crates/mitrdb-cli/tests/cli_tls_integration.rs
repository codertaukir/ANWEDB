use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TOKEN: &str = "mitrdb-integration-token-at-least-32-bytes";
const PROTOCOL: &[u8] = b"MITRDB/1";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

struct CliServer {
    child: Child,
    addr: SocketAddr,
    _db: PathBuf,
    _wal: PathBuf,
}
impl Drop for CliServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_cli(tls: bool) -> CliServer {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db = std::env::temp_dir().join(format!("mitrdb-cli-tls-{unique}.db"));
    let wal = std::env::temp_dir().join(format!("mitrdb-cli-tls-{unique}.wal"));
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mitrdb-cli"));
    cmd.arg("--listen")
        .arg(addr.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--wal")
        .arg(&wal)
        .env("MITRDB_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if tls {
        cmd.arg("--tls-cert")
            .arg(fixture("server-cert.pem"))
            .arg("--tls-key")
            .arg(fixture("server-key.pem"));
    }
    let child = cmd
        .spawn()
        .expect("launch actual mitrdb-cli server process");
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Ok(stream) = TcpStream::connect(addr) {
            drop(stream);
            break;
        }
        assert!(Instant::now() < deadline, "CLI server did not start");
        thread::sleep(Duration::from_millis(30));
    }
    CliServer {
        child,
        addr,
        _db: db,
        _wal: wal,
    }
}

fn tls_client(
    addr: SocketAddr,
    server_name: &str,
    trust_ca: bool,
) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut roots = RootCertStore::empty();
    if trust_ca {
        let file = File::open(fixture("ca.pem"))?;
        let certs = rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut connection = ClientConnection::new(Arc::new(config), name)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut socket = TcpStream::connect(addr)?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    connection.complete_io(&mut socket)?;
    Ok(StreamOwned::new(connection, socket))
}

fn send_frame<S: Write>(stream: &mut S, payload: &[u8]) -> io::Result<()> {
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}
fn recv_frame<S: Read>(stream: &mut S) -> io::Result<Vec<u8>> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized frame",
        ));
    }
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}
fn authenticated_stream(
    server: &CliServer,
    token: &str,
) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut stream = tls_client(server.addr, "localhost", true)?;
    send_frame(&mut stream, PROTOCOL)?;
    let greeting = recv_frame(&mut stream)?;
    if !String::from_utf8_lossy(&greeting).contains("MITRDB/1") {
        return Err(io::Error::other("protocol greeting missing"));
    }
    send_frame(&mut stream, format!("AUTH {token}").as_bytes())?;
    Ok(stream)
}

#[test]
fn actual_cli_tls_auth_and_sql_round_trip() {
    let server = start_cli(true);
    let mut stream = authenticated_stream(&server, TOKEN).unwrap();
    assert!(String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).contains("authenticated"));
    send_frame(
        &mut stream,
        b"CREATE TABLE cli_tls_smoke (id INT PRIMARY KEY)",
    )
    .unwrap();
    assert!(
        String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).contains("\"status\":\"ok\"")
    );
    send_frame(&mut stream, b"INSERT INTO cli_tls_smoke VALUES (1)").unwrap();
    assert!(String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).contains("affected"));
    send_frame(&mut stream, b"SELECT id FROM cli_tls_smoke").unwrap();
    let rows = String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).into_owned();
    assert!(
        rows.contains("\"status\":\"ok\"") && rows.contains("[1]"),
        "unexpected SQL response: {rows}"
    );
}

#[test]
fn actual_cli_rejects_untrusted_ca_and_hostname_mismatch() {
    let server = start_cli(true);
    assert!(
        tls_client(server.addr, "localhost", false).is_err(),
        "untrusted CA must be rejected"
    );
    assert!(
        tls_client(server.addr, "wrong-hostname.invalid", true).is_err(),
        "hostname mismatch must be rejected"
    );
}

#[test]
fn actual_cli_tls_listener_does_not_serve_plaintext_protocol() {
    let server = start_cli(true);
    let mut socket = TcpStream::connect(server.addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    send_frame(&mut socket, PROTOCOL).unwrap();
    let response = recv_frame(&mut socket);
    assert!(
        response.is_err() || !String::from_utf8_lossy(&response.unwrap()).contains("MITRDB/1"),
        "plaintext client must not receive a successful protocol greeting from TLS listener"
    );
}

#[test]
fn actual_cli_rate_limits_repeated_bad_authentication() {
    let server = start_cli(true);
    for attempt in 0..5 {
        let mut stream =
            authenticated_stream(&server, "incorrect-token-that-is-long-enough").unwrap();
        let response = String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).into_owned();
        assert!(
            response.contains("AUTH_FAILED"),
            "attempt {attempt} should fail authentication, got {response}"
        );
    }
    let mut stream = tls_client(server.addr, "localhost", true).unwrap();
    send_frame(&mut stream, PROTOCOL).unwrap();
    let _ = recv_frame(&mut stream).unwrap();
    let response = String::from_utf8_lossy(&recv_frame(&mut stream).unwrap()).into_owned();
    assert!(
        response.contains("AUTH_RATE_LIMITED"),
        "expected rate-limit response, got {response}"
    );
}
