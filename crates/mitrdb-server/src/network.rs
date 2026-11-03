use rustls::{ServerConfig as RustlsServerConfig, ServerConnection, StreamOwned};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

use mitrdb_exec::ExecutionResult;
use mitrdb_query::Value;

use crate::{Error, Result, Server};

const PROTOCOL_VERSION: &str = "MITRDB/1";
const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const MIN_FRAME_BYTES: usize = 256;
const DEFAULT_IO_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const AUTH_FAILURE_LIMIT: usize = 5;
const AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const AUTH_FAILURE_TRACKED_IP_LIMIT: usize = 4096;
type AuthFailureMap = Arc<Mutex<HashMap<IpAddr, VecDeque<Instant>>>>;

#[derive(Clone)]
pub struct TcpServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub max_frame_bytes: usize,
    pub io_timeout: Duration,
    /// Once the first byte of a frame arrives, the whole frame must arrive within this time.
    /// Protects against slow-loris clients that trickle bytes.
    pub frame_timeout: Duration,
    /// Time allowed from connection start (including the TLS handshake) until the
    /// protocol hello (and the AUTH frame, when enabled) has been received.
    pub handshake_timeout: Duration,
    /// Optional shared secret required before any session commands.
    /// Never transmit this token over a plaintext connection on an untrusted network.
    pub auth_token: Option<Zeroizing<String>>,
    /// PEM-encoded server certificate chain. Must be paired with `tls_key_path`.
    pub tls_cert_path: Option<PathBuf>,
    /// PEM-encoded unencrypted PKCS#8/RSA/EC private key. Protect this file.
    pub tls_key_path: Option<PathBuf>,
}

impl std::fmt::Debug for TcpServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpServerConfig")
            .field("bind_addr", &self.bind_addr)
            .field("max_connections", &self.max_connections)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("io_timeout", &self.io_timeout)
            .field("frame_timeout", &self.frame_timeout)
            .field("handshake_timeout", &self.handshake_timeout)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("tls_cert_path", &self.tls_cert_path)
            .field("tls_key_path", &self.tls_key_path)
            .finish()
    }
}

impl Default for TcpServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 7432)),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            io_timeout: DEFAULT_IO_TIMEOUT,
            frame_timeout: DEFAULT_FRAME_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            auth_token: None,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

pub struct TcpServerHandle {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<Result<()>>>>,
}

impl TcpServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        let join = self.join.lock().unwrap().take();
        match join {
            Some(join) => join
                .join()
                .map_err(|_| Error::Io("TCP server thread panicked".to_string()))?,
            None => Ok(()),
        }
    }

    pub fn wait(&self) -> Result<()> {
        let join = self.join.lock().unwrap().take();
        match join {
            Some(join) => join
                .join()
                .map_err(|_| Error::Io("TCP server thread panicked".to_string()))?,
            None => Ok(()),
        }
    }
}

impl Drop for TcpServerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(join) = self.join.get_mut()
            && let Some(join) = join.take()
        {
            let _ = join.join();
        }
    }
}

impl Server {
    pub fn bind_tcp(self: &Arc<Self>, config: TcpServerConfig) -> Result<TcpServerHandle> {
        if config.max_connections == 0 {
            return Err(Error::Io(
                "max_connections must be greater than zero".to_string(),
            ));
        }
        if !(MIN_FRAME_BYTES..=u32::MAX as usize).contains(&config.max_frame_bytes) {
            return Err(Error::Io(format!(
                "max_frame_bytes must be between {} and {}",
                MIN_FRAME_BYTES,
                u32::MAX
            )));
        }
        if config.io_timeout.is_zero() {
            return Err(Error::Io(
                "io_timeout must be greater than zero".to_string(),
            ));
        }
        if config.frame_timeout.is_zero() {
            return Err(Error::Io(
                "frame_timeout must be greater than zero".to_string(),
            ));
        }
        if config.handshake_timeout.is_zero() {
            return Err(Error::Io(
                "handshake_timeout must be greater than zero".to_string(),
            ));
        }

        if config.tls_cert_path.is_some() != config.tls_key_path.is_some() {
            return Err(Error::Io(
                "TLS requires both --tls-cert and --tls-key".to_string(),
            ));
        }
        let tls_config = match (&config.tls_cert_path, &config.tls_key_path) {
            (Some(cert_path), Some(key_path)) => Some(load_tls_config(cert_path, key_path)?),
            (None, None) => None,
            _ => unreachable!(),
        };
        if !config.bind_addr.ip().is_loopback()
            && (tls_config.is_none() || config.auth_token.is_none())
        {
            return Err(Error::Io(
                "remote binds require TLS and authentication".to_string(),
            ));
        }

        let listener = TcpListener::bind(config.bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let server = Arc::clone(self);
        let join = thread::Builder::new()
            .name("mitrdb-tcp-accept".to_string())
            .spawn(move || accept_loop(listener, server, config, tls_config, thread_shutdown))?;

        Ok(TcpServerHandle {
            local_addr,
            shutdown,
            join: Mutex::new(Some(join)),
        })
    }
}

fn accept_loop(
    listener: TcpListener,
    server: Arc<Server>,
    config: TcpServerConfig,
    tls_config: Option<Arc<RustlsServerConfig>>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let active = Arc::new(AtomicUsize::new(0));
    let auth_failures: AuthFailureMap = Arc::new(Mutex::new(HashMap::new()));
    let mut workers: Vec<JoinHandle<()>> = Vec::new();
    let outcome = loop {
        if shutdown.load(Ordering::Acquire) {
            break Ok(());
        }
        let mut i = 0;
        while i < workers.len() {
            if workers[i].is_finished() {
                let worker = workers.swap_remove(i);
                let _ = worker.join();
            } else {
                i += 1;
            }
        }

        match listener.accept() {
            Ok((stream, peer_addr)) => {
                if !reserve_connection(&active, config.max_connections) {
                    // Close excess connections without writing plaintext into a TLS socket.
                    drop(stream);
                    continue;
                }

                let worker_server = Arc::clone(&server);
                let worker_shutdown = Arc::clone(&shutdown);
                let worker_active = Arc::clone(&active);
                let worker_config = config.clone();
                let worker_tls = tls_config.clone();
                let worker_auth_failures = Arc::clone(&auth_failures);
                let peer_ip = peer_addr.ip();
                match thread::Builder::new()
                    .name("mitrdb-tcp-client".to_string())
                    .spawn(move || {
                        let _guard = ActiveConnection(worker_active);
                        let _ = serve_connection(
                            worker_server,
                            stream,
                            peer_ip,
                            worker_config,
                            worker_tls,
                            worker_shutdown,
                            worker_auth_failures,
                        );
                    }) {
                    Ok(worker) => workers.push(worker),
                    Err(err) => {
                        active.fetch_sub(1, Ordering::AcqRel);
                        break Err(Error::from(err));
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => break Err(Error::from(err)),
        }
    };

    shutdown.store(true, Ordering::Release);
    for worker in workers {
        let _ = worker.join();
    }
    outcome
}

fn reserve_connection(active: &AtomicUsize, max: usize) -> bool {
    let mut current = active.load(Ordering::Acquire);
    loop {
        if current >= max {
            return false;
        }
        match active.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

struct ActiveConnection(Arc<AtomicUsize>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn serve_connection(
    server: Arc<Server>,
    tcp_stream: TcpStream,
    peer_ip: IpAddr,
    config: TcpServerConfig,
    tls_config: Option<Arc<RustlsServerConfig>>,
    shutdown: Arc<AtomicBool>,
    auth_failures: AuthFailureMap,
) -> io::Result<()> {
    tcp_stream.set_nodelay(true)?;
    tcp_stream.set_read_timeout(Some(config.io_timeout))?;
    tcp_stream.set_write_timeout(Some(config.io_timeout))?;

    if let Some(tls_config) = tls_config {
        let connection = ServerConnection::new(tls_config)
            .map_err(|err| io::Error::other(format!("TLS setup failed: {err}")))?;
        let mut stream = ClientStream::Tls(Box::new(StreamOwned::new(connection, tcp_stream)));
        serve_stream(
            &server,
            &mut stream,
            &config,
            &shutdown,
            peer_ip,
            &auth_failures,
        )
    } else {
        let mut stream = ClientStream::Plain(tcp_stream);
        serve_stream(
            &server,
            &mut stream,
            &config,
            &shutdown,
            peer_ip,
            &auth_failures,
        )
    }
}

enum ClientStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}
impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

fn load_tls_config(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<Arc<RustlsServerConfig>> {
    let cert_file = File::open(cert_path)
        .map_err(|err| Error::Io(format!("cannot open TLS certificate: {err}")))?;
    let mut cert_reader = BufReader::new(cert_file);
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    let certs = CertificateDer::pem_reader_iter(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| Error::Io(format!("cannot parse TLS certificate PEM: {err}")))?;
    if certs.is_empty() {
        return Err(Error::Io(
            "TLS certificate file contains no certificates".to_string(),
        ));
    }

    validate_private_key_permissions(key_path)?;
    let key_file = File::open(key_path)
        .map_err(|err| Error::Io(format!("cannot open TLS private key: {err}")))?;
    let mut key_reader = BufReader::new(key_file);
    let key = PrivateKeyDer::from_pem_reader(&mut key_reader)
        .map_err(|err| Error::Io(format!("cannot parse TLS private key PEM: {err}")))?;
    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| Error::Io(format!("invalid TLS certificate/private key pair: {err}")))?;
    Ok(Arc::new(config))
}

#[cfg(unix)]
fn validate_private_key_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|err| Error::Io(format!("cannot inspect TLS private-key permissions: {err}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Io(
            "TLS private key must be a regular, non-symlink file".to_string(),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::Io(
            "TLS private key is accessible by group/others; restrict permissions (for example chmod 600)".to_string(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_key_permissions(path: &std::path::Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|err| Error::Io(format!("cannot inspect TLS private-key metadata: {err}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Io(
            "TLS private key must be a regular, non-symlink file".to_string(),
        ));
    }
    // std does not expose portable Windows ACL inspection; administrators must
    // restrict this file to the service identity using NTFS permissions.
    Ok(())
}

fn serve_stream<S: Read + Write>(
    server: &Arc<Server>,
    stream: &mut S,
    config: &TcpServerConfig,
    shutdown: &AtomicBool,
    peer_ip: IpAddr,
    auth_failures: &AuthFailureMap,
) -> io::Result<()> {
    let conn_started = Instant::now();
    let handshake_limits = FrameLimits {
        frame_timeout: Some(config.frame_timeout),
        deadline: conn_started.checked_add(config.handshake_timeout),
    };
    let request_limits = FrameLimits {
        frame_timeout: Some(config.frame_timeout),
        deadline: None,
    };
    let hello = match read_frame_with_limits(
        stream,
        config.max_frame_bytes,
        shutdown,
        handshake_limits,
    )? {
        Some(frame) => frame,
        None => return Ok(()),
    };
    if hello.as_slice() != PROTOCOL_VERSION.as_bytes() {
        let _ = write_frame(
            stream,
            br#"{"status":"error","code":"PROTOCOL_MISMATCH","error":"send MITRDB/1 as the first frame"}"#,
            config.max_frame_bytes,
        );
        return Ok(());
    }
    write_frame(
        stream,
        br#"{"status":"ok","protocol":"MITRDB/1"}"#,
        config.max_frame_bytes,
    )?;

    if let Some(expected_token) = config.auth_token.as_deref() {
        if auth_is_rate_limited(auth_failures, peer_ip, Instant::now()) {
            let _ = write_frame(
                stream,
                br#"{"status":"error","code":"AUTH_RATE_LIMITED","error":"authentication temporarily rate limited"}"#,
                config.max_frame_bytes,
            );
            return Ok(());
        }
        let auth_frame = match read_frame_with_limits(
            stream,
            config.max_frame_bytes,
            shutdown,
            handshake_limits,
        )? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let supplied = std::str::from_utf8(&auth_frame)
            .ok()
            .and_then(|frame| frame.strip_prefix("AUTH "));
        let authenticated = supplied
            .map(|token| constant_time_eq(token.as_bytes(), expected_token.as_bytes()))
            .unwrap_or(false);
        if !authenticated {
            record_auth_failure(auth_failures, peer_ip, Instant::now());
            let _ = write_frame(
                stream,
                br#"{"status":"error","code":"AUTH_FAILED","error":"authentication failed"}"#,
                config.max_frame_bytes,
            );
            return Ok(());
        }
        clear_auth_failures(auth_failures, peer_ip);
        write_frame(
            stream,
            br#"{"status":"ok","authenticated":true}"#,
            config.max_frame_bytes,
        )?;
    }

    let session_id = server.create_session();
    let result = loop {
        if shutdown.load(Ordering::Acquire) {
            break Ok(());
        }
        let request = match read_frame_with_limits(
            stream,
            config.max_frame_bytes,
            shutdown,
            request_limits,
        ) {
            Ok(Some(frame)) => frame,
            Ok(None) => break Ok(()),
            Err(err) => break Err(err),
        };
        let request = match std::str::from_utf8(&request) {
            Ok(request) => request.trim(),
            Err(_) => {
                write_response(
                    stream,
                    error_json("INVALID_UTF8", "request frame must contain UTF-8"),
                    config.max_frame_bytes,
                )?;
                continue;
            }
        };

        if request.eq_ignore_ascii_case("PING") {
            write_response(
                stream,
                r#"{"status":"ok","result":"PONG"}"#.to_string(),
                config.max_frame_bytes,
            )?;
            continue;
        }
        if request.eq_ignore_ascii_case("HEALTH") {
            write_response(
                stream,
                r#"{"status":"ok","result":"READY"}"#.to_string(),
                config.max_frame_bytes,
            )?;
            continue;
        }
        if request.eq_ignore_ascii_case("QUIT") {
            write_response(
                stream,
                r#"{"status":"ok","result":"BYE"}"#.to_string(),
                config.max_frame_bytes,
            )?;
            break Ok(());
        }
        if request.is_empty() {
            write_response(
                stream,
                error_json("EMPTY_REQUEST", "request frame is empty"),
                config.max_frame_bytes,
            )?;
            continue;
        }

        let response = match server.execute_in_session(session_id, request) {
            Ok(result) => success_json(&result),
            Err(err) => error_json("SQL_ERROR", &err.to_string()),
        };
        write_response(stream, response, config.max_frame_bytes)?;
    };

    let _ = server.close_session(session_id);
    result
}

fn prune_auth_failures(failures: &mut VecDeque<Instant>, now: Instant) {
    while failures
        .front()
        .is_some_and(|at| now.saturating_duration_since(*at) >= AUTH_FAILURE_WINDOW)
    {
        failures.pop_front();
    }
}

fn auth_is_rate_limited(state: &AuthFailureMap, ip: IpAddr, now: Instant) -> bool {
    let Ok(mut map) = state.lock() else {
        return true;
    };
    if !map.contains_key(&ip) && map.len() >= AUTH_FAILURE_TRACKED_IP_LIMIT {
        return true;
    }
    let mut remove_entry = false;
    let limited = if let Some(failures) = map.get_mut(&ip) {
        prune_auth_failures(failures, now);
        remove_entry = failures.is_empty();
        failures.len() >= AUTH_FAILURE_LIMIT
    } else {
        false
    };
    if remove_entry {
        map.remove(&ip);
    }
    limited
}

fn record_auth_failure(state: &AuthFailureMap, ip: IpAddr, now: Instant) {
    let Ok(mut map) = state.lock() else {
        return;
    };
    if map.len() >= AUTH_FAILURE_TRACKED_IP_LIMIT {
        map.retain(|_, entries| {
            prune_auth_failures(entries, now);
            !entries.is_empty()
        });
        if map.len() >= AUTH_FAILURE_TRACKED_IP_LIMIT && !map.contains_key(&ip) {
            // Fail closed for new identities when the bounded table is saturated.
            return;
        }
    }
    let entries = map.entry(ip).or_default();
    prune_auth_failures(entries, now);
    entries.push_back(now);
}

fn clear_auth_failures(state: &AuthFailureMap, ip: IpAddr) {
    if let Ok(mut map) = state.lock() {
        map.remove(&ip);
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

/// Per-call limits that stop slow-loris clients from holding a worker thread forever.
#[derive(Clone, Copy)]
struct FrameLimits {
    /// Max time from the first byte of a frame until the whole frame has arrived.
    frame_timeout: Option<Duration>,
    /// Absolute point in time by which the frame must be complete.
    deadline: Option<Instant>,
}

impl FrameLimits {
    #[cfg(test)]
    const NONE: Self = Self {
        frame_timeout: None,
        deadline: None,
    };
}

fn check_frame_deadlines(limits: FrameLimits, frame_started: Option<Instant>) -> io::Result<()> {
    if limits.frame_timeout.is_none() && limits.deadline.is_none() {
        return Ok(());
    }
    let now = Instant::now();
    if limits.deadline.is_some_and(|deadline| now >= deadline) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "handshake deadline exceeded",
        ));
    }
    if let (Some(limit), Some(started)) = (limits.frame_timeout, frame_started)
        && now.saturating_duration_since(started) >= limit
    {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "frame not completed within frame_timeout",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn read_frame<S: Read>(
    stream: &mut S,
    max_frame_bytes: usize,
    shutdown: &AtomicBool,
) -> io::Result<Option<Vec<u8>>> {
    read_frame_with_limits(stream, max_frame_bytes, shutdown, FrameLimits::NONE)
}

fn read_frame_with_limits<S: Read>(
    stream: &mut S,
    max_frame_bytes: usize,
    shutdown: &AtomicBool,
    limits: FrameLimits,
) -> io::Result<Option<Vec<u8>>> {
    let mut frame_started: Option<Instant> = None;
    let mut header = [0u8; 4];
    let mut offset = 0;
    while offset < header.len() {
        check_frame_deadlines(limits, frame_started)?;
        match stream.read(&mut header[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed inside frame header",
                ));
            }
            Ok(n) => {
                offset += n;
                frame_started.get_or_insert_with(Instant::now);
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if shutdown.load(Ordering::Acquire) {
                    return Ok(None);
                }
            }
            Err(err) => return Err(err),
        }
    }

    let frame_len = u32::from_be_bytes(header) as usize;
    if frame_len > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds configured maximum",
        ));
    }
    let mut frame = vec![0u8; frame_len];
    offset = 0;
    while offset < frame.len() {
        check_frame_deadlines(limits, frame_started)?;
        match stream.read(&mut frame[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed inside frame payload",
                ));
            }
            Ok(n) => offset += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if shutdown.load(Ordering::Acquire) {
                    return Ok(None);
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(Some(frame))
}

fn write_frame<S: Write>(stream: &mut S, frame: &[u8], max_frame_bytes: usize) -> io::Result<()> {
    if frame.len() > max_frame_bytes || frame.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response exceeds configured maximum frame size",
        ));
    }
    stream.write_all(&(frame.len() as u32).to_be_bytes())?;
    stream.write_all(frame)
}

fn write_response<S: Write>(
    stream: &mut S,
    response: String,
    max_frame_bytes: usize,
) -> io::Result<()> {
    if response.len() <= max_frame_bytes {
        return write_frame(stream, response.as_bytes(), max_frame_bytes);
    }
    write_frame(
        stream,
        br#"{"status":"error","code":"RESPONSE_TOO_LARGE","error":"response exceeds configured frame limit"}"#,
        max_frame_bytes,
    )
}

fn success_json(result: &ExecutionResult) -> String {
    let encoded = match result {
        ExecutionResult::Ok => r#"{"type":"ok"}"#.to_string(),
        ExecutionResult::Affected(count) => format!(r#"{{"type":"affected","count":{count}}}"#),
        ExecutionResult::Explained(plan) => {
            format!(r#"{{"type":"explained","plan":{}}}"#, json_string(plan))
        }
        ExecutionResult::Rows { columns, rows } => {
            let columns = columns
                .iter()
                .map(|column| json_string(column))
                .collect::<Vec<_>>()
                .join(",");
            let rows = rows
                .iter()
                .map(|row| {
                    format!(
                        "[{}]",
                        row.iter().map(value_json).collect::<Vec<_>>().join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"rows","columns":[{columns}],"rows":[{rows}]}}"#)
        }
    };
    format!(r#"{{"status":"ok","result":{encoded}}}"#)
}

fn error_json(code: &str, message: &str) -> String {
    format!(
        r#"{{"status":"error","error":{{"code":{},"message":{}}}}}"#,
        json_string(code),
        json_string(message)
    )
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r#"\\"#),
            '\n' => out.push_str(r#"\n"#),
            '\r' => out.push_str(r#"\r"#),
            '\t' => out.push_str(r#"\t"#),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, r#"\u{:04x}"#, c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn value_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Text(s) => json_string(s),
        Value::Bool(b) => b.to_string(),
        Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            format!(r#"{{"type":"bytes","hex":{}}}"#, json_string(&hex))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::{ClientConnection, StreamOwned};
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::AtomicBool;

    fn send_frame<W: Write>(writer: &mut W, payload: &[u8]) {
        let len = payload.len() as u32;
        if writer.write_all(&len.to_be_bytes()).is_ok() {
            let _ = writer.write_all(payload);
            let _ = writer.flush();
        }
    }

    fn receive_frame<R: Read>(reader: &mut R) -> Vec<u8> {
        let shutdown = AtomicBool::new(false);
        read_frame(reader, 16 * 1024 * 1024, &shutdown)
            .unwrap()
            .expect("expected frame response")
    }

    #[test]
    fn tcp_protocol_handshake_ping_and_sql_session() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let db_path = std::env::temp_dir().join(format!("mitrdb-network-test-{unique}.db"));
        let wal_path = std::env::temp_dir().join(format!("mitrdb-network-test-{unique}.wal"));

        let (server, _recovery) = Server::open(&db_path, &wal_path, 64).unwrap();
        let server = Arc::new(server);
        let config = TcpServerConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..TcpServerConfig::default()
        };
        let handle = server.bind_tcp(config).unwrap();
        let mut stream = TcpStream::connect(handle.local_addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        send_frame(&mut stream, PROTOCOL_VERSION.as_bytes());
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("MITRDB/1")
        );
        send_frame(&mut stream, b"PING");
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("PONG")
        );
        send_frame(
            &mut stream,
            b"CREATE TABLE net_smoke (id INT PRIMARY KEY, name TEXT)",
        );
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("\"status\":\"ok\"")
        );
        send_frame(&mut stream, b"INSERT INTO net_smoke VALUES (1, 'Taukir')");
        let insert_response = String::from_utf8(receive_frame(&mut stream)).unwrap();
        println!("INSERT RESPONSE: {insert_response}");
        assert!(insert_response.contains("affected"));
        send_frame(&mut stream, b"SELECT id, name FROM net_smoke");
        let rows = String::from_utf8(receive_frame(&mut stream)).unwrap();
        assert!(rows.contains("Taukir"));
        send_frame(&mut stream, b"QUIT");
        let _ = receive_frame(&mut stream);
        drop(stream);
        handle.shutdown().unwrap();
    }

    #[test]
    fn tcp_session_rejects_wrong_authentication_token() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("mitrdb-auth-test-{unique}.db"));
        let wal_path = std::env::temp_dir().join(format!("mitrdb-auth-test-{unique}.wal"));
        let (server, _recovery) = Server::open(&db_path, &wal_path, 64).unwrap();
        let server = Arc::new(server);
        let config = TcpServerConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            auth_token: Some(Zeroizing::new(
                "correct-token-with-at-least-32-bytes".to_string(),
            )),
            ..TcpServerConfig::default()
        };
        let handle = server.bind_tcp(config).unwrap();
        let mut stream = TcpStream::connect(handle.local_addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        send_frame(&mut stream, PROTOCOL_VERSION.as_bytes());
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("MITRDB/1")
        );
        send_frame(&mut stream, b"AUTH wrong-token-with-at-least-32-bytes");
        let response = String::from_utf8(receive_frame(&mut stream)).unwrap();
        assert!(response.contains("AUTH_FAILED"));
        drop(stream);
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn tcp_server_config_debug_redacts_authentication_token() {
        let secret = "never-print-this-test-token-0123456789";
        let config = TcpServerConfig {
            auth_token: Some(Zeroizing::new(secret.to_string())),
            ..TcpServerConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(secret));
    }

    #[test]
    fn authentication_token_comparison_checks_content_and_length() {
        assert!(constant_time_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_eq(b"secret-token", b"secret-tokeN"));
        assert!(!constant_time_eq(b"secret-token", b"secret"));
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("tls")
            .join(name)
    }

    /// Git does not preserve 0600, so copy the fixture key to a private temp file.
    fn private_key_copy(tag: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let dst = std::env::temp_dir().join(format!("mitrdb-tls-key-{tag}.pem"));
        std::fs::copy(fixture("server-key.pem"), &dst).unwrap();
        std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o600)).unwrap();
        dst
    }

    fn start_tls_test_server(
        cert_name: &str,
        token: Option<&str>,
    ) -> (TcpServerHandle, PathBuf, PathBuf) {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("mitrdb-tls-test-{unique}.db"));
        let wal_path = std::env::temp_dir().join(format!("mitrdb-tls-test-{unique}.wal"));
        let (server, _recovery) = Server::open(&db_path, &wal_path, 64).unwrap();
        let config = TcpServerConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            auth_token: token.map(|value| Zeroizing::new(value.to_owned())),
            tls_cert_path: Some(fixture(cert_name)),
            tls_key_path: Some(private_key_copy(&unique.to_string())),
            io_timeout: Duration::from_millis(500),
            ..TcpServerConfig::default()
        };
        let handle = Arc::new(server).bind_tcp(config).unwrap();
        (handle, db_path, wal_path)
    }

    fn trusted_tls_client(
        addr: SocketAddr,
    ) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
        trusted_tls_client_for_name(addr, "localhost")
    }

    fn trusted_tls_client_for_name(
        addr: SocketAddr,
        hostname: &str,
    ) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
        let ca_file = File::open(fixture("ca.pem"))?;
        let mut reader = BufReader::new(ca_file);
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let roots = CertificateDer::pem_reader_iter(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| io::Error::other(format!("test CA parse failed: {err}")))?;
        let mut root_store = rustls::RootCertStore::empty();
        for root in roots {
            root_store
                .add(root)
                .map_err(|err| io::Error::other(format!("test CA rejected: {err}")))?;
        }
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let server_name = rustls::pki_types::ServerName::try_from(hostname.to_owned())
            .map_err(|err| io::Error::other(format!("invalid test server name: {err}")))?;
        let connection = ClientConnection::new(Arc::new(client_config), server_name)
            .map_err(|err| io::Error::other(format!("TLS client setup failed: {err}")))?;
        let socket = TcpStream::connect(addr)?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(StreamOwned::new(connection, socket))
    }

    fn start_plain_test_server(
        frame_timeout: Duration,
        handshake_timeout: Duration,
    ) -> (TcpServerHandle, PathBuf, PathBuf) {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("mitrdb-plain-test-{unique}.db"));
        let wal_path = std::env::temp_dir().join(format!("mitrdb-plain-test-{unique}.wal"));
        let (server, _recovery) = Server::open(&db_path, &wal_path, 64).unwrap();
        let config = TcpServerConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_connections: 4,
            io_timeout: Duration::from_millis(100),
            frame_timeout,
            handshake_timeout,
            ..TcpServerConfig::default()
        };
        let handle = Arc::new(server).bind_tcp(config).unwrap();
        (handle, db_path, wal_path)
    }

    /// True if the peer closed the connection (EOF or reset) while we waited on a short read.
    fn peer_closed(sock: &mut TcpStream) -> bool {
        let mut buf = [0u8; 16];
        match sock.read(&mut buf) {
            Ok(0) => true,
            Ok(_) => false,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                false
            }
            Err(_) => true,
        }
    }

    #[test]
    fn slow_loris_trickled_frame_is_disconnected() {
        let (handle, db_path, wal_path) =
            start_plain_test_server(Duration::from_millis(600), Duration::from_secs(5));
        let mut sock = TcpStream::connect(handle.local_addr()).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        // Header announces a 100-byte frame; payload then trickles in at 1 byte / 100 ms.
        sock.write_all(&100u32.to_be_bytes()).unwrap();
        let started = Instant::now();
        let mut closed = false;
        for _ in 0..30 {
            std::thread::sleep(Duration::from_millis(100));
            if sock.write_all(b"x").is_err() || peer_closed(&mut sock) {
                closed = true;
                break;
            }
        }
        let elapsed = started.elapsed();
        drop(sock);
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
        assert!(closed, "server kept a trickling connection open for {elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(3),
            "disconnect took too long: {elapsed:?}"
        );
    }

    #[test]
    fn silent_connection_is_closed_after_handshake_timeout() {
        let (handle, db_path, wal_path) =
            start_plain_test_server(Duration::from_secs(5), Duration::from_millis(500));
        let mut sock = TcpStream::connect(handle.local_addr()).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let started = Instant::now();
        let mut closed = false;
        while started.elapsed() < Duration::from_secs(3) {
            if peer_closed(&mut sock) {
                closed = true;
                break;
            }
        }
        let elapsed = started.elapsed();
        drop(sock);
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
        assert!(closed, "server kept a silent connection open for {elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(2),
            "silent connection closed too late: {elapsed:?}"
        );
    }

    #[test]
    fn tls_valid_certificate_and_token_allow_sql_session() {
        let token = "valid-test-token-with-more-than-32-bytes";
        let (handle, db_path, wal_path) = start_tls_test_server("server-cert.pem", Some(token));
        let mut stream = trusted_tls_client(handle.local_addr()).unwrap();
        send_frame(&mut stream, PROTOCOL_VERSION.as_bytes());
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("MITRDB/1")
        );
        send_frame(&mut stream, format!("AUTH {token}").as_bytes());
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("\"authenticated\":true")
        );
        send_frame(&mut stream, b"PING");
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("PONG")
        );
        send_frame(&mut stream, b"QUIT");
        let _ = receive_frame(&mut stream);
        drop(stream);
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn tls_client_rejects_certificate_hostname_mismatch() {
        let (handle, db_path, wal_path) = start_tls_test_server(
            "server-cert.pem",
            Some("valid-test-token-with-more-than-32-bytes"),
        );
        let result = (|| -> io::Result<()> {
            // Drive the TLS handshake directly: the certificate error may surface
            // during the first I/O operation, before the MITRDB framing is sent.
            let mut stream =
                trusted_tls_client_for_name(handle.local_addr(), "wrong-hostname.invalid")?;
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock)?;
            }
            Ok(())
        })();
        assert!(
            result.is_err(),
            "TLS client must reject a trusted certificate for the wrong hostname"
        );
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn tls_client_rejects_expired_server_certificate() {
        let (handle, db_path, wal_path) = start_tls_test_server(
            "expired-cert.pem",
            Some("valid-test-token-with-more-than-32-bytes"),
        );
        let result = (|| -> io::Result<()> {
            // Complete the handshake explicitly so certificate validation errors
            // are observed as errors rather than panics in the framing helper.
            let mut stream = trusted_tls_client(handle.local_addr())?;
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock)?;
            }
            Ok(())
        })();
        assert!(
            result.is_err(),
            "trusted TLS client must reject the expired server certificate"
        );
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn tls_wrong_token_never_opens_sql_session() {
        let (handle, db_path, wal_path) = start_tls_test_server(
            "server-cert.pem",
            Some("correct-token-with-at-least-32-bytes"),
        );
        let mut stream = trusted_tls_client(handle.local_addr()).unwrap();
        send_frame(&mut stream, PROTOCOL_VERSION.as_bytes());
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("MITRDB/1")
        );
        send_frame(&mut stream, b"AUTH wrong-token-with-at-least-32-bytes");
        assert!(
            String::from_utf8(receive_frame(&mut stream))
                .unwrap()
                .contains("AUTH_FAILED")
        );
        send_frame(
            &mut stream,
            b"CREATE TABLE should_not_exist (id INT PRIMARY KEY)",
        );
        let mut header = [0u8; 4];
        let closed = stream.read_exact(&mut header).is_err();
        assert!(
            closed,
            "server must close the connection before any SQL response"
        );
        drop(stream);
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn remote_bind_requires_tls_and_authentication_independently() {
        let server = Arc::new(Server::new());
        let remote = SocketAddr::from(([0, 0, 0, 0], 0));
        let no_tls = TcpServerConfig {
            bind_addr: remote,
            auth_token: Some(Zeroizing::new(
                "valid-test-token-with-more-than-32-bytes".to_string(),
            )),
            ..TcpServerConfig::default()
        };
        assert!(
            server.bind_tcp(no_tls).is_err(),
            "remote plaintext must be rejected even with a token"
        );
        let no_auth = TcpServerConfig {
            bind_addr: remote,
            tls_cert_path: Some(fixture("server-cert.pem")),
            tls_key_path: Some(private_key_copy(&format!("noauth-{}", std::process::id()))),
            ..TcpServerConfig::default()
        };
        assert!(
            server.bind_tcp(no_auth).is_err(),
            "remote TLS must be rejected without authentication"
        );
    }

    #[test]
    fn malformed_tls_handshake_and_early_close_are_handled_safely() {
        let (handle, db_path, wal_path) = start_tls_test_server(
            "server-cert.pem",
            Some("valid-test-token-with-more-than-32-bytes"),
        );
        let mut malformed = TcpStream::connect(handle.local_addr()).unwrap();
        malformed
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        malformed
            .write_all(b"this is not a TLS ClientHello")
            .unwrap();
        let mut first = [0u8; 1];
        let result = malformed.read(&mut first);
        // A TLS implementation may send a fatal-alert record before closing.
        // Accept that alert, but require the peer to close promptly afterwards.
        match result {
            Ok(0) | Err(_) => {}
            Ok(1) => {
                assert_eq!(first[0], 0x15, "only a TLS alert may precede closure");
                let mut trailing = [0u8; 64];
                let mut total = 1usize;
                loop {
                    match malformed.read(&mut trailing) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            total += n;
                            assert!(
                                total <= 64,
                                "server must not keep malformed TLS connection open"
                            );
                        }
                    }
                }
            }
            Ok(_) => unreachable!("one-byte read cannot return more than one byte"),
        }
        drop(malformed);
        // Closing before sending a complete ClientHello must not panic the server worker.
        drop(TcpStream::connect(handle.local_addr()).unwrap());
        handle.shutdown().unwrap();
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn tcp_server_rejects_non_loopback_bind() {
        let server = Arc::new(Server::new());
        let config = TcpServerConfig {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 7432)),
            ..TcpServerConfig::default()
        };
        assert!(server.bind_tcp(config).is_err());
    }
}
