pub type PageId = u32;
pub type Lsn = u64;
pub type TxnId = u64;

pub const INVALID_LSN: Lsn = 0;
pub const INVALID_PAGE_ID: PageId = u32::MAX;
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 32;
pub const MAGIC_BYTES: u32 = 0x4D495452;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Error {
    PageCorrupted(String),
    InvalidPageId(PageId),
    BufferPoolExhausted,
    WriteAheadViolation(String),
    WalCorrupted(String),
    Io(String),
    CrashRecovery(String),
    SlotOutOfBounds(usize),
    OutOfSpace,
    LockTimeout,
    Deadlock,
    WriteConflict(String),
    TransactionError(String),
    BackupError(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::PageCorrupted(msg) => write!(f, "PAGE_CORRUPTED: {}", msg),
            Error::InvalidPageId(id) => write!(f, "INVALID_PAGE_ID: {}", id),
            Error::BufferPoolExhausted => write!(f, "BUFFER_POOL_EXHAUSTED"),
            Error::WriteAheadViolation(msg) => write!(f, "WRITE_AHEAD_VIOLATION: {}", msg),
            Error::WalCorrupted(msg) => write!(f, "WAL_CORRUPTED: {}", msg),
            Error::Io(msg) => write!(f, "IO_ERROR: {}", msg),
            Error::CrashRecovery(msg) => write!(f, "CRASH_RECOVERY_ERROR: {}", msg),
            Error::SlotOutOfBounds(slot) => write!(f, "SLOT_OUT_OF_BOUNDS: {}", slot),
            Error::OutOfSpace => write!(f, "OUT_OF_SPACE"),
            Error::LockTimeout => write!(f, "LOCK_TIMEOUT"),
            Error::Deadlock => write!(f, "DEADLOCK"),
            Error::WriteConflict(msg) => write!(f, "WRITE_CONFLICT: {}", msg),
            Error::TransactionError(msg) => write!(f, "TRANSACTION_ERROR: {}", msg),
            Error::BackupError(msg) => write!(f, "BACKUP_ERROR: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

