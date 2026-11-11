#![allow(clippy::chunks_exact_to_as_chunks, clippy::collapsible_if)]
// MITRDB is Linux-only (O_DIRECT/fsync/permission semantics are Linux-specific).
#[cfg(not(target_os = "linux"))]
compile_error!("MITRDB supports Linux only. Build on a Linux target.");

pub mod backup;
pub mod btree;
pub mod buffer_pool;
pub mod crc32c;
pub mod disk;
pub mod page;
pub mod recovery;
pub mod wal;

pub use backup::{BackupFileMeta, BackupManager, BackupManifest, RestoreReport};
pub use btree::{BPlusTree, BTreeNode, KeyBound, KeyRange};
pub use buffer_pool::{BufferPoolManager, Frame, WalCheckHook};
pub use disk::DiskManager;
pub use page::{
    PAGE_FLAG_FREE, PAGE_TYPE_BTREE_INTERNAL, PAGE_TYPE_BTREE_LEAF, PAGE_TYPE_RAW,
    PAGE_TYPE_SLOTTED, Page, PageHeader,
};
pub use recovery::{RecoveryManager, RecoveryReport};
pub use wal::{
    CheckpointMetadata, DEFAULT_WAL_SEGMENT_BYTES, LogRecord, LogRecordType, MIN_WAL_SEGMENT_BYTES,
    WalManager,
};
