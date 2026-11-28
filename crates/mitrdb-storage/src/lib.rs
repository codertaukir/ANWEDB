#![allow(clippy::chunks_exact_to_as_chunks, clippy::collapsible_if)]
// MITRDB is Linux-only (O_DIRECT/fsync/permission semantics are Linux-specific).
#[cfg(not(target_os = "linux"))]
compile_error!("MITRDB supports Linux only. Build on a Linux target.");

pub mod backup;
pub mod blob;
pub mod btree;
pub mod buffer_pool;
pub mod crc32c;
pub mod disk;
pub mod overflow;
pub mod page;
pub mod recovery;
pub mod wal;

pub use backup::{BackupFileMeta, BackupManager, BackupManifest, RestoreReport};
pub use btree::{BPlusTree, BTreeNode, KeyBound, KeyRange};
pub use buffer_pool::{BufferPoolManager, Frame, WalCheckHook};
pub use disk::DiskManager;
pub use page::{
    PAGE_FLAG_FREE, PAGE_TYPE_BTREE_INTERNAL, PAGE_TYPE_BTREE_LEAF, PAGE_TYPE_OVERFLOW,
    PAGE_TYPE_RAW, PAGE_TYPE_SLOTTED, Page, PageHeader,
};
pub use recovery::{RecoveryManager, RecoveryReport};
pub use wal::{
    CheckpointMetadata, DEFAULT_WAL_SEGMENT_BYTES, LogRecord, LogRecordType, MIN_WAL_SEGMENT_BYTES,
    WalManager,
};

pub use blob::{
    BlobFinished, BlobReader, BlobWriter, DEFAULT_MAX_BLOB_BYTES, MAX_READ_RANGE_BYTES,
    free_blob_pages,
};
pub use overflow::{
    INLINE_BLOB_MAX_BYTES, MAX_INLINE_TUPLE_BYTES, MAX_OVERFLOW_PAYLOAD_BYTES, NODE_KIND_OVERFLOW,
    OVERFLOW_CHUNK_CAPACITY, OVERFLOW_DESCRIPTOR_LEN, OVERFLOW_DESCRIPTOR_V2_LEN, OverflowDescriptor,
    OverflowDescriptorV2, TUPLE_TAG_BOOL, TUPLE_TAG_BYTES, TUPLE_TAG_FLOAT, TUPLE_TAG_INT64,
    TUPLE_TAG_NULL, TUPLE_TAG_OVERFLOW_BLOB, TUPLE_TAG_OVERFLOW_BYTES, TUPLE_TAG_OVERFLOW_TEXT,
    TUPLE_TAG_OVERFLOW_TUPLE, TUPLE_TAG_TEXT, collect_overflow_chain_pages,
    collect_tuple_overflow_heads, read_overflow_chain, stage_free_overflow_chain,
    write_overflow_chain,
};
