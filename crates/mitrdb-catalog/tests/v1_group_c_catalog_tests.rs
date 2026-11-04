use std::sync::Arc;

use mitrdb_catalog::{Catalog, ColumnDef, DataType, IndexMetadata, TableSchema};
use mitrdb_common::Error;
use mitrdb_storage::buffer_pool::BufferPoolManager;
use mitrdb_storage::disk::DiskManager;
use mitrdb_storage::recovery::RecoveryManager;
use mitrdb_storage::wal::WalManager;
use mitrdb_txn::TxnManager;

#[test]
fn test_schema_and_index_metadata_serialization_and_corruption() {
    let schema = TableSchema {
        table_id: 7,
        name: "accounts".to_string(),
        root_page_id: 42,
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_primary_key: true,
            },
            ColumnDef {
                name: "owner".to_string(),
                data_type: DataType::Text,
                nullable: false,
                is_primary_key: false,
            },
            ColumnDef {
                name: "active".to_string(),
                data_type: DataType::Bool,
                nullable: true,
                is_primary_key: false,
            },
            ColumnDef {
                name: "payload".to_string(),
                data_type: DataType::Bytes,
                nullable: true,
                is_primary_key: false,
            },
        ],
    };

    let encoded = schema.serialize();
    let decoded = TableSchema::deserialize(&encoded).unwrap();
    assert_eq!(decoded, schema);

    for cut in 0..encoded.len() {
        assert!(matches!(
            TableSchema::deserialize(&encoded[..cut]),
            Err(Error::PageCorrupted(_))
        ));
    }

    let idx = IndexMetadata {
        index_id: 19,
        name: "idx_accounts_owner".to_string(),
        table_id: 7,
        column_name: "owner".to_string(),
        root_page_id: 99,
        unique: true,
    };
    let idx_encoded = idx.serialize();
    let idx_decoded = IndexMetadata::deserialize(&idx_encoded).unwrap();
    assert_eq!(idx_decoded, idx);

    for cut in 0..idx_encoded.len() {
        assert!(matches!(
            IndexMetadata::deserialize(&idx_encoded[..cut]),
            Err(Error::PageCorrupted(_))
        ));
    }
}

#[test]
fn test_catalog_root_splits_and_uncheckpointed_wal_recovery() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_c_cat_split_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));

        for i in 1..=22 {
            let tname = format!("table_entity_number_{:03}", i);
            let cols = vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: format!("col_text_attribute_{:03}", i),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
            ];
            catalog.create_table(&tname, cols).unwrap();
            let iname = format!("idx_entity_number_{:03}", i);
            let cname = format!("col_text_attribute_{:03}", i);
            catalog.create_index(&iname, &tname, &cname, true).unwrap();
        }

        let records = wal.read_all_records().unwrap();
        assert!(!records.is_empty());
        assert!(records.iter().all(|r| r.txn_id >= 1_000_000_001));
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());

        let report = recovery.recover().unwrap();
        assert!(report.aborted_txns.is_empty());
        assert!(report.committed_txns.len() >= 44);

        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());
        let first_user_tid = txn_mgr.begin().unwrap();
        assert_eq!(first_user_tid, 1);
        txn_mgr.commit(first_user_tid).unwrap();

        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));
        for i in 1..=22 {
            let tname = format!("table_entity_number_{:03}", i);
            let schema = catalog
                .get_table_schema(&tname)
                .unwrap()
                .expect("table schema must survive catalog root split and recovery");
            assert_eq!(schema.name, tname);
            assert!(catalog.table_tree(schema.table_id).is_some());

            let indexes = catalog.get_table_indexes(schema.table_id);
            assert_eq!(indexes.len(), 1);
            assert!(indexes[0].unique);
            assert!(catalog.index_tree(indexes[0].index_id).is_some());
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_table_and_index_root_resolution_after_split_and_sync() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitrdb_group_c_roots_{}", std::process::id()));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let initial_tbl_root: u32;
    let initial_idx_root: u32;
    let post_split_tbl_root: u32;
    let post_split_idx_root: u32;

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));
        let txn_mgr = TxnManager::with_storage(wal.clone(), bpm.clone());

        let tid_table = catalog
            .create_table(
                "items",
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_primary_key: true,
                    },
                    ColumnDef {
                        name: "code".to_string(),
                        data_type: DataType::Text,
                        nullable: false,
                        is_primary_key: false,
                    },
                ],
            )
            .unwrap();
        let iid = catalog
            .create_index("idx_items_code", "items", "code", true)
            .unwrap();

        let tbl_tree = catalog.table_tree(tid_table).unwrap();
        let idx_tree = catalog.index_tree(iid).unwrap();
        initial_tbl_root = tbl_tree.root_page_id();
        initial_idx_root = idx_tree.root_page_id();

        let txn = txn_mgr.begin().unwrap();
        let big_pad = "X".repeat(220);
        for i in 1..=35 {
            let k = format!("pk_{:04}", i);
            let v = format!("row_payload_{:04}_{}", i, big_pad);
            let ik = format!("code_{:04}_{}", i, big_pad);
            txn_mgr
                .put(txn, &tbl_tree, k.as_bytes(), v.as_bytes())
                .unwrap();
            txn_mgr
                .put(txn, &idx_tree, ik.as_bytes(), k.as_bytes())
                .unwrap();
        }
        txn_mgr.commit(txn).unwrap();

        post_split_tbl_root = tbl_tree.root_page_id();
        post_split_idx_root = idx_tree.root_page_id();
        assert_ne!(post_split_tbl_root, initial_tbl_root);
        assert_ne!(post_split_idx_root, initial_idx_root);

        bpm.flush_all().unwrap();
    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
        recovery.recover().unwrap();

        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));
        let schema = catalog.get_table_schema("items").unwrap().unwrap();
        assert_eq!(schema.root_page_id, post_split_tbl_root);
        let tbl_tree = catalog.table_tree(schema.table_id).unwrap();
        assert_eq!(tbl_tree.root_page_id(), post_split_tbl_root);
        assert_eq!(tbl_tree.range_scan(None, None).unwrap().len(), 35);

        let indexes = catalog.get_table_indexes(schema.table_id);
        assert_eq!(indexes[0].root_page_id, post_split_idx_root);
        let idx_tree = catalog.index_tree(indexes[0].index_id).unwrap();
        assert_eq!(idx_tree.root_page_id(), post_split_idx_root);
        assert_eq!(idx_tree.range_scan(None, None).unwrap().len(), 35);

        catalog.sync_roots().unwrap();
        bpm.flush_all().unwrap();
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_populate_index_entries_logged_durability_across_crash_recovery() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mitrdb_group_c_populate_idx_logged_{}",
        std::process::id()
    ));
    let db_path = temp_dir.join("test.db");
    let wal_path = temp_dir.join("test.wal");
    let _ = std::fs::remove_dir_all(&temp_dir);

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));

        catalog
            .create_table(
                "events",
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_primary_key: true,
                    },
                    ColumnDef {
                        name: "tag".to_string(),
                        data_type: DataType::Text,
                        nullable: false,
                        is_primary_key: false,
                    },
                ],
            )
            .unwrap();
        let idx_id = catalog
            .create_index("idx_events_tag", "events", "tag", true)
            .unwrap();

        let mut entries = Vec::new();
        for i in 1..=20u32 {
            let k = format!("tag_key_{:04}_{}", i, "P".repeat(160)).into_bytes();
            let v = i.to_be_bytes().to_vec();
            entries.push((k, v));
        }
        catalog.populate_index_entries(idx_id, &entries).unwrap();

    }

    {
        let disk = Arc::new(DiskManager::open_or_create(db_path.as_path()).unwrap());
        let wal = Arc::new(WalManager::open_or_create(wal_path.as_path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(disk.clone(), 64));
        let recovery = RecoveryManager::new(wal.clone(), bpm.clone());
        let report = recovery.recover().unwrap();
        assert!(report.aborted_txns.is_empty());

        let catalog = Catalog::with_storage(bpm.clone(), Some(wal.clone()));
        let idx_meta = catalog.get_index_by_name("idx_events_tag").unwrap();
        let idx_tree = catalog.index_tree(idx_meta.index_id).unwrap();
        assert_eq!(idx_tree.range_scan(None, None).unwrap().len(), 20);

    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
