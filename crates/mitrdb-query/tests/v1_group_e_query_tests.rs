use std::sync::Arc;

use mitrdb_catalog::{Catalog, ColumnDef, DataType};
use mitrdb_common::Error;
use mitrdb_query::{CompareOp, PhysicalPlan, QueryEngine, Statement, Value};

#[test]
fn test_value_key_bytes_ordering() {
    let vals = [
        Value::Int64(i64::MIN),
        Value::Int64(-10_000),
        Value::Int64(-1),
        Value::Int64(0),
        Value::Int64(1),
        Value::Int64(10_000),
        Value::Int64(i64::MAX),
    ];
    for w in vals.windows(2) {
        assert!(w[0].to_key_bytes() < w[1].to_key_bytes());
    }
    assert!(Value::Bool(false).to_key_bytes() < Value::Bool(true).to_key_bytes());
    assert!(
        Value::Text("alpha".to_string()).to_key_bytes()
            < Value::Text("beta".to_string()).to_key_bytes()
    );
}

#[test]
fn test_quoted_literals_commas_keywords_and_escaped_quotes() {
    let catalog = Arc::new(Catalog::new());
    let engine = QueryEngine::new(catalog.clone());

    engine
        .parse_and_plan("CREATE TABLE notes (id INT PRIMARY KEY, title TEXT, body TEXT, flag BOOL)")
        .unwrap();
    catalog
        .create_table(
            "notes",
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "title".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnDef {
                    name: "body".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnDef {
                    name: "flag".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        )
        .unwrap();

    let ins_stmt = engine
        .parse(
            "INSERT INTO notes VALUES (1, 'hello, world, WHERE SET VALUES', \"SELECT FROM ORDER BY, LIMIT\", true)",
        )
        .unwrap();
    match ins_stmt {
        Statement::Insert { table_name, values } => {
            assert_eq!(table_name, "notes");
            assert_eq!(
                values,
                vec![
                    Value::Int64(1),
                    Value::Text("hello, world, WHERE SET VALUES".to_string()),
                    Value::Text("SELECT FROM ORDER BY, LIMIT".to_string()),
                    Value::Bool(true),
                ]
            );
        }
        other => panic!("Unexpected statement: {:?}", other),
    }

    let escaped_stmt = engine
        .parse(
            "INSERT INTO notes VALUES (2, 'it''s ''quoted''', \"a \"\"double\"\" quote\", false)",
        )
        .unwrap();
    match escaped_stmt {
        Statement::Insert { values, .. } => {
            assert_eq!(values[1], Value::Text("it's 'quoted'".to_string()));
            assert_eq!(values[2], Value::Text("a \"double\" quote".to_string()));
        }
        other => panic!("Unexpected statement: {:?}", other),
    }

    let upd_plan = engine
        .parse_and_plan(
            "UPDATE notes SET title = 'SET x = 1, WHERE id = 9', body = \"comma, inside\" WHERE title = 'FROM WHERE ORDER BY'",
        )
        .unwrap();
    match upd_plan {
        PhysicalPlan::Update {
            assignments,
            filter,
            ..
        } => {
            assert_eq!(
                assignments,
                vec![
                    (1, Value::Text("SET x = 1, WHERE id = 9".to_string())),
                    (2, Value::Text("comma, inside".to_string())),
                ]
            );
            let bf = filter.unwrap();
            assert_eq!(bf.column_idx, 1);
            assert_eq!(bf.op, CompareOp::Eq);
            assert_eq!(bf.value, Value::Text("FROM WHERE ORDER BY".to_string()));
        }
        other => panic!("Unexpected plan: {:?}", other),
    }

    let sel_plan = engine
        .parse_and_plan(
            "SELECT id, title FROM notes WHERE body = 'LIMIT 10 ORDER BY id DESC' ORDER BY id DESC LIMIT 5",
        )
        .unwrap();
    match sel_plan {
        PhysicalPlan::SeqScan {
            projection,
            filter,
            order_by,
            limit,
            ..
        } => {
            assert_eq!(projection, vec![0, 1]);
            assert_eq!(
                filter.unwrap().value,
                Value::Text("LIMIT 10 ORDER BY id DESC".to_string())
            );
            assert_eq!(order_by, Some((0, true)));
            assert_eq!(limit, Some(5));
        }
        other => panic!("Unexpected plan: {:?}", other),
    }
}

#[test]
fn test_malformed_quotes_and_invalid_sql_rejection() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        )
        .unwrap();
    let engine = QueryEngine::new(catalog);

    let invalid_cases = [
        "INSERT INTO t VALUES (1, 'unclosed single quote)",
        "INSERT INTO t VALUES (1, \"unclosed double quote)",
        "INSERT INTO t VALUES (1, ')",
        "INSERT INTO t VALUES (1, \")",
        "INSERT INTO t VALUES (1, 'unescaped'interior')",
        "INSERT INTO t VALUES (1, \"unescaped\"interior\")",
        "SELECT * FROM unknown_table",
        "SELECT unknown_col FROM t",
        "INSERT INTO t VALUES (1, 'ok', 'extra_col')",
        "UPDATE t SET name = 'x' WHERE unknown_col = 1",
        "SELECT * FROM t LIMIT not_a_number",
        "INVALID COMMAND",
    ];

    for sql in invalid_cases {
        let res = engine.parse_and_plan(sql);
        assert!(
            matches!(res, Err(Error::TransactionError(_))),
            "Expected TransactionError for SQL: {}",
            sql
        );
    }
}

#[test]
fn test_select_order_by_limit_and_explain_index_selection() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "users",
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        )
        .unwrap();
    let engine = QueryEngine::new(catalog.clone());

    let plan_before_idx = engine
        .parse_and_plan("SELECT * FROM users WHERE email = 'a@b.c'")
        .unwrap();
    assert!(matches!(plan_before_idx, PhysicalPlan::SeqScan { .. }));

    catalog
        .create_index("idx_users_email", "users", "email", true)
        .unwrap();

    let plan_after_idx = engine
        .parse_and_plan("SELECT id, email FROM users WHERE email = 'a@b.c' ORDER BY id ASC LIMIT 3")
        .unwrap();
    match plan_after_idx {
        PhysicalPlan::IndexScan {
            index,
            lookup_value,
            projection,
            order_by,
            limit,
            ..
        } => {
            assert_eq!(index.name, "idx_users_email");
            assert_eq!(lookup_value, Value::Text("a@b.c".to_string()));
            assert_eq!(projection, vec![0, 1]);
            assert_eq!(order_by, Some((0, false)));
            assert_eq!(limit, Some(3));
        }
        other => panic!("Expected IndexScan, got {:?}", other),
    }

    let range_plan = engine
        .parse_and_plan("SELECT * FROM users WHERE email > 'a@b.c'")
        .unwrap();
    assert!(matches!(range_plan, PhysicalPlan::IndexRangeScan { .. }));

    let explain_plan = engine
        .parse_and_plan("EXPLAIN SELECT * FROM users WHERE email = 'a@b.c'")
        .unwrap();
    assert!(matches!(explain_plan, PhysicalPlan::Explain(_)));
}

#[test]
fn test_union_named_column_order_by_and_schema_validation() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "users",
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        )
        .unwrap();
    let engine = QueryEngine::new(catalog);

    let union_plan = engine
        .parse_and_plan(
            "SELECT id, email FROM users UNION SELECT id, email FROM users ORDER BY email DESC, id ASC",
        )
        .unwrap();
    match union_plan {
        PhysicalPlan::Union { order_by_list, .. } => {
            assert_eq!(order_by_list, vec![(1, true), (0, false)]);
        }
        other => panic!("Expected Union plan, got {:?}", other),
    }

    assert!(
        engine
            .parse_and_plan(
                "SELECT id, email FROM users UNION SELECT id, email FROM users ORDER BY missing_col ASC"
            )
            .is_err()
    );
    assert!(
        engine
            .parse_and_plan("INSERT INTO users VALUES ('not_an_int', 'a@b.c')")
            .is_err()
    );
    assert!(
        engine
            .parse_and_plan("INSERT INTO users VALUES (1, NULL)")
            .is_err()
    );
    assert!(
        engine
            .parse_and_plan("UPDATE users SET email = 999 WHERE id = 1")
            .is_err()
    );
    assert!(
        engine
            .parse_and_plan("UPDATE users SET email = NULL WHERE id = 1")
            .is_err()
    );
}
