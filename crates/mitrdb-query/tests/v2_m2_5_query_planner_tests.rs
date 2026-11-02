use std::sync::Arc;

use mitrdb_catalog::{Catalog, ColumnDef, DataType};
use mitrdb_common::Error;
use mitrdb_query::{
    AggregateFunc, BoundPredicate, KeyBound, KeyRange, PhysicalPlan, QueryEngine, Value,
};

fn setup_items_catalog(with_indexes: bool) -> (Arc<Catalog>, QueryEngine) {
    let catalog = Arc::new(Catalog::new());
    catalog
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
                    name: "category".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_primary_key: false,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_primary_key: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                },
                ColumnDef {
                    name: "active".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
        )
        .unwrap();

    if with_indexes {
        catalog
            .create_index("idx_items_category", "items", "category", false)
            .unwrap();
        catalog
            .create_index("idx_items_price", "items", "price", false)
            .unwrap();
    }

    let engine = QueryEngine::new(catalog.clone());
    (catalog, engine)
}

#[test]
fn m2_5_planner_selects_index_range_scan_for_comparison_operators() {
    let (_cat_no_idx, engine_no_idx) = setup_items_catalog(false);
    let (_cat, engine) = setup_items_catalog(true);

    // Without index, range predicates fall back to SeqScan
    let fallback_plan = engine_no_idx
        .parse_and_plan("SELECT * FROM items WHERE price >= 100")
        .unwrap();
    assert!(matches!(fallback_plan, PhysicalPlan::SeqScan { .. }));

    // Equality uses IndexScan
    let eq_plan = engine
        .parse_and_plan("SELECT * FROM items WHERE price = 100")
        .unwrap();
    assert!(matches!(eq_plan, PhysicalPlan::IndexScan { .. }));

    // >, >=, <, <= on indexed column select IndexRangeScan with exact KeyBounds
    let gt_plan = engine
        .parse_and_plan("SELECT id, price FROM items WHERE price > 100")
        .unwrap();
    match gt_plan {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::from_exclusive(Value::Int64(100).to_key_bytes())]
            );
        }
        other => panic!("Expected IndexRangeScan for >, got {:?}", other),
    }

    let ge_plan = engine
        .parse_and_plan("SELECT id, price FROM items WHERE price >= 100")
        .unwrap();
    match ge_plan {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::from_inclusive(Value::Int64(100).to_key_bytes())]
            );
        }
        other => panic!("Expected IndexRangeScan for >=, got {:?}", other),
    }

    let lt_plan = engine
        .parse_and_plan("SELECT id, price FROM items WHERE price < 500")
        .unwrap();
    match lt_plan {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::to_exclusive(Value::Int64(500).to_key_bytes())]
            );
        }
        other => panic!("Expected IndexRangeScan for <, got {:?}", other),
    }

    let le_plan = engine
        .parse_and_plan("SELECT id, price FROM items WHERE price <= 500")
        .unwrap();
    match le_plan {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::to_inclusive(Value::Int64(500).to_key_bytes())]
            );
        }
        other => panic!("Expected IndexRangeScan for <=, got {:?}", other),
    }

    // Unindexed column falls back to SeqScan even when other indexes exist
    let unindexed_plan = engine
        .parse_and_plan("SELECT * FROM items WHERE name >= 'm'")
        .unwrap();
    assert!(matches!(unindexed_plan, PhysicalPlan::SeqScan { .. }));
}

#[test]
fn m2_5_planner_compound_and_or_precedence_and_range_combining() {
    let (_cat, engine) = setup_items_catalog(true);

    // 1. Bounded range via AND: price >= 100 AND price < 500
    let bounded_and = engine
        .parse_and_plan("SELECT * FROM items WHERE price >= 100 AND price < 500")
        .unwrap();
    match bounded_and {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::new(
                    KeyBound::Included(Value::Int64(100).to_key_bytes()),
                    KeyBound::Excluded(Value::Int64(500).to_key_bytes()),
                )]
            );
        }
        other => panic!("Expected IndexRangeScan, got {:?}", other),
    }

    // 2. Empty range via contradictory AND: price > 500 AND price < 100
    let empty_and = engine
        .parse_and_plan("SELECT * FROM items WHERE price > 500 AND price < 100")
        .unwrap();
    match empty_and {
        PhysicalPlan::IndexRangeScan { ranges, .. } => {
            assert_eq!(ranges.len(), 1);
            assert!(ranges[0].is_empty());
        }
        other => panic!("Expected IndexRangeScan with empty range, got {:?}", other),
    }

    // 3. Disjoint OR on indexed column: category = 10 OR category = 20
    let or_plan = engine
        .parse_and_plan("SELECT * FROM items WHERE category = 10 OR category = 20")
        .unwrap();
    match or_plan {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_category");
            assert_eq!(
                ranges,
                vec![
                    KeyRange::point(Value::Int64(10).to_key_bytes()),
                    KeyRange::point(Value::Int64(20).to_key_bytes()),
                ]
            );
        }
        other => panic!("Expected IndexRangeScan for OR, got {:?}", other),
    }

    // 4. Overlapping OR ranges are merged into a single normalized range: [100, 300] OR [200, 400] -> [100, 400]
    let overlapping_or = engine
        .parse_and_plan(
            "SELECT * FROM items WHERE (price >= 100 AND price <= 300) OR (price >= 200 AND price <= 400)",
        )
        .unwrap();
    match overlapping_or {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::inclusive(
                    Value::Int64(100).to_key_bytes(),
                    Value::Int64(400).to_key_bytes(),
                )]
            );
        }
        other => panic!("Expected merged IndexRangeScan, got {:?}", other),
    }

    // 5. Mixed indexed and unindexed in AND pushes down indexed range: price >= 100 AND name = 'widget'
    let mixed_and = engine
        .parse_and_plan("SELECT * FROM items WHERE price >= 100 AND name = 'widget'")
        .unwrap();
    match mixed_and {
        PhysicalPlan::IndexRangeScan { index, ranges, .. } => {
            assert_eq!(index.name, "idx_items_price");
            assert_eq!(
                ranges,
                vec![KeyRange::from_inclusive(Value::Int64(100).to_key_bytes())]
            );
        }
        other => panic!(
            "Expected IndexRangeScan pushdown for mixed AND, got {:?}",
            other
        ),
    }

    // 6. Mixed indexed and unindexed in OR must fall back to SeqScan: price >= 100 OR name = 'widget'
    let mixed_or = engine
        .parse_and_plan("SELECT * FROM items WHERE price >= 100 OR name = 'widget'")
        .unwrap();
    assert!(matches!(mixed_or, PhysicalPlan::SeqScan { .. }));

    // 7. Operator precedence: A OR B AND C parses as Or(A, And(B, C)), while (A OR B) AND C parses as And(Or(A, B), C)
    let prec_plan = engine
        .parse_and_plan("SELECT * FROM items WHERE name = 'a' OR name = 'b' AND active = true")
        .unwrap();
    if let PhysicalPlan::SeqScan {
        filter: Some(bf), ..
    } = prec_plan
    {
        assert!(matches!(
            bf.predicate,
            BoundPredicate::Or(_, ref r) if matches!(**r, BoundPredicate::And(_, _))
        ));
    } else {
        panic!("Expected SeqScan with Or(A, And(B, C))");
    }

    let paren_plan = engine
        .parse_and_plan("SELECT * FROM items WHERE (name = 'a' OR name = 'b') AND price >= 100")
        .unwrap();
    if let PhysicalPlan::IndexRangeScan {
        index,
        filter: Some(bf),
        ..
    } = paren_plan
    {
        assert_eq!(index.name, "idx_items_price");
        assert!(matches!(
            bf.predicate,
            BoundPredicate::And(ref l, _) if matches!(**l, BoundPredicate::Or(_, _))
        ));
    } else {
        panic!("Expected IndexRangeScan with And(Or(A, B), C)");
    }
}

#[test]
fn m2_5_planner_aggregates_and_invalid_sql_rejection() {
    let (_cat, engine) = setup_items_catalog(true);

    let agg_plan = engine
        .parse_and_plan(
            "SELECT COUNT(*), COUNT(name), SUM(price), MIN(price), MAX(name), AVG(price) FROM items WHERE price >= 100 AND price <= 500",
        )
        .unwrap();
    match agg_plan {
        PhysicalPlan::Aggregate {
            input, aggregates, ..
        } => {
            assert!(matches!(*input, PhysicalPlan::IndexRangeScan { .. }));
            let funcs: Vec<AggregateFunc> = aggregates.iter().map(|a| a.func).collect();
            assert_eq!(
                funcs,
                vec![
                    AggregateFunc::CountStar,
                    AggregateFunc::Count,
                    AggregateFunc::Sum,
                    AggregateFunc::Min,
                    AggregateFunc::Max,
                    AggregateFunc::Avg,
                ]
            );
        }
        other => panic!("Expected Aggregate over IndexRangeScan, got {:?}", other),
    }

    // Invalid expressions, type mismatches, and unsupported aggregate usages must be rejected
    let invalid_queries = [
        "SELECT * FROM items WHERE price > 'not_an_int'",
        "SELECT * FROM items WHERE name = 12345",
        "SELECT * FROM items WHERE active = 1",
        "SELECT * FROM items WHERE (price >= 100",
        "SELECT * FROM items WHERE price >= 100)",
        "SELECT * FROM items WHERE ()",
        "SELECT * FROM items WHERE price >= 100 AND",
        "SELECT * FROM items WHERE OR price >= 100",
        "SELECT * FROM items WHERE price = 100 = 200",
        "SELECT SUM(name) FROM items",
        "SELECT AVG(name) FROM items",
        "SELECT SUM(active) FROM items",
        "SELECT AVG(active) FROM items",
        "SELECT MIN(active) FROM items",
        "SELECT MAX(active) FROM items",
        "SELECT SUM(*) FROM items",
        "SELECT AVG(*) FROM items",
        "SELECT MIN(*) FROM items",
        "SELECT COUNT() FROM items",
        "SELECT MEDIAN(price) FROM items",
        "SELECT id, COUNT(*) FROM items",
        "SELECT category, name, COUNT(*) FROM items GROUP BY category",
    ];

    for sql in invalid_queries {
        let res = engine.parse_and_plan(sql);
        assert!(
            matches!(res, Err(Error::TransactionError(_))),
            "Expected TransactionError for invalid query: {}",
            sql
        );
    }
}
