use std::cmp::Ordering;
use std::sync::Arc;

pub use mitrdb_catalog::{
    Catalog, ColumnDef, DataType, IndexMetadata, KeyBound, KeyRange, TableSchema,
};
use mitrdb_common::{Error, Result};

thread_local! {
    static PARAMS_ALLOWED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct ParamsGuard(bool);

impl ParamsGuard {
    fn enable() -> Self {
        ParamsGuard(PARAMS_ALLOWED.with(|f| f.replace(true)))
    }
}

impl Drop for ParamsGuard {
    fn drop(&mut self) {
        PARAMS_ALLOWED.with(|f| f.set(self.0));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int64(i64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
    /// Placeholder `$n` (stored 0-based). Exists only between parse and bind.
    Param(usize),
}

impl Value {
    pub fn to_key_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![0],
            Value::Int64(v) => {
                let mut out = vec![1];
                let biased = (*v as u64) ^ (1u64 << 63);
                out.extend_from_slice(&biased.to_be_bytes());
                out
            }
            Value::Text(s) => {
                let mut out = vec![2];
                out.extend_from_slice(s.as_bytes());
                out
            }
            Value::Bool(b) => vec![3, u8::from(*b)],
            Value::Bytes(b) => {
                let mut out = vec![4];
                out.extend_from_slice(b);
                out
            }
            Value::Param(_) => vec![255],
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_key_bytes().cmp(&other.to_key_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    pub fn evaluate(self, left: &Value, right: &Value) -> bool {
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return false;
        }
        match self {
            CompareOp::Eq => left == right,
            CompareOp::Ne => left != right,
            CompareOp::Lt => left < right,
            CompareOp::Le => left <= right,
            CompareOp::Gt => left > right,
            CompareOp::Ge => left >= right,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(String),
    Literal(Value),
    Eq(Box<Expr>, Box<Expr>),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundPredicate {
    Comparison {
        column_idx: usize,
        op: CompareOp,
        value: Value,
    },
    And(Box<BoundPredicate>, Box<BoundPredicate>),
    Or(Box<BoundPredicate>, Box<BoundPredicate>),
    IsNull(usize),
    IsNotNull(usize),
}

impl BoundPredicate {
    pub fn eval_three_valued(&self, tuple: &[Value]) -> TruthValue {
        match self {
            BoundPredicate::Comparison {
                column_idx,
                op,
                value,
            } => {
                let col_val = &tuple[*column_idx];
                if matches!(col_val, Value::Null) || matches!(value, Value::Null) {
                    TruthValue::Unknown
                } else if op.evaluate(col_val, value) {
                    TruthValue::True
                } else {
                    TruthValue::False
                }
            }
            BoundPredicate::And(left, right) => {
                match (
                    left.eval_three_valued(tuple),
                    right.eval_three_valued(tuple),
                ) {
                    (TruthValue::False, _) | (_, TruthValue::False) => TruthValue::False,
                    (TruthValue::True, TruthValue::True) => TruthValue::True,
                    _ => TruthValue::Unknown,
                }
            }
            BoundPredicate::Or(left, right) => {
                match (
                    left.eval_three_valued(tuple),
                    right.eval_three_valued(tuple),
                ) {
                    (TruthValue::True, _) | (_, TruthValue::True) => TruthValue::True,
                    (TruthValue::False, TruthValue::False) => TruthValue::False,
                    _ => TruthValue::Unknown,
                }
            }
            BoundPredicate::IsNull(column_idx) => {
                if matches!(tuple[*column_idx], Value::Null) {
                    TruthValue::True
                } else {
                    TruthValue::False
                }
            }
            BoundPredicate::IsNotNull(column_idx) => {
                if matches!(tuple[*column_idx], Value::Null) {
                    TruthValue::False
                } else {
                    TruthValue::True
                }
            }
        }
    }

    pub fn evaluate(&self, tuple: &[Value]) -> bool {
        matches!(self.eval_three_valued(tuple), TruthValue::True)
    }

    fn first_comparison(&self) -> (usize, CompareOp, Value) {
        match self {
            BoundPredicate::Comparison {
                column_idx,
                op,
                value,
            } => (*column_idx, *op, value.clone()),
            BoundPredicate::And(left, _) | BoundPredicate::Or(left, _) => left.first_comparison(),
            BoundPredicate::IsNull(idx) => (*idx, CompareOp::Eq, Value::Null),
            BoundPredicate::IsNotNull(idx) => (*idx, CompareOp::Ne, Value::Null),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundFilter {
    pub column_idx: usize,
    pub op: CompareOp,
    pub value: Value,
    pub predicate: BoundPredicate,
}

impl BoundFilter {
    pub fn from_predicate(predicate: BoundPredicate) -> Self {
        let (column_idx, op, value) = predicate.first_comparison();
        BoundFilter {
            column_idx,
            op,
            value,
            predicate,
        }
    }

    pub fn evaluate(&self, tuple: &[Value]) -> bool {
        self.predicate.evaluate(tuple)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAggregate {
    pub func: AggregateFunc,
    pub column_idx: Option<usize>,
    pub output_name: String,
}

impl BoundAggregate {
    pub fn same_metric(&self, other: &BoundAggregate) -> bool {
        self.func == other.func && self.column_idx == other.column_idx
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundSelectItem {
    Column {
        column_idx: usize,
        output_name: String,
    },
    Aggregate(BoundAggregate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundGroupTarget {
    GroupColumn(usize),
    Aggregate(BoundAggregate),
}

impl BoundGroupTarget {
    pub fn resolve_value<'a>(
        &self,
        group_cols: &[usize],
        group_key: &'a [Value],
        aggregates: &[BoundAggregate],
        agg_values: &'a [Value],
    ) -> &'a Value {
        match self {
            BoundGroupTarget::GroupColumn(col_idx) => {
                let pos = group_cols
                    .iter()
                    .position(|c| c == col_idx)
                    .expect("Validated GROUP BY column index must exist in group_cols");
                &group_key[pos]
            }
            BoundGroupTarget::Aggregate(target_agg) => {
                let pos = aggregates
                    .iter()
                    .position(|a| a.same_metric(target_agg))
                    .expect("Validated aggregate must exist in aggregates list");
                &agg_values[pos]
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundHavingPredicate {
    Comparison {
        target: BoundGroupTarget,
        op: CompareOp,
        value: Value,
    },
    And(Box<BoundHavingPredicate>, Box<BoundHavingPredicate>),
    Or(Box<BoundHavingPredicate>, Box<BoundHavingPredicate>),
    IsNull(BoundGroupTarget),
    IsNotNull(BoundGroupTarget),
}

impl BoundHavingPredicate {
    pub fn eval_three_valued(
        &self,
        group_cols: &[usize],
        group_key: &[Value],
        aggregates: &[BoundAggregate],
        agg_values: &[Value],
    ) -> TruthValue {
        match self {
            BoundHavingPredicate::Comparison { target, op, value } => {
                let actual = target.resolve_value(group_cols, group_key, aggregates, agg_values);
                if matches!(actual, Value::Null) || matches!(value, Value::Null) {
                    TruthValue::Unknown
                } else if op.evaluate(actual, value) {
                    TruthValue::True
                } else {
                    TruthValue::False
                }
            }
            BoundHavingPredicate::And(left, right) => {
                match (
                    left.eval_three_valued(group_cols, group_key, aggregates, agg_values),
                    right.eval_three_valued(group_cols, group_key, aggregates, agg_values),
                ) {
                    (TruthValue::False, _) | (_, TruthValue::False) => TruthValue::False,
                    (TruthValue::True, TruthValue::True) => TruthValue::True,
                    _ => TruthValue::Unknown,
                }
            }
            BoundHavingPredicate::Or(left, right) => {
                match (
                    left.eval_three_valued(group_cols, group_key, aggregates, agg_values),
                    right.eval_three_valued(group_cols, group_key, aggregates, agg_values),
                ) {
                    (TruthValue::True, _) | (_, TruthValue::True) => TruthValue::True,
                    (TruthValue::False, TruthValue::False) => TruthValue::False,
                    _ => TruthValue::Unknown,
                }
            }
            BoundHavingPredicate::IsNull(target) => {
                let actual = target.resolve_value(group_cols, group_key, aggregates, agg_values);
                if matches!(actual, Value::Null) {
                    TruthValue::True
                } else {
                    TruthValue::False
                }
            }
            BoundHavingPredicate::IsNotNull(target) => {
                let actual = target.resolve_value(group_cols, group_key, aggregates, agg_values);
                if matches!(actual, Value::Null) {
                    TruthValue::False
                } else {
                    TruthValue::True
                }
            }
        }
    }

    pub fn evaluate(
        &self,
        group_cols: &[usize],
        group_key: &[Value],
        aggregates: &[BoundAggregate],
        agg_values: &[Value],
    ) -> bool {
        matches!(
            self.eval_three_valued(group_cols, group_key, aggregates, agg_values),
            TruthValue::True
        )
    }

    pub fn collect_aggregates(&self, out: &mut Vec<BoundAggregate>) {
        match self {
            BoundHavingPredicate::Comparison { target, .. }
            | BoundHavingPredicate::IsNull(target)
            | BoundHavingPredicate::IsNotNull(target) => {
                if let BoundGroupTarget::Aggregate(agg) = target
                    && !out.iter().any(|a| a.same_metric(agg))
                {
                    out.push(agg.clone());
                }
            }
            BoundHavingPredicate::And(l, r) | BoundHavingPredicate::Or(l, r) => {
                l.collect_aggregates(out);
                r.collect_aggregates(out);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundGroupOrderBy {
    pub target: BoundGroupTarget,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOn {
    pub left_col: String,
    pub right_col: String,
    pub op: CompareOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table_name: String,
    pub on: Vec<JoinOn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinProjSource {
    Left(usize),
    Right(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinProjectionItem {
    pub source: JoinProjSource,
    pub output_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinFilter {
    Left {
        column_idx: usize,
        op: CompareOp,
        value: Value,
    },
    Right {
        column_idx: usize,
        op: CompareOp,
        value: Value,
    },
    And(Box<JoinFilter>, Box<JoinFilter>),
    Or(Box<JoinFilter>, Box<JoinFilter>),
    IsNullLeft(usize),
    IsNullRight(usize),
    IsNotNullLeft(usize),
    IsNotNullRight(usize),
}

impl JoinFilter {
    pub fn evaluate(&self, left_row: &[Value], right_row: &[Value]) -> bool {
        match self {
            JoinFilter::Left {
                column_idx,
                op,
                value,
            } => op.evaluate(&left_row[*column_idx], value),
            JoinFilter::Right {
                column_idx,
                op,
                value,
            } => op.evaluate(&right_row[*column_idx], value),
            JoinFilter::And(l, r) => {
                l.evaluate(left_row, right_row) && r.evaluate(left_row, right_row)
            }
            JoinFilter::Or(l, r) => {
                l.evaluate(left_row, right_row) || r.evaluate(left_row, right_row)
            }
            JoinFilter::IsNullLeft(idx) => matches!(left_row[*idx], Value::Null),
            JoinFilter::IsNullRight(idx) => matches!(right_row[*idx], Value::Null),
            JoinFilter::IsNotNullLeft(idx) => !matches!(left_row[*idx], Value::Null),
            JoinFilter::IsNotNullRight(idx) => !matches!(right_row[*idx], Value::Null),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByClause {
    pub column_name: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
    },
    AlterTableAddColumn {
        table_name: String,
        column: ColumnDef,
    },
    AlterTableRenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterTableDropColumn {
        table_name: String,
        column_name: String,
    },
    AlterTableSetNullable {
        table_name: String,
        column_name: String,
    },
    AlterTableSetNotNull {
        table_name: String,
        column_name: String,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        index_name: String,
    },
    Insert {
        table_name: String,
        values: Vec<Value>,
    },
    InsertMany {
        table_name: String,
        rows: Vec<Vec<Value>>,
    },
    InsertColumns {
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Value)>,
        increments: Vec<(String, i64)>,
        /// `col = col +/- $n` (column, 0-based param index, negative). Resolved by bind.
        param_increments: Vec<(String, usize, bool)>,
        filter: Option<Expr>,
    },
    Select {
        table_name: String,
        projection: Vec<String>,
        filter: Option<Expr>,
        group_by: Vec<String>,
        having: Option<Expr>,
        order_by: Option<OrderByClause>,
        order_by_list: Vec<OrderByClause>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: bool,
        join: Option<JoinClause>,
    },
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
        order_by_list: Vec<OrderByClause>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Delete {
        table_name: String,
        filter: Option<Expr>,
    },
    Explain(Box<Statement>),
    Checkpoint,
    Begin,
    Commit,
    Rollback,
}

fn bind_value(v: &mut Value, params: &[Value], used: &mut [bool]) -> Result<()> {
    if let Value::Param(i) = *v {
        let p = params.get(i).ok_or_else(|| {
            Error::TransactionError(format!("Missing value for parameter ${}", i + 1))
        })?;
        if matches!(p, Value::Param(_)) {
            return Err(Error::TransactionError(
                "A parameter value cannot itself be a placeholder".to_string(),
            ));
        }
        used[i] = true;
        *v = p.clone();
    }
    Ok(())
}

fn bind_row(row: &mut [Value], params: &[Value], used: &mut [bool]) -> Result<()> {
    for v in row.iter_mut() {
        bind_value(v, params, used)?;
    }
    Ok(())
}

fn bind_expr(e: &mut Expr, params: &[Value], used: &mut [bool]) -> Result<()> {
    match e {
        Expr::Literal(v) => bind_value(v, params, used),
        Expr::Column(_) => Ok(()),
        Expr::Eq(a, b) | Expr::And(a, b) | Expr::Or(a, b) => {
            bind_expr(a, params, used)?;
            bind_expr(b, params, used)
        }
        Expr::Compare { left, right, .. } => {
            bind_expr(left, params, used)?;
            bind_expr(right, params, used)
        }
        Expr::IsNull(a) | Expr::IsNotNull(a) => bind_expr(a, params, used),
    }
}

impl Statement {
    fn bind_params(&mut self, params: &[Value], used: &mut [bool]) -> Result<()> {
        match self {
            Statement::Insert { values, .. } => bind_row(values, params, used)?,
            Statement::InsertMany { rows, .. } | Statement::InsertColumns { rows, .. } => {
                for r in rows.iter_mut() {
                    bind_row(r, params, used)?;
                }
            }
            Statement::Update {
                assignments,
                increments,
                param_increments,
                filter,
                ..
            } => {
                for (_, v) in assignments.iter_mut() {
                    bind_value(v, params, used)?;
                }
                for (col, i, negative) in param_increments.drain(..) {
                    let p = params.get(i).ok_or_else(|| {
                        Error::TransactionError(format!("Missing value for parameter ${}", i + 1))
                    })?;
                    let n = match p {
                        Value::Int64(n) => *n,
                        _ => {
                            return Err(Error::TransactionError(format!(
                                "Parameter ${} must be an integer (used in {} = {} +/- ${})",
                                i + 1, col, col, i + 1
                            )));
                        }
                    };
                    let delta = if negative { n.checked_neg() } else { Some(n) }
                        .ok_or_else(|| Error::TransactionError("Number too large in UPDATE SET".to_string()))?;
                    used[i] = true;
                    increments.push((col, delta));
                }
                if let Some(e) = filter {
                    bind_expr(e, params, used)?;
                }
            }
            Statement::Select { filter, having, .. } => {
                if let Some(e) = filter {
                    bind_expr(e, params, used)?;
                }
                if let Some(e) = having {
                    bind_expr(e, params, used)?;
                }
            }
            Statement::Delete { filter, .. } => {
                if let Some(e) = filter {
                    bind_expr(e, params, used)?;
                }
            }
            Statement::Union { left, right, .. } => {
                left.bind_params(params, used)?;
                right.bind_params(params, used)?;
            }
            Statement::Explain(inner) => inner.bind_params(params, used)?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
    },
    AlterTableAddColumn {
        table_name: String,
        column: ColumnDef,
    },
    AlterTableRenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterTableDropColumn {
        table_name: String,
        column_name: String,
    },
    AlterTableSetNullable {
        table_name: String,
        column_name: String,
    },
    AlterTableSetNotNull {
        table_name: String,
        column_name: String,
    },
    Distinct {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
    Union {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        order_by_list: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_schema: TableSchema,
        right_schema: TableSchema,
        join_conditions: Vec<(usize, usize, CompareOp)>,
        projection: Vec<JoinProjectionItem>,
        filter: Option<JoinFilter>,
        sort_keys: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        index_name: String,
    },
    Insert {
        schema: TableSchema,
        values: Vec<Value>,
    },
    InsertMany {
        schema: TableSchema,
        rows: Vec<Vec<Value>>,
    },
    Update {
        schema: TableSchema,
        assignments: Vec<(usize, Value)>,
        increments: Vec<(usize, i64)>,
        filter: Option<BoundFilter>,
    },
    SeqScan {
        schema: TableSchema,
        projection: Vec<usize>,
        filter: Option<BoundFilter>,
        order_by: Option<(usize, bool)>,
        sort_keys: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    },
    IndexScan {
        schema: TableSchema,
        index: IndexMetadata,
        lookup_value: Value,
        projection: Vec<usize>,
        order_by: Option<(usize, bool)>,
        sort_keys: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    },
    IndexRangeScan {
        schema: TableSchema,
        index: IndexMetadata,
        ranges: Vec<KeyRange>,
        filter: Option<BoundFilter>,
        projection: Vec<usize>,
        order_by: Option<(usize, bool)>,
        sort_keys: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
        top_n: bool,
    },
    IndexOrderedScan {
        schema: TableSchema,
        index: IndexMetadata,
        ranges: Vec<KeyRange>,
        descending: bool,
        pk_descending: bool,
        filter: Option<BoundFilter>,
        projection: Vec<usize>,
        limit: Option<usize>,
        offset: usize,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        aggregates: Vec<BoundAggregate>,
        having: Option<BoundHavingPredicate>,
        order_by: Vec<BoundGroupOrderBy>,
        limit: Option<usize>,
        offset: usize,
    },
    GroupAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<usize>,
        select_items: Vec<BoundSelectItem>,
        aggregates: Vec<BoundAggregate>,
        having: Option<BoundHavingPredicate>,
        order_by: Vec<BoundGroupOrderBy>,
        limit: Option<usize>,
        offset: usize,
    },
    Delete {
        schema: TableSchema,
        filter: Option<BoundFilter>,
    },
    Explain(Box<PhysicalPlan>),
    Checkpoint,
    Begin,
    Commit,
    Rollback,
}

impl PhysicalPlan {
    pub fn is_top_n(&self) -> bool {
        match self {
            PhysicalPlan::SeqScan { top_n, .. }
            | PhysicalPlan::IndexScan { top_n, .. }
            | PhysicalPlan::IndexRangeScan { top_n, .. } => *top_n,
            PhysicalPlan::GroupAggregate {
                order_by, limit, ..
            } => !order_by.is_empty() && limit.is_some(),
            _ => false,
        }
    }

    pub fn is_index_ordered(&self) -> bool {
        matches!(self, PhysicalPlan::IndexOrderedScan { .. })
    }

    pub fn output_columns(&self) -> Vec<String> {
        match self {
            PhysicalPlan::SeqScan {
                schema, projection, ..
            }
            | PhysicalPlan::IndexScan {
                schema, projection, ..
            }
            | PhysicalPlan::IndexRangeScan {
                schema, projection, ..
            }
            | PhysicalPlan::IndexOrderedScan {
                schema, projection, ..
            } => projection
                .iter()
                .map(|&idx| schema.columns[idx].name.clone())
                .collect(),
            PhysicalPlan::Aggregate { aggregates, .. } => {
                aggregates.iter().map(|a| a.output_name.clone()).collect()
            }
            PhysicalPlan::GroupAggregate { select_items, .. } => select_items
                .iter()
                .map(|item| match item {
                    BoundSelectItem::Column { output_name, .. } => output_name.clone(),
                    BoundSelectItem::Aggregate(agg) => agg.output_name.clone(),
                })
                .collect(),
            PhysicalPlan::Distinct { input, .. } => input.output_columns(),
            PhysicalPlan::Union { left, .. } => left.output_columns(),
            PhysicalPlan::NestedLoopJoin { projection, .. } => {
                projection.iter().map(|p| p.output_name.clone()).collect()
            }
            _ => Vec::new(),
        }
    }
}

enum ResolvedProjection {
    Columns(Vec<usize>),
    Aggregates(Vec<BoundAggregate>),
}

pub struct QueryEngine {
    catalog: Arc<Catalog>,
}

impl QueryEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        QueryEngine { catalog }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn compile(&self, sql: &str) -> Result<()> {
        let _ = self.parse_and_plan(sql)?;
        Ok(())
    }

    pub fn parse(&self, sql: &str) -> Result<Statement> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return Err(Error::TransactionError("Empty SQL statement".to_string()));
        }
        let upper = trimmed.to_uppercase();
        if upper == "BEGIN" {
            return Ok(Statement::Begin);
        }
        if upper == "COMMIT" {
            return Ok(Statement::Commit);
        }
        if upper == "ROLLBACK" || upper == "ABORT" {
            return Ok(Statement::Rollback);
        }
        if upper == "CHECKPOINT" {
            return Ok(Statement::Checkpoint);
        }
        if upper.starts_with("EXPLAIN ") {
            let inner = self.parse(&trimmed[8..])?;
            return Ok(Statement::Explain(Box::new(inner)));
        }
        if upper.starts_with("CREATE TABLE ") {
            return Self::parse_create_table(trimmed);
        }
        if upper.starts_with("DROP TABLE ") {
            let name = trimmed[11..].trim().to_string();
            return Ok(Statement::DropTable { name });
        }
        if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
            return Self::parse_create_index(trimmed);
        }
        if upper.starts_with("DROP INDEX ") {
            let remainder = trimmed[11..].trim();
            let upper_rem = remainder.to_uppercase();
            let index_name = if let Some(on_pos) = upper_rem.find(" ON ") {
                remainder[..on_pos].trim().to_string()
            } else {
                remainder.to_string()
            };
            if index_name.is_empty() {
                return Err(Error::TransactionError(
                    "Missing index name in DROP INDEX".to_string(),
                ));
            }
            return Ok(Statement::DropIndex { index_name });
        }
        if upper.starts_with("INSERT INTO ") {
            return Self::parse_insert(trimmed);
        }
        if upper.starts_with("UPDATE ") {
            return Self::parse_update(trimmed);
        }
        if upper.starts_with("ALTER TABLE ") {
            return Self::parse_alter_table(trimmed);
        }
        if upper.starts_with("SELECT ") {
            let (is_union, is_union_all, union_pos, union_kw_len) =
                if let Some(pos) = Self::find_top_level_keyword(trimmed, " UNION ALL ") {
                    (true, true, pos, 11)
                } else if let Some(pos) = Self::find_top_level_keyword(trimmed, " UNION ") {
                    (true, false, pos, 7)
                } else {
                    (false, false, 0, 0)
                };

            if is_union {
                let left_sql = trimmed[..union_pos].trim();
                let right_full = trimmed[union_pos + union_kw_len..].trim();

                let mut remainder = right_full;
                let mut offset = None;
                if let Some(off_pos) = Self::find_top_level_keyword(remainder, " OFFSET ") {
                    let off_str = remainder[off_pos + 8..].trim();
                    if let Ok(n) = off_str.parse::<usize>() {
                        offset = Some(n);
                        remainder = remainder[..off_pos].trim();
                    }
                }
                let mut limit = None;
                if let Some(lim_pos) = Self::find_top_level_keyword(remainder, " LIMIT ") {
                    let lim_str = remainder[lim_pos + 7..].trim();
                    if let Ok(n) = lim_str.parse::<usize>() {
                        limit = Some(n);
                        remainder = remainder[..lim_pos].trim();
                    }
                }
                let mut order_by_list = Vec::new();
                if let Some(ob_pos) = Self::find_top_level_keyword(remainder, " ORDER BY ") {
                    let ob_str = remainder[ob_pos + 10..].trim();
                    for part in Self::split_top_level_commas(ob_str) {
                        order_by_list.push(Self::parse_order_by_item(part)?);
                    }
                    remainder = remainder[..ob_pos].trim();
                }

                let left_stmt = self.parse(left_sql)?;
                let right_stmt = self.parse(remainder)?;
                return Ok(Statement::Union {
                    left: Box::new(left_stmt),
                    right: Box::new(right_stmt),
                    all: is_union_all,
                    order_by_list,
                    limit,
                    offset,
                });
            }

            return Self::parse_select(trimmed);
        }
        if upper.starts_with("DELETE FROM ") {
            return Self::parse_delete(trimmed);
        }
        Err(Error::TransactionError(format!(
            "Unsupported SQL syntax: {}",
            trimmed
        )))
    }

    fn validate_column_value(table_name: &str, col: &ColumnDef, val: &Value) -> Result<()> {
        match val {
            Value::Null => {
                if !col.nullable || col.is_primary_key {
                    return Err(Error::TransactionError(format!(
                        "NOT NULL constraint failed: {}.{}",
                        table_name, col.name
                    )));
                }
                Ok(())
            }
            Value::Int64(_) if col.data_type == DataType::Int64 => Ok(()),
            Value::Text(_) if col.data_type == DataType::Text => Ok(()),
            Value::Bool(_) if col.data_type == DataType::Bool => Ok(()),
            Value::Bytes(_) if col.data_type == DataType::Bytes => Ok(()),
            _ => Err(Error::TransactionError(format!(
                "Type mismatch for column '{}.{}': expected {:?}",
                table_name, col.name, col.data_type
            ))),
        }
    }

    pub fn parse_and_plan(&self, sql: &str) -> Result<PhysicalPlan> {
        let stmt = self.parse(sql)?;
        self.plan(stmt)
    }

    /// Like `parse_and_plan`, but `$1, $2, ...` placeholders are allowed and bound
    /// to `params` (values never pass through SQL text, so no injection).
    pub fn parse_and_plan_params(&self, sql: &str, params: &[Value]) -> Result<PhysicalPlan> {
        let parsed = {
            let _guard = ParamsGuard::enable();
            self.parse(sql)
        };
        let mut stmt = parsed?;
        let mut used = vec![false; params.len()];
        stmt.bind_params(params, &mut used)?;
        if let Some(i) = used.iter().position(|u| !u) {
            return Err(Error::TransactionError(format!(
                "Parameter ${} was supplied but not used in the statement",
                i + 1
            )));
        }
        self.plan(stmt)
    }

    pub fn plan(&self, stmt: Statement) -> Result<PhysicalPlan> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateTable {
                name,
                columns,
                if_not_exists,
            }),
            Statement::DropTable { name } => Ok(PhysicalPlan::DropTable { name }),
            Statement::CreateIndex {
                index_name,
                table_name,
                column_name,
                unique,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateIndex {
                index_name,
                table_name,
                column_name,
                unique,
                if_not_exists,
            }),
            Statement::DropIndex { index_name } => Ok(PhysicalPlan::DropIndex { index_name }),
            Statement::InsertMany { table_name, rows } => {
                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let rows: Vec<Vec<Value>> = if schema.columns.iter().any(|c| c.is_dropped()) {
                    rows.into_iter()
                        .map(|r| Self::pad_dropped(&schema, r))
                        .collect()
                } else {
                    rows
                };
                for values in &rows {
                    if values.len() != schema.columns.len() {
                        return Err(Error::TransactionError(format!(
                            "Column count mismatch on INSERT into '{}'",
                            table_name
                        )));
                    }
                    for (col, val) in schema.columns.iter().zip(values.iter()) {
                        Self::validate_column_value(&table_name, col, val)?;
                    }
                }
                Ok(PhysicalPlan::InsertMany { schema, rows })
            }
            Statement::InsertColumns {
                table_name,
                columns,
                rows,
            } => {
                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let mut positions: Vec<usize> = Vec::with_capacity(columns.len());
                for name in &columns {
                    let pos = schema
                        .columns
                        .iter()
                        .position(|c| c.name == *name)
                        .ok_or_else(|| {
                            Error::TransactionError(format!(
                                "Unknown column '{}' in INSERT into '{}'",
                                name, table_name
                            ))
                        })?;
                    if positions.contains(&pos) {
                        return Err(Error::TransactionError(format!(
                            "Column '{}' given twice in INSERT",
                            name
                        )));
                    }
                    positions.push(pos);
                }
                let mut full_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() != positions.len() {
                        return Err(Error::TransactionError(format!(
                            "Value count does not match column list on INSERT into '{}'",
                            table_name
                        )));
                    }
                    let mut values = vec![Value::Null; schema.columns.len()];
                    for (pos, v) in positions.iter().zip(row) {
                        values[*pos] = v;
                    }
                    for (col, v) in schema.columns.iter().zip(values.iter()) {
                        Self::validate_column_value(&table_name, col, v)?;
                    }
                    full_rows.push(values);
                }
                if full_rows.len() == 1 {
                    let values = full_rows.remove(0);
                    Ok(PhysicalPlan::Insert { schema, values })
                } else {
                    Ok(PhysicalPlan::InsertMany {
                        schema,
                        rows: full_rows,
                    })
                }
            }
            Statement::Insert { table_name, values } => {
                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let values = if schema.columns.iter().any(|c| c.is_dropped()) {
                    Self::pad_dropped(&schema, values)
                } else {
                    values
                };
                if values.len() != schema.columns.len() {
                    return Err(Error::TransactionError(format!(
                        "Column count mismatch on INSERT into '{}'",
                        table_name
                    )));
                }
                for (col, val) in schema.columns.iter().zip(values.iter()) {
                    Self::validate_column_value(&table_name, col, val)?;
                }
                Ok(PhysicalPlan::Insert { schema, values })
            }
            Statement::Update {
                table_name,
                assignments,
                increments,
                param_increments: _,
                filter,
            } => {
                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let mut bound_assignments = Vec::with_capacity(assignments.len());
                for (col_name, val) in assignments {
                    let idx = Self::resolve_column_index(&schema, &col_name)?;
                    Self::validate_column_value(&table_name, &schema.columns[idx], &val)?;
                    bound_assignments.push((idx, val));
                }
                let mut bound_increments: Vec<(usize, i64)> = Vec::with_capacity(increments.len());
                for (col_name, delta) in increments {
                    let idx = Self::resolve_column_index(&schema, &col_name)?;
                    if schema.columns[idx].data_type != DataType::Int64 {
                        return Err(Error::TransactionError(format!(
                            "Cannot add a number to non-INT column '{}.{}'",
                            table_name, col_name
                        )));
                    }
                    if bound_assignments.iter().any(|(i, _)| *i == idx)
                        || bound_increments.iter().any(|(i, _)| *i == idx)
                    {
                        return Err(Error::TransactionError(format!(
                            "Column '{}' is set more than once in UPDATE",
                            col_name
                        )));
                    }
                    bound_increments.push((idx, delta));
                }
                let bound_filter = Self::bind_filter(&schema, filter)?;
                Ok(PhysicalPlan::Update {
                    schema,
                    assignments: bound_assignments,
                    increments: bound_increments,
                    filter: bound_filter,
                })
            }
            Statement::AlterTableDropColumn {
                table_name,
                column_name,
            } => Ok(PhysicalPlan::AlterTableDropColumn {
                table_name,
                column_name,
            }),
            Statement::AlterTableSetNullable {
                table_name,
                column_name,
            } => {
                let _ = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                Ok(PhysicalPlan::AlterTableSetNullable {
                    table_name,
                    column_name,
                })
            }
            Statement::AlterTableSetNotNull {
                table_name,
                column_name,
            } => {
                let _ = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                Ok(PhysicalPlan::AlterTableSetNotNull {
                    table_name,
                    column_name,
                })
            }
            Statement::AlterTableAddColumn { table_name, column } => {
                let _ = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                Ok(PhysicalPlan::AlterTableAddColumn { table_name, column })
            }
            Statement::AlterTableRenameColumn {
                table_name,
                old_name,
                new_name,
            } => {
                let _ = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                Ok(PhysicalPlan::AlterTableRenameColumn {
                    table_name,
                    old_name,
                    new_name,
                })
            }
            Statement::Union {
                left,
                right,
                all,
                order_by_list,
                limit,
                offset,
            } => {
                let left_plan = self.plan(*left)?;
                let right_plan = self.plan(*right)?;
                let left_cols = left_plan.output_columns();
                let right_cols = right_plan.output_columns();
                if !left_cols.is_empty()
                    && !right_cols.is_empty()
                    && left_cols.len() != right_cols.len()
                {
                    return Err(Error::TransactionError(
                        "UNION branches must have the same number of columns".to_string(),
                    ));
                }
                let mut sort_keys = Vec::with_capacity(order_by_list.len());
                for ob in &order_by_list {
                    if let Ok(col_num) = ob.column_name.parse::<usize>() {
                        if col_num == 0 || (!left_cols.is_empty() && col_num > left_cols.len()) {
                            return Err(Error::TransactionError(format!(
                                "ORDER BY ordinal {} out of range for UNION query",
                                col_num
                            )));
                        }
                        sort_keys.push((col_num - 1, ob.descending));
                    } else {
                        let target = ob.column_name.trim();
                        let stripped = target
                            .rsplit_once('.')
                            .map(|(_, col)| col.trim())
                            .unwrap_or(target);
                        if let Some(pos) = left_cols.iter().position(|c| {
                            c.eq_ignore_ascii_case(target)
                                || c.eq_ignore_ascii_case(stripped)
                                || c.rsplit_once('.')
                                    .is_some_and(|(_, col)| col.eq_ignore_ascii_case(stripped))
                        }) {
                            sort_keys.push((pos, ob.descending));
                        } else {
                            return Err(Error::TransactionError(format!(
                                "Unknown ORDER BY column '{}' in UNION query",
                                ob.column_name
                            )));
                        }
                    }
                }
                Ok(PhysicalPlan::Union {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                    order_by_list: sort_keys,
                    limit,
                    offset: offset.unwrap_or(0),
                })
            }
            Statement::Select {
                table_name,
                projection,
                filter,
                group_by,
                having,
                order_by,
                order_by_list,
                limit,
                offset,
                distinct,
                join,
            } => {
                if let Some(join_clause) = join {
                    let left_schema =
                        self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                            Error::TransactionError(format!("Table '{}' not found", table_name))
                        })?;
                    let right_schema = self
                        .catalog
                        .get_table_schema(&join_clause.table_name)?
                        .ok_or_else(|| {
                            Error::TransactionError(format!(
                                "Table '{}' not found",
                                join_clause.table_name
                            ))
                        })?;

                    let mut join_conditions = Vec::with_capacity(join_clause.on.len());
                    for on in &join_clause.on {
                        let left_src =
                            Self::resolve_join_column(&on.left_col, &left_schema, &right_schema)?;
                        let right_src =
                            Self::resolve_join_column(&on.right_col, &left_schema, &right_schema)?;
                        match (left_src, right_src) {
                            (JoinProjSource::Left(l), JoinProjSource::Right(r)) => {
                                join_conditions.push((l, r, on.op));
                            }
                            (JoinProjSource::Right(r), JoinProjSource::Left(l)) => {
                                let inv_op = match on.op {
                                    CompareOp::Eq => CompareOp::Eq,
                                    CompareOp::Ne => CompareOp::Ne,
                                    CompareOp::Lt => CompareOp::Gt,
                                    CompareOp::Le => CompareOp::Ge,
                                    CompareOp::Gt => CompareOp::Lt,
                                    CompareOp::Ge => CompareOp::Le,
                                };
                                join_conditions.push((l, r, inv_op));
                            }
                            _ => {
                                return Err(Error::TransactionError(
                                    "JOIN ON condition must compare a left table column with a right table column"
                                        .to_string(),
                                ));
                            }
                        }
                    }

                    let mut proj_items = Vec::new();
                    if projection.len() == 1 && projection[0] == "*" {
                        for (i, c) in left_schema.columns.iter().enumerate() {
                            if c.is_dropped() {
                                continue;
                            }
                            proj_items.push(JoinProjectionItem {
                                source: JoinProjSource::Left(i),
                                output_name: format!("{}.{}", left_schema.name, c.name),
                            });
                        }
                        for (j, c) in right_schema.columns.iter().enumerate() {
                            if c.is_dropped() {
                                continue;
                            }
                            proj_items.push(JoinProjectionItem {
                                source: JoinProjSource::Right(j),
                                output_name: format!("{}.{}", right_schema.name, c.name),
                            });
                        }
                    } else {
                        for item in &projection {
                            let src = Self::resolve_join_column(item, &left_schema, &right_schema)?;
                            let name = match src {
                                JoinProjSource::Left(i) => left_schema.columns[i].name.clone(),
                                JoinProjSource::Right(j) => right_schema.columns[j].name.clone(),
                            };
                            proj_items.push(JoinProjectionItem {
                                source: src,
                                output_name: name,
                            });
                        }
                    }

                    let effective_order_list = if !order_by_list.is_empty() {
                        order_by_list
                    } else if let Some(ob) = order_by {
                        vec![ob]
                    } else {
                        Vec::new()
                    };

                    let mut sort_keys = Vec::new();
                    for ob in &effective_order_list {
                        let src = Self::resolve_join_column(
                            &ob.column_name,
                            &left_schema,
                            &right_schema,
                        )?;
                        match proj_items.iter().position(|p| p.source == src) {
                            Some(pos) => sort_keys.push((pos, ob.descending)),
                            None => {
                                return Err(Error::TransactionError(format!(
                                    "ORDER BY column '{}' must appear in the SELECT list of a JOIN query",
                                    ob.column_name
                                )));
                            }
                        }
                    }

                    let all_left_cols = (0..left_schema.columns.len()).collect();
                    let left_plan = self.plan_select_scan(
                        left_schema.clone(),
                        all_left_cols,
                        None,
                        Vec::new(),
                        None,
                        0,
                    );
                    let all_right_cols = (0..right_schema.columns.len()).collect();
                    let right_plan = self.plan_select_scan(
                        right_schema.clone(),
                        all_right_cols,
                        None,
                        Vec::new(),
                        None,
                        0,
                    );

                    let bound_join_filter = if let Some(f_expr) = filter {
                        Some(Self::bind_join_filter(f_expr, &left_schema, &right_schema)?)
                    } else {
                        None
                    };

                    let effective_offset = offset.unwrap_or(0);
                    let join_limit = if distinct { None } else { limit };
                    let join_offset = if distinct { 0 } else { effective_offset };

                    let join_plan = PhysicalPlan::NestedLoopJoin {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type: join_clause.join_type,
                        left_schema,
                        right_schema,
                        join_conditions,
                        projection: proj_items,
                        filter: bound_join_filter,
                        sort_keys,
                        limit: join_limit,
                        offset: join_offset,
                    };

                    if distinct {
                        return Ok(PhysicalPlan::Distinct {
                            input: Box::new(join_plan),
                            limit,
                            offset: effective_offset,
                        });
                    } else {
                        return Ok(join_plan);
                    }
                }

                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let bound_filter = Self::bind_filter(&schema, filter)?;
                let effective_order_list = if !order_by_list.is_empty() {
                    order_by_list
                } else if let Some(ob) = order_by {
                    vec![ob]
                } else {
                    Vec::new()
                };
                let effective_offset = offset.unwrap_or(0);
                let scan_limit = if distinct { None } else { limit };
                let scan_offset = if distinct { 0 } else { effective_offset };

                if !group_by.is_empty() {
                    let mut group_cols = Vec::with_capacity(group_by.len());
                    for gb_col in &group_by {
                        let col_idx = Self::resolve_column_index(&schema, gb_col)?;
                        group_cols.push(col_idx);
                    }

                    let (select_items, mut all_aggregates) =
                        Self::resolve_grouped_projection(&schema, &group_cols, &projection)?;
                    let bound_having = match having {
                        Some(h_expr) => {
                            let hp = Self::bind_having_predicate(&schema, &group_cols, h_expr)?;
                            hp.collect_aggregates(&mut all_aggregates);
                            Some(hp)
                        }
                        None => None,
                    };
                    let bound_group_order = Self::bind_group_order_by(
                        &schema,
                        &group_cols,
                        &effective_order_list,
                        &mut all_aggregates,
                    )?;

                    let all_cols = (0..schema.columns.len()).collect();
                    let scan_plan =
                        self.plan_select_scan(schema, all_cols, bound_filter, Vec::new(), None, 0);

                    let grp_plan = PhysicalPlan::GroupAggregate {
                        input: Box::new(scan_plan),
                        group_by: group_cols,
                        select_items,
                        aggregates: all_aggregates,
                        having: bound_having,
                        order_by: bound_group_order,
                        limit: scan_limit,
                        offset: scan_offset,
                    };
                    if distinct {
                        return Ok(PhysicalPlan::Distinct {
                            input: Box::new(grp_plan),
                            limit,
                            offset: effective_offset,
                        });
                    } else {
                        return Ok(grp_plan);
                    }
                }

                let resolved_proj = Self::resolve_projection_or_aggregates(&schema, &projection)?;
                match resolved_proj {
                    ResolvedProjection::Columns(cols) => {
                        if having.is_some() {
                            return Err(Error::TransactionError(
                                "HAVING clause requires GROUP BY or aggregate functions"
                                    .to_string(),
                            ));
                        }
                        let mut sort_keys = Vec::with_capacity(effective_order_list.len());
                        for ob in &effective_order_list {
                            if Self::looks_like_aggregate(&ob.column_name) {
                                return Err(Error::TransactionError(
                                    "Cannot use aggregate in ORDER BY without GROUP BY".to_string(),
                                ));
                            }
                            let col_idx = Self::resolve_column_index(&schema, &ob.column_name)?;
                            sort_keys.push((col_idx, ob.descending));
                        }
                        let plan = self.plan_select_scan(
                            schema,
                            cols,
                            bound_filter,
                            sort_keys,
                            scan_limit,
                            scan_offset,
                        );
                        if distinct {
                            Ok(PhysicalPlan::Distinct {
                                input: Box::new(plan),
                                limit,
                                offset: effective_offset,
                            })
                        } else {
                            Ok(plan)
                        }
                    }
                    ResolvedProjection::Aggregates(aggs) => {
                        let empty_groups: [usize; 0] = [];
                        let mut extra_aggs = aggs.clone();
                        let bound_having = match having {
                            Some(h_expr) => {
                                let hp =
                                    Self::bind_having_predicate(&schema, &empty_groups, h_expr)?;
                                hp.collect_aggregates(&mut extra_aggs);
                                Some(hp)
                            }
                            None => None,
                        };
                        let bound_group_order = Self::bind_group_order_by(
                            &schema,
                            &empty_groups,
                            &effective_order_list,
                            &mut extra_aggs,
                        )?;
                        let all_cols = (0..schema.columns.len()).collect();
                        let scan_plan = self.plan_select_scan(
                            schema,
                            all_cols,
                            bound_filter,
                            Vec::new(),
                            None,
                            0,
                        );
                        let plan = PhysicalPlan::Aggregate {
                            input: Box::new(scan_plan),
                            aggregates: aggs,
                            having: bound_having,
                            order_by: bound_group_order,
                            limit: scan_limit,
                            offset: scan_offset,
                        };
                        if distinct {
                            Ok(PhysicalPlan::Distinct {
                                input: Box::new(plan),
                                limit,
                                offset: effective_offset,
                            })
                        } else {
                            Ok(plan)
                        }
                    }
                }
            }
            Statement::Delete { table_name, filter } => {
                let schema = self.catalog.get_table_schema(&table_name)?.ok_or_else(|| {
                    Error::TransactionError(format!("Table '{}' not found", table_name))
                })?;
                let bound_filter = Self::bind_filter(&schema, filter)?;
                Ok(PhysicalPlan::Delete {
                    schema,
                    filter: bound_filter,
                })
            }
            Statement::Explain(inner) => {
                let inner_plan = self.plan(*inner)?;
                Ok(PhysicalPlan::Explain(Box::new(inner_plan)))
            }
            Statement::Checkpoint => Ok(PhysicalPlan::Checkpoint),
            Statement::Begin => Ok(PhysicalPlan::Begin),
            Statement::Commit => Ok(PhysicalPlan::Commit),
            Statement::Rollback => Ok(PhysicalPlan::Rollback),
        }
    }

    fn plan_select_scan(
        &self,
        schema: TableSchema,
        projection: Vec<usize>,
        bound_filter: Option<BoundFilter>,
        sort_keys: Vec<(usize, bool)>,
        limit: Option<usize>,
        offset: usize,
    ) -> PhysicalPlan {
        let order_by = sort_keys.first().copied();
        let top_n = !sort_keys.is_empty() && limit.is_some();
        let mut indexes = self.catalog.get_table_indexes(schema.table_id);
        indexes.sort_by_key(|i| i.index_id);
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.is_primary_key)
            .unwrap_or(0);

        // Check if an existing secondary B+Tree index can directly satisfy ORDER BY
        if !sort_keys.is_empty() {
            let (first_sort_col, descending) = sort_keys[0];
            let sort_compatible =
                sort_keys.len() == 1 || (sort_keys.len() == 2 && sort_keys[1].0 == pk_idx);
            let pk_descending = if sort_keys.len() == 2 {
                sort_keys[1].1
            } else {
                false
            };

            if sort_compatible {
                let sort_col_name = &schema.columns[first_sort_col].name;
                if let Some(ord_idx) = indexes.iter().find(|i| &i.column_name == sort_col_name) {
                    match &bound_filter {
                        None => {
                            return PhysicalPlan::IndexOrderedScan {
                                schema,
                                index: ord_idx.clone(),
                                ranges: vec![KeyRange::full()],
                                descending,
                                pk_descending,
                                filter: None,
                                projection,
                                limit,
                                offset,
                            };
                        }
                        Some(bf) => {
                            if let Some(ranges) =
                                Self::extract_column_ranges(&bf.predicate, first_sort_col)
                            {
                                return PhysicalPlan::IndexOrderedScan {
                                    schema,
                                    index: ord_idx.clone(),
                                    ranges,
                                    descending,
                                    pk_descending,
                                    filter: Some(bf.clone()),
                                    projection,
                                    limit,
                                    offset,
                                };
                            }
                            let any_other_idx_bounded = indexes.iter().any(|idx_meta| {
                                schema
                                    .columns
                                    .iter()
                                    .position(|c| c.name == idx_meta.column_name)
                                    .is_some_and(|c_idx| {
                                        Self::extract_column_ranges(&bf.predicate, c_idx).is_some()
                                    })
                            });
                            if !any_other_idx_bounded {
                                return PhysicalPlan::IndexOrderedScan {
                                    schema,
                                    index: ord_idx.clone(),
                                    ranges: vec![KeyRange::full()],
                                    descending,
                                    pk_descending,
                                    filter: Some(bf.clone()),
                                    projection,
                                    limit,
                                    offset,
                                };
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref bf) = bound_filter {
            // Preserve PhysicalPlan::IndexScan for single simple equality predicate on an indexed column
            if let BoundPredicate::Comparison {
                column_idx,
                op: CompareOp::Eq,
                ref value,
            } = bf.predicate
                && !matches!(value, Value::Null)
            {
                let col_name = &schema.columns[column_idx].name;
                if let Some(idx_meta) = indexes.iter().find(|i| &i.column_name == col_name) {
                    return PhysicalPlan::IndexScan {
                        schema,
                        index: idx_meta.clone(),
                        lookup_value: value.clone(),
                        projection,
                        order_by,
                        sort_keys,
                        limit,
                        offset,
                        top_n,
                    };
                }
            }

            // Evaluate candidate IndexRangeScans across available secondary indexes
            let mut best_candidate: Option<(u8, IndexMetadata, Vec<KeyRange>)> = None;
            for idx_meta in indexes {
                if let Some(col_idx) = schema
                    .columns
                    .iter()
                    .position(|c| c.name == idx_meta.column_name)
                    && let Some(ranges) = Self::extract_column_ranges(&bf.predicate, col_idx)
                {
                    let score = Self::score_index_ranges(&ranges);
                    let replace = match &best_candidate {
                        None => true,
                        Some((best_score, _, _)) => score < *best_score,
                    };
                    if replace {
                        best_candidate = Some((score, idx_meta, ranges));
                    }
                }
            }

            if let Some((_, idx_meta, ranges)) = best_candidate {
                return PhysicalPlan::IndexRangeScan {
                    schema,
                    index: idx_meta,
                    ranges,
                    filter: Some(bf.clone()),
                    projection,
                    order_by,
                    sort_keys,
                    limit,
                    offset,
                    top_n,
                };
            }
        }

        PhysicalPlan::SeqScan {
            schema,
            projection,
            filter: bound_filter,
            order_by,
            sort_keys,
            limit,
            offset,
            top_n,
        }
    }

    fn score_index_ranges(ranges: &[KeyRange]) -> u8 {
        if ranges.iter().all(|r| r.is_empty()) {
            return 0;
        }
        if ranges
            .iter()
            .all(|r| matches!((&r.start, &r.end), (KeyBound::Included(a), KeyBound::Included(b)) if a == b))
        {
            return 1;
        }
        if ranges.iter().all(|r| {
            !matches!(r.start, KeyBound::Unbounded) && !matches!(r.end, KeyBound::Unbounded)
        }) {
            return 2;
        }
        3
    }

    /// Extracts a sound set of B+Tree `KeyRange`s for `target_col` from `pred`,
    /// or returns `None` if `pred` does not bound `target_col` to a subset of the index.
    pub fn extract_column_ranges(
        pred: &BoundPredicate,
        target_col: usize,
    ) -> Option<Vec<KeyRange>> {
        match pred {
            BoundPredicate::Comparison {
                column_idx,
                op,
                value,
            } => {
                if *column_idx != target_col {
                    return None;
                }
                if matches!(value, Value::Null) {
                    return Some(vec![KeyRange::exclusive(vec![0], vec![0])]);
                }
                let key = value.to_key_bytes();
                match op {
                    CompareOp::Eq => Some(vec![KeyRange::point(key)]),
                    CompareOp::Gt => Some(vec![KeyRange::from_exclusive(key)]),
                    CompareOp::Ge => Some(vec![KeyRange::from_inclusive(key)]),
                    CompareOp::Lt => Some(vec![KeyRange::to_exclusive(key)]),
                    CompareOp::Le => Some(vec![KeyRange::to_inclusive(key)]),
                    CompareOp::Ne => None,
                }
            }
            BoundPredicate::IsNull(column_idx) => {
                if *column_idx == target_col {
                    Some(vec![KeyRange::point(Value::Null.to_key_bytes())])
                } else {
                    None
                }
            }
            BoundPredicate::IsNotNull(_) => None,
            BoundPredicate::And(left, right) => {
                let l_ranges = Self::extract_column_ranges(left, target_col);
                let r_ranges = Self::extract_column_ranges(right, target_col);
                match (l_ranges, r_ranges) {
                    (Some(lr), Some(rr)) => Some(Self::intersect_range_sets(&lr, &rr)),
                    (Some(lr), None) => Some(lr),
                    (None, Some(rr)) => Some(rr),
                    (None, None) => None,
                }
            }
            BoundPredicate::Or(left, right) => {
                let lr = Self::extract_column_ranges(left, target_col)?;
                let rr = Self::extract_column_ranges(right, target_col)?;
                Some(Self::union_range_sets(lr, rr))
            }
        }
    }

    pub fn intersect_key_range(a: &KeyRange, b: &KeyRange) -> KeyRange {
        let start = Self::max_lower_bound(&a.start, &b.start);
        let end = Self::min_upper_bound(&a.end, &b.end);
        KeyRange::new(start, end)
    }

    fn intersect_range_sets(left: &[KeyRange], right: &[KeyRange]) -> Vec<KeyRange> {
        let mut out = Vec::new();
        let mut first_empty = None;
        for a in left {
            for b in right {
                let inter = Self::intersect_key_range(a, b);
                if inter.is_empty() {
                    if first_empty.is_none() {
                        first_empty = Some(inter);
                    }
                } else {
                    out.push(inter);
                }
            }
        }
        if out.is_empty() {
            vec![first_empty.unwrap_or_else(|| KeyRange::exclusive(vec![0], vec![0]))]
        } else {
            Self::normalize_range_set(out)
        }
    }

    fn union_range_sets(mut left: Vec<KeyRange>, right: Vec<KeyRange>) -> Vec<KeyRange> {
        left.extend(right);
        Self::normalize_range_set(left)
    }

    fn normalize_range_set(ranges: Vec<KeyRange>) -> Vec<KeyRange> {
        let mut non_empty: Vec<KeyRange> = ranges.into_iter().filter(|r| !r.is_empty()).collect();
        if non_empty.is_empty() {
            return vec![KeyRange::exclusive(vec![0], vec![0])];
        }
        non_empty.sort_by(|a, b| Self::cmp_lower_bounds(&a.start, &b.start));

        let mut merged: Vec<KeyRange> = Vec::with_capacity(non_empty.len());
        for next in non_empty {
            if let Some(last) = merged.last_mut()
                && Self::ranges_touch_or_overlap(last, &next)
            {
                last.end = Self::max_upper_bound(&last.end, &next.end);
                continue;
            }
            merged.push(next);
        }
        merged
    }

    fn ranges_touch_or_overlap(a: &KeyRange, b: &KeyRange) -> bool {
        if a.overlaps(b) {
            return true;
        }
        match (&a.end, &b.start) {
            (KeyBound::Included(k1), KeyBound::Excluded(k2))
            | (KeyBound::Excluded(k1), KeyBound::Included(k2))
            | (KeyBound::Included(k1), KeyBound::Included(k2)) => k1 == k2,
            _ => false,
        }
    }

    fn cmp_lower_bounds(a: &KeyBound, b: &KeyBound) -> Ordering {
        match (a, b) {
            (KeyBound::Unbounded, KeyBound::Unbounded) => Ordering::Equal,
            (KeyBound::Unbounded, _) => Ordering::Less,
            (_, KeyBound::Unbounded) => Ordering::Greater,
            (KeyBound::Included(k1), KeyBound::Included(k2))
            | (KeyBound::Excluded(k1), KeyBound::Excluded(k2)) => k1.cmp(k2),
            (KeyBound::Included(k1), KeyBound::Excluded(k2)) => {
                let c = k1.cmp(k2);
                if c == Ordering::Equal {
                    Ordering::Less
                } else {
                    c
                }
            }
            (KeyBound::Excluded(k1), KeyBound::Included(k2)) => {
                let c = k1.cmp(k2);
                if c == Ordering::Equal {
                    Ordering::Greater
                } else {
                    c
                }
            }
        }
    }

    fn max_lower_bound(a: &KeyBound, b: &KeyBound) -> KeyBound {
        if Self::cmp_lower_bounds(a, b) >= Ordering::Equal {
            a.clone()
        } else {
            b.clone()
        }
    }

    fn min_upper_bound(a: &KeyBound, b: &KeyBound) -> KeyBound {
        match (a, b) {
            (KeyBound::Unbounded, other) | (other, KeyBound::Unbounded) => other.clone(),
            (KeyBound::Included(k1), KeyBound::Included(k2)) => {
                KeyBound::Included(k1.min(k2).clone())
            }
            (KeyBound::Excluded(k1), KeyBound::Excluded(k2)) => {
                KeyBound::Excluded(k1.min(k2).clone())
            }
            (KeyBound::Included(k1), KeyBound::Excluded(k2))
            | (KeyBound::Excluded(k2), KeyBound::Included(k1)) => {
                if k1 < k2 {
                    KeyBound::Included(k1.clone())
                } else {
                    KeyBound::Excluded(k2.clone())
                }
            }
        }
    }

    fn max_upper_bound(a: &KeyBound, b: &KeyBound) -> KeyBound {
        match (a, b) {
            (KeyBound::Unbounded, _) | (_, KeyBound::Unbounded) => KeyBound::Unbounded,
            (KeyBound::Included(k1), KeyBound::Included(k2)) => {
                KeyBound::Included(k1.max(k2).clone())
            }
            (KeyBound::Excluded(k1), KeyBound::Excluded(k2)) => {
                KeyBound::Excluded(k1.max(k2).clone())
            }
            (KeyBound::Included(k1), KeyBound::Excluded(k2))
            | (KeyBound::Excluded(k2), KeyBound::Included(k1)) => {
                if k2 > k1 {
                    KeyBound::Excluded(k2.clone())
                } else {
                    KeyBound::Included(k1.clone())
                }
            }
        }
    }

    fn resolve_column_index(schema: &TableSchema, col_name: &str) -> Result<usize> {
        let lookup_name = if let Some(dot_pos) = col_name.find('.') {
            let tbl_part = &col_name[..dot_pos];
            if !tbl_part.eq_ignore_ascii_case(&schema.name) {
                return Err(Error::TransactionError(format!(
                    "Table '{}' does not match table '{}'",
                    tbl_part, schema.name
                )));
            }
            &col_name[dot_pos + 1..]
        } else {
            col_name
        };
        schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(lookup_name))
            .ok_or_else(|| {
                Error::TransactionError(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, schema.name
                ))
            })
    }

    fn resolve_join_column(
        col_expr: &str,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<JoinProjSource> {
        let trimmed = col_expr.trim();
        if let Some(dot_pos) = trimmed.find('.') {
            let tbl = &trimmed[..dot_pos];
            let col = &trimmed[dot_pos + 1..];
            if tbl.eq_ignore_ascii_case(&left_schema.name) {
                let idx = left_schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col))
                    .ok_or_else(|| {
                        Error::TransactionError(format!(
                            "Column '{}' not found in table '{}'",
                            col, left_schema.name
                        ))
                    })?;
                return Ok(JoinProjSource::Left(idx));
            } else if tbl.eq_ignore_ascii_case(&right_schema.name) {
                let idx = right_schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col))
                    .ok_or_else(|| {
                        Error::TransactionError(format!(
                            "Column '{}' not found in table '{}'",
                            col, right_schema.name
                        ))
                    })?;
                return Ok(JoinProjSource::Right(idx));
            } else {
                return Err(Error::TransactionError(format!(
                    "Unknown table '{}' in qualified column '{}'",
                    tbl, trimmed
                )));
            }
        }

        let in_left = left_schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(trimmed));
        let in_right = right_schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(trimmed));

        match (in_left, in_right) {
            (Some(_), Some(_)) => Err(Error::TransactionError(format!(
                "Column '{}' is ambiguous in JOIN query; qualify with table name",
                trimmed
            ))),
            (Some(l_idx), None) => Ok(JoinProjSource::Left(l_idx)),
            (None, Some(r_idx)) => Ok(JoinProjSource::Right(r_idx)),
            (None, None) => Err(Error::TransactionError(format!(
                "Column '{}' not found in table '{}' or '{}'",
                trimmed, left_schema.name, right_schema.name
            ))),
        }
    }

    fn bind_join_filter(
        expr: Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<JoinFilter> {
        match expr {
            Expr::Eq(l, r) => {
                Self::bind_join_compare(CompareOp::Eq, *l, *r, left_schema, right_schema)
            }
            Expr::Compare { op, left, right } => {
                Self::bind_join_compare(op, *left, *right, left_schema, right_schema)
            }
            Expr::And(left, right) => Ok(JoinFilter::And(
                Box::new(Self::bind_join_filter(*left, left_schema, right_schema)?),
                Box::new(Self::bind_join_filter(*right, left_schema, right_schema)?),
            )),
            Expr::Or(left, right) => Ok(JoinFilter::Or(
                Box::new(Self::bind_join_filter(*left, left_schema, right_schema)?),
                Box::new(Self::bind_join_filter(*right, left_schema, right_schema)?),
            )),
            Expr::IsNull(inner) => match *inner {
                Expr::Column(col_name) => {
                    let src = Self::resolve_join_column(&col_name, left_schema, right_schema)?;
                    match src {
                        JoinProjSource::Left(idx) => Ok(JoinFilter::IsNullLeft(idx)),
                        JoinProjSource::Right(idx) => Ok(JoinFilter::IsNullRight(idx)),
                    }
                }
                _ => Err(Error::TransactionError(
                    "IS NULL requires a column reference".to_string(),
                )),
            },
            Expr::IsNotNull(inner) => match *inner {
                Expr::Column(col_name) => {
                    let src = Self::resolve_join_column(&col_name, left_schema, right_schema)?;
                    match src {
                        JoinProjSource::Left(idx) => Ok(JoinFilter::IsNotNullLeft(idx)),
                        JoinProjSource::Right(idx) => Ok(JoinFilter::IsNotNullRight(idx)),
                    }
                }
                _ => Err(Error::TransactionError(
                    "IS NOT NULL requires a column reference".to_string(),
                )),
            },
            _ => Err(Error::TransactionError(
                "Unsupported filter expression in JOIN".to_string(),
            )),
        }
    }

    fn bind_join_compare(
        op: CompareOp,
        left: Expr,
        right: Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<JoinFilter> {
        match (left, right) {
            (Expr::Column(col), Expr::Literal(val)) => {
                let src = Self::resolve_join_column(&col, left_schema, right_schema)?;
                match src {
                    JoinProjSource::Left(idx) => Ok(JoinFilter::Left {
                        column_idx: idx,
                        op,
                        value: val,
                    }),
                    JoinProjSource::Right(idx) => Ok(JoinFilter::Right {
                        column_idx: idx,
                        op,
                        value: val,
                    }),
                }
            }
            (Expr::Literal(val), Expr::Column(col)) => {
                let src = Self::resolve_join_column(&col, left_schema, right_schema)?;
                let inv_op = match op {
                    CompareOp::Eq => CompareOp::Eq,
                    CompareOp::Ne => CompareOp::Ne,
                    CompareOp::Lt => CompareOp::Gt,
                    CompareOp::Le => CompareOp::Ge,
                    CompareOp::Gt => CompareOp::Lt,
                    CompareOp::Ge => CompareOp::Le,
                };
                match src {
                    JoinProjSource::Left(idx) => Ok(JoinFilter::Left {
                        column_idx: idx,
                        op: inv_op,
                        value: val,
                    }),
                    JoinProjSource::Right(idx) => Ok(JoinFilter::Right {
                        column_idx: idx,
                        op: inv_op,
                        value: val,
                    }),
                }
            }
            _ => Err(Error::TransactionError(
                "JOIN filter comparison must be between column and literal".to_string(),
            )),
        }
    }

    fn parse_alter_table(sql: &str) -> Result<Statement> {
        let rest = sql[12..].trim();
        let add_pos = Self::find_top_level_keyword(rest, " ADD ");
        let rename_pos = Self::find_top_level_keyword(rest, " RENAME ");
        let drop_pos = Self::find_top_level_keyword(rest, " DROP ");
        let alter_pos = Self::find_top_level_keyword(rest, " ALTER ");

        // ALTER COLUMN must be checked before DROP: "... DROP NOT NULL" contains " DROP ".
        if let Some(pos) = alter_pos {
            let after = rest[pos + 7..].trim_start();
            if after.len() >= 7
                && after.is_char_boundary(7)
                && after[..7].eq_ignore_ascii_case("COLUMN ")
            {
                let table_name = rest[..pos].trim().to_string();
                let tokens: Vec<&str> = after[7..].split_whitespace().collect();
                if tokens.len() < 2 {
                    return Err(Error::TransactionError(
                        "Invalid ALTER TABLE ALTER COLUMN syntax".to_string(),
                    ));
                }
                let column_name = tokens[0].to_string();
                let action = tokens[1..].join(" ").to_uppercase();
                if action == "DROP NOT NULL" {
                    return Ok(Statement::AlterTableSetNullable {
                        table_name,
                        column_name,
                    });
                }
                if action == "SET NOT NULL" {
                    return Ok(Statement::AlterTableSetNotNull {
                        table_name,
                        column_name,
                    });
                }
                return Err(Error::TransactionError(format!(
                    "Unsupported ALTER COLUMN operation: {}",
                    action
                )));
            }
        }

        if let Some(pos) = drop_pos {
            let table_name = rest[..pos].trim().to_string();
            let mut col = rest[pos + 6..].trim();
            if col.to_uppercase().starts_with("COLUMN ") {
                col = col[7..].trim();
            }
            if col.is_empty() || col.split_whitespace().count() != 1 {
                return Err(Error::TransactionError(
                    "Invalid column name in ALTER TABLE DROP COLUMN".to_string(),
                ));
            }
            return Ok(Statement::AlterTableDropColumn {
                table_name,
                column_name: col.to_string(),
            });
        }

        if let Some(pos) = add_pos {
            let table_name = rest[..pos].trim().to_string();
            let mut col_def_str = rest[pos + 5..].trim();
            if col_def_str.to_uppercase().starts_with("COLUMN ") {
                col_def_str = col_def_str[7..].trim();
            }
            let tokens: Vec<&str> = col_def_str.split_whitespace().collect();
            if tokens.len() < 2 {
                return Err(Error::TransactionError(
                    "Invalid column definition in ALTER TABLE ADD COLUMN".to_string(),
                ));
            }
            let col_name = tokens[0].to_string();
            Self::validate_name("column", &col_name)?;
            let dtype = match tokens[1].to_uppercase().as_str() {
                "INT" | "INT64" | "BIGINT" | "INTEGER" => DataType::Int64,
                "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                "BOOL" | "BOOLEAN" => DataType::Bool,
                "BYTES" | "BLOB" => DataType::Bytes,
                other => {
                    return Err(Error::TransactionError(format!(
                        "Unknown column type in ALTER TABLE: {}",
                        other
                    )));
                }
            };
            let rest_upper = tokens[2..].join(" ").to_uppercase();
            let is_pk = rest_upper.contains("PRIMARY KEY");
            let nullable = !is_pk && !rest_upper.contains("NOT NULL");
            let column = ColumnDef {
                name: col_name,
                data_type: dtype,
                nullable,
                is_primary_key: is_pk,
            };
            Ok(Statement::AlterTableAddColumn { table_name, column })
        } else if let Some(pos) = rename_pos {
            let table_name = rest[..pos].trim().to_string();
            let mut rename_str = rest[pos + 8..].trim();
            if rename_str.to_uppercase().starts_with("COLUMN ") {
                rename_str = rename_str[7..].trim();
            }
            let to_pos = Self::find_top_level_keyword(rename_str, " TO ").ok_or_else(|| {
                Error::TransactionError("Missing TO in ALTER TABLE RENAME COLUMN".to_string())
            })?;
            let old_name = rename_str[..to_pos].trim().to_string();
            let new_name = rename_str[to_pos + 4..].trim().to_string();
            if old_name.is_empty() || new_name.is_empty() {
                return Err(Error::TransactionError(
                    "Invalid column names in ALTER TABLE RENAME COLUMN".to_string(),
                ));
            }
            Self::validate_name("column", &new_name)?;
            Ok(Statement::AlterTableRenameColumn {
                table_name,
                old_name,
                new_name,
            })
        } else {
            Err(Error::TransactionError(format!(
                "Unsupported ALTER TABLE operation: {}",
                sql
            )))
        }
    }

    /// Expand live-column values to full width (NULL at dropped positions).
    fn pad_dropped(schema: &TableSchema, values: Vec<Value>) -> Vec<Value> {
        let live = schema.columns.iter().filter(|c| !c.is_dropped()).count();
        if values.len() != live {
            return values;
        }
        let mut it = values.into_iter();
        schema
            .columns
            .iter()
            .map(|c| {
                if c.is_dropped() {
                    Value::Null
                } else {
                    it.next().unwrap()
                }
            })
            .collect()
    }

    fn resolve_projection_or_aggregates(
        schema: &TableSchema,
        projection: &[String],
    ) -> Result<ResolvedProjection> {
        if projection.is_empty() || projection.iter().any(|p| p.trim().is_empty()) {
            return Err(Error::TransactionError(
                "Empty SELECT projection".to_string(),
            ));
        }
        if projection.len() == 1 && projection[0] == "*" {
            return Ok(ResolvedProjection::Columns(
                (0..schema.columns.len())
                    .filter(|&i| !schema.columns[i].is_dropped())
                    .collect(),
            ));
        }

        let has_agg = projection
            .iter()
            .any(|item| Self::looks_like_aggregate(item));
        if !has_agg {
            let mut indices = Vec::with_capacity(projection.len());
            for col_name in projection {
                indices.push(Self::resolve_column_index(schema, col_name)?);
            }
            return Ok(ResolvedProjection::Columns(indices));
        }

        let mut aggregates = Vec::with_capacity(projection.len());
        for item in projection {
            if !Self::looks_like_aggregate(item) {
                return Err(Error::TransactionError(
                    "Cannot mix aggregate and non-aggregate columns without GROUP BY".to_string(),
                ));
            }
            aggregates.push(Self::bind_aggregate(schema, item)?);
        }
        Ok(ResolvedProjection::Aggregates(aggregates))
    }

    fn resolve_grouped_projection(
        schema: &TableSchema,
        group_cols: &[usize],
        projection: &[String],
    ) -> Result<(Vec<BoundSelectItem>, Vec<BoundAggregate>)> {
        if projection.is_empty() || projection.iter().any(|p| p.trim().is_empty()) {
            return Err(Error::TransactionError(
                "Empty SELECT projection".to_string(),
            ));
        }
        if projection.iter().any(|p| p.trim() == "*") {
            return Err(Error::TransactionError(
                "SELECT * is not allowed with GROUP BY; explicitly list grouped columns and aggregates"
                    .to_string(),
            ));
        }

        let mut select_items = Vec::with_capacity(projection.len());
        let mut aggregates: Vec<BoundAggregate> = Vec::new();

        for item in projection {
            let trimmed = item.trim();
            if Self::looks_like_aggregate(trimmed) {
                let agg = Self::bind_aggregate(schema, trimmed)?;
                if !aggregates.iter().any(|a| a.same_metric(&agg)) {
                    aggregates.push(agg.clone());
                }
                select_items.push(BoundSelectItem::Aggregate(agg));
            } else {
                let col_idx = Self::resolve_column_index(schema, trimmed)?;
                if !group_cols.contains(&col_idx) {
                    return Err(Error::TransactionError(format!(
                        "Column '{}' in SELECT must appear in GROUP BY or be used in an aggregate function",
                        schema.columns[col_idx].name
                    )));
                }
                select_items.push(BoundSelectItem::Column {
                    column_idx: col_idx,
                    output_name: schema.columns[col_idx].name.clone(),
                });
            }
        }

        Ok((select_items, aggregates))
    }

    fn bind_having_predicate(
        schema: &TableSchema,
        group_cols: &[usize],
        expr: Expr,
    ) -> Result<BoundHavingPredicate> {
        match expr {
            Expr::Eq(left, right) => {
                Self::bind_having_compare(schema, group_cols, CompareOp::Eq, *left, *right)
            }
            Expr::Compare { op, left, right } => {
                Self::bind_having_compare(schema, group_cols, op, *left, *right)
            }
            Expr::And(left, right) => Ok(BoundHavingPredicate::And(
                Box::new(Self::bind_having_predicate(schema, group_cols, *left)?),
                Box::new(Self::bind_having_predicate(schema, group_cols, *right)?),
            )),
            Expr::Or(left, right) => Ok(BoundHavingPredicate::Or(
                Box::new(Self::bind_having_predicate(schema, group_cols, *left)?),
                Box::new(Self::bind_having_predicate(schema, group_cols, *right)?),
            )),
            Expr::IsNull(inner) => match *inner {
                Expr::Column(ref token) => {
                    let (target, _) = Self::bind_having_target(schema, group_cols, token)?;
                    Ok(BoundHavingPredicate::IsNull(target))
                }
                _ => Err(Error::TransactionError(
                    "HAVING IS NULL requires a grouped column or aggregate expression".to_string(),
                )),
            },
            Expr::IsNotNull(inner) => match *inner {
                Expr::Column(ref token) => {
                    let (target, _) = Self::bind_having_target(schema, group_cols, token)?;
                    Ok(BoundHavingPredicate::IsNotNull(target))
                }
                _ => Err(Error::TransactionError(
                    "HAVING IS NOT NULL requires a grouped column or aggregate expression"
                        .to_string(),
                )),
            },
            _ => Err(Error::TransactionError(
                "Unsupported HAVING expression".to_string(),
            )),
        }
    }

    fn bind_having_compare(
        schema: &TableSchema,
        group_cols: &[usize],
        op: CompareOp,
        left: Expr,
        right: Expr,
    ) -> Result<BoundHavingPredicate> {
        match (left, right) {
            (Expr::Column(token), Expr::Literal(value)) => {
                let (target, expected_type) = Self::bind_having_target(schema, group_cols, &token)?;
                Self::validate_value_type(&token, expected_type, &value)?;
                Ok(BoundHavingPredicate::Comparison { target, op, value })
            }
            _ => Err(Error::TransactionError(
                "HAVING comparison must be <aggregate_or_group_col> <op> <literal>".to_string(),
            )),
        }
    }

    fn bind_having_target(
        schema: &TableSchema,
        group_cols: &[usize],
        token: &str,
    ) -> Result<(BoundGroupTarget, DataType)> {
        let trimmed = token.trim();
        if Self::looks_like_aggregate(trimmed) {
            let agg = Self::bind_aggregate(schema, trimmed)?;
            let dtype = match agg.func {
                AggregateFunc::CountStar
                | AggregateFunc::Count
                | AggregateFunc::Sum
                | AggregateFunc::Avg => DataType::Int64,
                AggregateFunc::Min | AggregateFunc::Max => {
                    let col_idx = agg.column_idx.unwrap();
                    schema.columns[col_idx].data_type
                }
            };
            Ok((BoundGroupTarget::Aggregate(agg), dtype))
        } else {
            let col_idx = Self::resolve_column_index(schema, trimmed)?;
            if !group_cols.contains(&col_idx) {
                return Err(Error::TransactionError(format!(
                    "Column '{}' in HAVING must appear in GROUP BY or be used in an aggregate function",
                    schema.columns[col_idx].name
                )));
            }
            Ok((
                BoundGroupTarget::GroupColumn(col_idx),
                schema.columns[col_idx].data_type,
            ))
        }
    }

    fn bind_group_order_by(
        schema: &TableSchema,
        group_cols: &[usize],
        order_by_list: &[OrderByClause],
        all_aggregates: &mut Vec<BoundAggregate>,
    ) -> Result<Vec<BoundGroupOrderBy>> {
        let mut out = Vec::with_capacity(order_by_list.len());
        for ob in order_by_list {
            let trimmed = ob.column_name.trim();
            if Self::looks_like_aggregate(trimmed) {
                let agg = Self::bind_aggregate(schema, trimmed)?;
                if !all_aggregates.iter().any(|a| a.same_metric(&agg)) {
                    all_aggregates.push(agg.clone());
                }
                out.push(BoundGroupOrderBy {
                    target: BoundGroupTarget::Aggregate(agg),
                    descending: ob.descending,
                });
            } else {
                let col_idx = Self::resolve_column_index(schema, trimmed)?;
                if !group_cols.contains(&col_idx) {
                    return Err(Error::TransactionError(format!(
                        "Column '{}' in ORDER BY must appear in GROUP BY or be used in an aggregate function",
                        schema.columns[col_idx].name
                    )));
                }
                out.push(BoundGroupOrderBy {
                    target: BoundGroupTarget::GroupColumn(col_idx),
                    descending: ob.descending,
                });
            }
        }
        Ok(out)
    }

    fn looks_like_aggregate(item: &str) -> bool {
        item.contains('(') || item.contains(')')
    }

    fn bind_aggregate(schema: &TableSchema, item: &str) -> Result<BoundAggregate> {
        let trimmed = item.trim();
        let open_paren = trimmed.find('(').ok_or_else(|| {
            Error::TransactionError(format!("Malformed aggregate expression: {}", trimmed))
        })?;
        if !trimmed.ends_with(')') {
            return Err(Error::TransactionError(format!(
                "Malformed aggregate expression: {}",
                trimmed
            )));
        }
        let func_str = trimmed[..open_paren].trim().to_uppercase();
        let arg_str = trimmed[open_paren + 1..trimmed.len() - 1].trim();
        if arg_str.is_empty()
            || arg_str.contains(',')
            || arg_str.contains('(')
            || arg_str.contains(')')
        {
            return Err(Error::TransactionError(format!(
                "Invalid aggregate argument in '{}'",
                trimmed
            )));
        }

        match func_str.as_str() {
            "COUNT" => {
                if arg_str == "*" {
                    Ok(BoundAggregate {
                        func: AggregateFunc::CountStar,
                        column_idx: None,
                        output_name: "COUNT(*)".to_string(),
                    })
                } else {
                    let col_idx = Self::resolve_column_index(schema, arg_str)?;
                    Ok(BoundAggregate {
                        func: AggregateFunc::Count,
                        column_idx: Some(col_idx),
                        output_name: format!("COUNT({})", schema.columns[col_idx].name),
                    })
                }
            }
            "SUM" => {
                if arg_str == "*" {
                    return Err(Error::TransactionError(
                        "SUM(*) is not supported; specify a numeric column".to_string(),
                    ));
                }
                let col_idx = Self::resolve_column_index(schema, arg_str)?;
                let dtype = schema.columns[col_idx].data_type;
                if dtype != DataType::Int64 {
                    return Err(Error::TransactionError(format!(
                        "SUM requires numeric INT64 column, found {:?} on '{}'",
                        dtype, schema.columns[col_idx].name
                    )));
                }
                Ok(BoundAggregate {
                    func: AggregateFunc::Sum,
                    column_idx: Some(col_idx),
                    output_name: format!("SUM({})", schema.columns[col_idx].name),
                })
            }
            "AVG" => {
                if arg_str == "*" {
                    return Err(Error::TransactionError(
                        "AVG(*) is not supported; specify a numeric column".to_string(),
                    ));
                }
                let col_idx = Self::resolve_column_index(schema, arg_str)?;
                let dtype = schema.columns[col_idx].data_type;
                if dtype != DataType::Int64 {
                    return Err(Error::TransactionError(format!(
                        "AVG requires numeric INT64 column, found {:?} on '{}'",
                        dtype, schema.columns[col_idx].name
                    )));
                }
                Ok(BoundAggregate {
                    func: AggregateFunc::Avg,
                    column_idx: Some(col_idx),
                    output_name: format!("AVG({})", schema.columns[col_idx].name),
                })
            }
            "MIN" | "MAX" => {
                if arg_str == "*" {
                    return Err(Error::TransactionError(format!(
                        "{}(*) is not supported; specify a column",
                        func_str
                    )));
                }
                let col_idx = Self::resolve_column_index(schema, arg_str)?;
                let dtype = schema.columns[col_idx].data_type;
                if !matches!(dtype, DataType::Int64 | DataType::Text) {
                    return Err(Error::TransactionError(format!(
                        "{} requires INT64 or TEXT column, found {:?} on '{}'",
                        func_str, dtype, schema.columns[col_idx].name
                    )));
                }
                let func = if func_str == "MIN" {
                    AggregateFunc::Min
                } else {
                    AggregateFunc::Max
                };
                Ok(BoundAggregate {
                    func,
                    column_idx: Some(col_idx),
                    output_name: format!("{}({})", func_str, schema.columns[col_idx].name),
                })
            }
            other => Err(Error::TransactionError(format!(
                "Unsupported aggregate function: {}",
                other
            ))),
        }
    }

    fn bind_filter(schema: &TableSchema, filter: Option<Expr>) -> Result<Option<BoundFilter>> {
        match filter {
            None => Ok(None),
            Some(expr) => {
                let pred = Self::bind_predicate(schema, expr)?;
                Ok(Some(BoundFilter::from_predicate(pred)))
            }
        }
    }

    fn bind_predicate(schema: &TableSchema, expr: Expr) -> Result<BoundPredicate> {
        match expr {
            Expr::Eq(left, right) => Self::bind_compare_pair(schema, CompareOp::Eq, *left, *right),
            Expr::Compare { op, left, right } => Self::bind_compare_pair(schema, op, *left, *right),
            Expr::And(left, right) => Ok(BoundPredicate::And(
                Box::new(Self::bind_predicate(schema, *left)?),
                Box::new(Self::bind_predicate(schema, *right)?),
            )),
            Expr::Or(left, right) => Ok(BoundPredicate::Or(
                Box::new(Self::bind_predicate(schema, *left)?),
                Box::new(Self::bind_predicate(schema, *right)?),
            )),
            Expr::IsNull(inner) => match *inner {
                Expr::Column(col_name) => {
                    if Self::looks_like_aggregate(&col_name) {
                        return Err(Error::TransactionError(
                            "Aggregate functions are not allowed in WHERE clause; use HAVING"
                                .to_string(),
                        ));
                    }
                    let column_idx = Self::resolve_column_index(schema, &col_name)?;
                    Ok(BoundPredicate::IsNull(column_idx))
                }
                _ => Err(Error::TransactionError(
                    "IS NULL requires a column reference".to_string(),
                )),
            },
            Expr::IsNotNull(inner) => match *inner {
                Expr::Column(col_name) => {
                    if Self::looks_like_aggregate(&col_name) {
                        return Err(Error::TransactionError(
                            "Aggregate functions are not allowed in WHERE clause; use HAVING"
                                .to_string(),
                        ));
                    }
                    let column_idx = Self::resolve_column_index(schema, &col_name)?;
                    Ok(BoundPredicate::IsNotNull(column_idx))
                }
                _ => Err(Error::TransactionError(
                    "IS NOT NULL requires a column reference".to_string(),
                )),
            },
            _ => Err(Error::TransactionError(
                "Unsupported filter expression".to_string(),
            )),
        }
    }

    fn bind_compare_pair(
        schema: &TableSchema,
        op: CompareOp,
        left: Expr,
        right: Expr,
    ) -> Result<BoundPredicate> {
        match (left, right) {
            (Expr::Column(col_name), Expr::Literal(value)) => {
                if Self::looks_like_aggregate(&col_name) {
                    return Err(Error::TransactionError(
                        "Aggregate functions are not allowed in WHERE clause; use HAVING"
                            .to_string(),
                    ));
                }
                let column_idx = Self::resolve_column_index(schema, &col_name)?;
                let col_type = schema.columns[column_idx].data_type;
                Self::validate_value_type(&col_name, col_type, &value)?;
                Ok(BoundPredicate::Comparison {
                    column_idx,
                    op,
                    value,
                })
            }
            _ => Err(Error::TransactionError(
                "WHERE comparison must be <column> <op> <literal>".to_string(),
            )),
        }
    }

    fn validate_value_type(col_name: &str, expected: DataType, value: &Value) -> Result<()> {
        let valid = matches!(
            (expected, value),
            (_, Value::Null)
                | (DataType::Int64, Value::Int64(_))
                | (DataType::Text, Value::Text(_))
                | (DataType::Bool, Value::Bool(_))
                | (DataType::Bytes, Value::Bytes(_))
        );
        if !valid {
            return Err(Error::TransactionError(format!(
                "Type mismatch in predicate on column '{}': expected {:?}, got {:?}",
                col_name, expected, value
            )));
        }
        Ok(())
    }

    /// New table, index and column names: letters, digits, `_`; start with a letter or `_`; max 64.
    fn validate_name(kind: &str, name: &str) -> Result<()> {
        // same value as mitrdb_catalog::DROPPED_PREFIX; reserved for logically dropped columns
        if kind == "column" && name.starts_with("__dropped_") {
            return Err(Error::TransactionError(
                "Column names starting with '__dropped_' are reserved".to_string(),
            ));
        }
        let mut chars = name.chars();
        let ok = name.len() <= 64
            && matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !ok {
            return Err(Error::TransactionError(format!(
                "Invalid {} name (letters, digits and _ only, must start with a letter or _, max 64 characters)",
                kind
            )));
        }
        Ok(())
    }

    /// Splits an optional `IF NOT EXISTS` off the front of a name.
    fn strip_if_not_exists(s: &str) -> Result<(&str, bool)> {
        const KW: &str = "IF NOT EXISTS";
        let t = s.trim();
        match t.get(..KW.len()) {
            Some(head) if head.eq_ignore_ascii_case(KW) => {
                let rest = &t[KW.len()..];
                let name = rest.trim();
                if name.is_empty() || !rest.starts_with(char::is_whitespace) {
                    return Err(Error::TransactionError(
                        "Invalid IF NOT EXISTS clause".to_string(),
                    ));
                }
                Ok((name, true))
            }
            _ => Ok((t, false)),
        }
    }

    fn parse_create_table(sql: &str) -> Result<Statement> {
        let open_paren = sql
            .find('(')
            .ok_or_else(|| Error::TransactionError("Missing '(' in CREATE TABLE".to_string()))?;
        let close_paren = sql
            .rfind(')')
            .ok_or_else(|| Error::TransactionError("Missing ')' in CREATE TABLE".to_string()))?;
        let (name, if_not_exists) = Self::strip_if_not_exists(&sql[13..open_paren])?;
        let name = name.to_string();
        Self::validate_name("table", &name)?;
        let cols_str = &sql[open_paren + 1..close_paren];
        let mut columns = Vec::new();

        for part in cols_str.split(',') {
            let tokens: Vec<&str> = part.split_whitespace().collect();
            if tokens.len() < 2 {
                return Err(Error::TransactionError(
                    "Invalid column definition in CREATE TABLE".to_string(),
                ));
            }
            let col_name = tokens[0].to_string();
            Self::validate_name("column", &col_name)?;
            let dtype = match tokens[1].to_uppercase().as_str() {
                "INT" | "INT64" | "BIGINT" | "INTEGER" => DataType::Int64,
                "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                "BOOL" | "BOOLEAN" => DataType::Bool,
                "BYTES" | "BLOB" => DataType::Bytes,
                other => {
                    return Err(Error::TransactionError(format!(
                        "Unknown column type: {}",
                        other
                    )));
                }
            };
            let rest_upper = tokens[2..].join(" ").to_uppercase();
            let is_pk = rest_upper.contains("PRIMARY KEY");
            let nullable = !is_pk && !rest_upper.contains("NOT NULL");
            columns.push(ColumnDef {
                name: col_name,
                data_type: dtype,
                nullable,
                is_primary_key: is_pk,
            });
        }

        Ok(Statement::CreateTable {
            name,
            columns,
            if_not_exists,
        })
    }

    fn parse_create_index(sql: &str) -> Result<Statement> {
        let upper = sql.to_uppercase();
        let (unique, prefix_len) = if upper.starts_with("CREATE UNIQUE INDEX ") {
            (true, 20)
        } else {
            (false, 13)
        };
        let rest = sql[prefix_len..].trim();
        let on_pos = rest
            .to_uppercase()
            .find(" ON ")
            .ok_or_else(|| Error::TransactionError("Missing ON in CREATE INDEX".to_string()))?;
        let (index_name, if_not_exists) = Self::strip_if_not_exists(&rest[..on_pos])?;
        let index_name = index_name.to_string();
        Self::validate_name("index", &index_name)?;
        let after_on = rest[on_pos + 4..].trim();
        let open_paren = after_on
            .find('(')
            .ok_or_else(|| Error::TransactionError("Missing '(' in CREATE INDEX".to_string()))?;
        let close_paren = after_on
            .rfind(')')
            .ok_or_else(|| Error::TransactionError("Missing ')' in CREATE INDEX".to_string()))?;
        let table_name = after_on[..open_paren].trim().to_string();
        let column_name = after_on[open_paren + 1..close_paren].trim().to_string();
        Ok(Statement::CreateIndex {
            index_name,
            table_name,
            column_name,
            unique,
            if_not_exists,
        })
    }

    fn split_top_level_commas(input: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut paren_depth = 0usize;
        let mut start = 0;
        for (idx, ch) in input.char_indices() {
            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            } else if !in_single_quote && !in_double_quote {
                if ch == '(' {
                    paren_depth += 1;
                } else if ch == ')' {
                    paren_depth = paren_depth.saturating_sub(1);
                } else if ch == ',' && paren_depth == 0 {
                    result.push(&input[start..idx]);
                    start = idx + 1;
                }
            }
        }
        if start <= input.len() {
            result.push(&input[start..]);
        }
        result
    }

    fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut paren_depth = 0usize;
        let kw_len = keyword.len();
        for (idx, ch) in input.char_indices() {
            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            } else if !in_single_quote && !in_double_quote {
                if ch == '(' {
                    paren_depth += 1;
                } else if ch == ')' {
                    paren_depth = paren_depth.saturating_sub(1);
                } else if paren_depth == 0
                    && input
                        .get(idx..idx + kw_len)
                        .is_some_and(|sub| sub.eq_ignore_ascii_case(keyword))
                {
                    return Some(idx);
                }
            }
        }
        None
    }

    /// Splits `(1,'a'), (2,'b')` into the text inside each bracket pair.
    /// Returns None for anything that is not exactly a comma-separated list of groups.
    fn split_value_groups(s: &str) -> Option<Vec<&str>> {
        let mut groups = Vec::new();
        let (mut in_single, mut in_double) = (false, false);
        let mut depth = 0usize;
        let mut start = 0usize;
        let mut after_group = false;
        for (i, ch) in s.char_indices() {
            if ch == '\'' && !in_double {
                in_single = !in_single;
                continue;
            }
            if ch == '"' && !in_single {
                in_double = !in_double;
                continue;
            }
            if in_single || in_double {
                continue;
            }
            match ch {
                '(' => {
                    if depth != 0 || after_group {
                        return None;
                    }
                    depth = 1;
                    start = i + 1;
                }
                ')' => {
                    if depth != 1 {
                        return None;
                    }
                    groups.push(&s[start..i]);
                    depth = 0;
                    after_group = true;
                }
                ',' if depth == 0 => {
                    if !after_group {
                        return None;
                    }
                    after_group = false;
                }
                c if depth == 0 && c.is_ascii_whitespace() => {}
                _ if depth == 0 => return None,
                _ => {}
            }
        }
        if in_single || in_double || depth != 0 || !after_group {
            return None;
        }
        Some(groups)
    }

    fn parse_insert(sql: &str) -> Result<Statement> {
        let rest = sql[12..].trim();
        let values_pos = Self::find_top_level_keyword(rest, "VALUES")
            .ok_or_else(|| Error::TransactionError("Missing VALUES in INSERT".to_string()))?;
        let head = rest[..values_pos].trim();
        let after_values = rest[values_pos + 6..].trim();
        let (table_name, columns) = match head.find('(') {
            None => (head.to_string(), None),
            Some(open) => {
                if !head.ends_with(')') {
                    return Err(Error::TransactionError(
                        "Invalid column list in INSERT".to_string(),
                    ));
                }
                let mut cols: Vec<String> = Vec::new();
                for c in head[open + 1..head.len() - 1].split(',') {
                    let c = c.trim();
                    Self::validate_name("column", c)?;
                    cols.push(c.to_string());
                }
                (head[..open].trim().to_string(), Some(cols))
            }
        };
        if let Some(columns) = columns {
            const MAX_COLUMN_INSERT_ROWS: usize = 1000;
            let groups = Self::split_value_groups(after_values).ok_or_else(|| {
                Error::TransactionError("Invalid VALUES list in INSERT".to_string())
            })?;
            if groups.len() > MAX_COLUMN_INSERT_ROWS {
                return Err(Error::TransactionError(format!(
                    "Too many rows in one INSERT (max {})",
                    MAX_COLUMN_INSERT_ROWS
                )));
            }
            let mut rows = Vec::with_capacity(groups.len());
            for group in groups {
                let mut row = Vec::new();
                for item in Self::split_top_level_commas(group) {
                    row.push(Self::parse_literal(item.trim())?);
                }
                rows.push(row);
            }
            return Ok(Statement::InsertColumns {
                table_name,
                columns,
                rows,
            });
        }
        if let Some(groups) = Self::split_value_groups(after_values)
            && groups.len() > 1
        {
            const MAX_INSERT_ROWS: usize = 1000;
            if groups.len() > MAX_INSERT_ROWS {
                return Err(Error::TransactionError(format!(
                    "Too many rows in one INSERT (max {})",
                    MAX_INSERT_ROWS
                )));
            }
            let mut rows = Vec::with_capacity(groups.len());
            for group in groups {
                let mut row = Vec::new();
                for item in Self::split_top_level_commas(group) {
                    row.push(Self::parse_literal(item.trim())?);
                }
                rows.push(row);
            }
            return Ok(Statement::InsertMany { table_name, rows });
        }
        if !after_values.starts_with('(') || !after_values.ends_with(')') {
            return Err(Error::TransactionError(
                "Invalid VALUES list in INSERT".to_string(),
            ));
        }
        let list_str = &after_values[1..after_values.len() - 1];
        let mut values = Vec::new();
        for item in Self::split_top_level_commas(list_str) {
            values.push(Self::parse_literal(item.trim())?);
        }
        Ok(Statement::Insert { table_name, values })
    }

    /// Reads `col + 5` or `col - 5` (same column on both sides). Returns None for anything else.
    fn parse_increment(col: &str, rhs: &str) -> Result<Option<i64>> {
        if col.is_empty() || rhs.starts_with('\'') || rhs.starts_with('"') {
            return Ok(None);
        }
        let Some(head) = rhs.get(..col.len()) else {
            return Ok(None);
        };
        if !head.eq_ignore_ascii_case(col) {
            return Ok(None);
        }
        let rest = rhs[col.len()..].trim_start();
        let negative = match rest.chars().next() {
            Some('+') => false,
            Some('-') => true,
            _ => return Ok(None),
        };
        let digits = rest[1..].trim();
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::TransactionError(format!(
                "Invalid number in UPDATE SET: {}",
                rhs
            )));
        }
        let n: i64 = digits
            .parse()
            .map_err(|_| Error::TransactionError("Number too large in UPDATE SET".to_string()))?;
        Ok(Some(if negative { -n } else { n }))
    }

    /// `col + $n` / `col - $n` (same column on the left). None for anything else.
    fn parse_param_increment(col: &str, rhs: &str) -> Result<Option<(usize, bool)>> {
        if col.is_empty() || rhs.starts_with('\'') || rhs.starts_with('"') {
            return Ok(None);
        }
        let Some(head) = rhs.get(..col.len()) else {
            return Ok(None);
        };
        if !head.eq_ignore_ascii_case(col) {
            return Ok(None);
        }
        let rest = rhs[col.len()..].trim_start();
        let negative = match rest.chars().next() {
            Some('+') => false,
            Some('-') => true,
            _ => return Ok(None),
        };
        let operand = rest[1..].trim();
        if !operand.starts_with('$') {
            return Ok(None);
        }
        match Self::parse_literal(operand)? {
            Value::Param(i) => Ok(Some((i, negative))),
            _ => Ok(None),
        }
    }

    fn parse_update(sql: &str) -> Result<Statement> {
        let rest = sql[7..].trim();
        let set_pos = Self::find_top_level_keyword(rest, " SET ")
            .ok_or_else(|| Error::TransactionError("Missing SET in UPDATE".to_string()))?;
        let table_name = rest[..set_pos].trim().to_string();
        let after_set = rest[set_pos + 5..].trim();
        let (assign_str, filter) =
            if let Some(where_pos) = Self::find_top_level_keyword(after_set, " WHERE ") {
                let a = after_set[..where_pos].trim();
                let w = after_set[where_pos + 7..].trim();
                (a, Some(Self::parse_where_clause(w)?))
            } else {
                (after_set, None)
            };

        let mut assignments = Vec::new();
        let mut increments: Vec<(String, i64)> = Vec::new();
        let mut param_increments: Vec<(String, usize, bool)> = Vec::new();
        for part in Self::split_top_level_commas(assign_str) {
            let eq_pos = part.find('=').ok_or_else(|| {
                Error::TransactionError("Missing '=' in UPDATE SET assignment".to_string())
            })?;
            let col = part[..eq_pos].trim().to_string();
            let rhs = part[eq_pos + 1..].trim();
            if let Some((idx, negative)) = Self::parse_param_increment(&col, rhs)? {
                param_increments.push((col, idx, negative));
                continue;
            }
            if let Some(delta) = Self::parse_increment(&col, rhs)? {
                increments.push((col, delta));
                continue;
            }
            let val = Self::parse_literal(rhs)?;
            assignments.push((col, val));
        }

        Ok(Statement::Update {
            table_name,
            assignments,
            increments,
            param_increments,
            filter,
        })
    }

    fn parse_order_by_item(item: &str) -> Result<OrderByClause> {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            return Err(Error::TransactionError(
                "Empty ORDER BY expression".to_string(),
            ));
        }
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut paren_depth = 0usize;
        let mut top_level_tokens: Vec<&str> = Vec::new();
        let mut token_start: Option<usize> = None;

        for (idx, ch) in trimmed.char_indices() {
            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            } else if !in_single_quote && !in_double_quote {
                if ch == '(' {
                    paren_depth += 1;
                } else if ch == ')' {
                    paren_depth = paren_depth.saturating_sub(1);
                }
            }
            if paren_depth == 0 && !in_single_quote && !in_double_quote && ch.is_ascii_whitespace()
            {
                if let Some(start) = token_start.take() {
                    top_level_tokens.push(&trimmed[start..idx]);
                }
            } else if token_start.is_none() {
                token_start = Some(idx);
            }
        }
        if let Some(start) = token_start {
            top_level_tokens.push(&trimmed[start..]);
        }

        if top_level_tokens.is_empty() || top_level_tokens.len() > 2 {
            return Err(Error::TransactionError(format!(
                "Invalid ORDER BY clause: {}",
                trimmed
            )));
        }

        let col_expr = top_level_tokens[0].trim();
        if !Self::is_valid_identifier(col_expr) && !Self::looks_like_aggregate(col_expr) {
            return Err(Error::TransactionError(format!(
                "Invalid ORDER BY expression: {}",
                col_expr
            )));
        }

        let descending = match top_level_tokens.get(1) {
            None => false,
            Some(dir) if dir.eq_ignore_ascii_case("DESC") => true,
            Some(dir) if dir.eq_ignore_ascii_case("ASC") => false,
            Some(other) => {
                return Err(Error::TransactionError(format!(
                    "Invalid ORDER BY direction: {}",
                    other
                )));
            }
        };

        Ok(OrderByClause {
            column_name: col_expr.to_string(),
            descending,
        })
    }

    fn parse_select(sql: &str) -> Result<Statement> {
        let rest_sql = sql[7..].trim();
        let (distinct, rest) = if rest_sql.to_uppercase().starts_with("DISTINCT ") {
            (true, rest_sql[9..].trim())
        } else {
            (false, rest_sql)
        };
        let from_pos = Self::find_top_level_keyword(rest, " FROM ")
            .ok_or_else(|| Error::TransactionError("Missing FROM in SELECT".to_string()))?;
        let proj_str = rest[..from_pos].trim();
        let projection: Vec<String> = Self::split_top_level_commas(proj_str)
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect();
        let mut remainder = rest[from_pos + 6..].trim();

        let mut offset = None;
        if let Some(off_pos) = Self::find_top_level_keyword(remainder, " OFFSET ") {
            let off_str = remainder[off_pos + 8..].trim();
            let n = off_str.parse::<usize>().map_err(|_| {
                Error::TransactionError(format!("Invalid OFFSET value: {}", off_str))
            })?;
            offset = Some(n);
            remainder = remainder[..off_pos].trim();
        }

        let mut limit = None;
        if let Some(lim_pos) = Self::find_top_level_keyword(remainder, " LIMIT ") {
            let lim_str = remainder[lim_pos + 7..].trim();
            let n = lim_str.parse::<usize>().map_err(|_| {
                Error::TransactionError(format!("Invalid LIMIT value: {}", lim_str))
            })?;
            limit = Some(n);
            remainder = remainder[..lim_pos].trim();
        }

        let mut order_by_list = Vec::new();
        if let Some(ob_pos) = Self::find_top_level_keyword(remainder, " ORDER BY ") {
            let ob_str = remainder[ob_pos + 10..].trim();
            if ob_str.is_empty() {
                return Err(Error::TransactionError("Empty ORDER BY clause".to_string()));
            }
            for part in Self::split_top_level_commas(ob_str) {
                order_by_list.push(Self::parse_order_by_item(part)?);
            }
            remainder = remainder[..ob_pos].trim();
        }
        let order_by = order_by_list.first().cloned();

        let mut having = None;
        if let Some(hav_pos) = Self::find_top_level_keyword(remainder, " HAVING ") {
            let hav_str = remainder[hav_pos + 8..].trim();
            if hav_str.is_empty() {
                return Err(Error::TransactionError("Empty HAVING clause".to_string()));
            }
            having = Some(Self::parse_where_clause(hav_str)?);
            remainder = remainder[..hav_pos].trim();
        }

        let mut group_by = Vec::new();
        if let Some(gb_pos) = Self::find_top_level_keyword(remainder, " GROUP BY ") {
            let gb_str = remainder[gb_pos + 10..].trim();
            if gb_str.is_empty() {
                return Err(Error::TransactionError("Empty GROUP BY clause".to_string()));
            }
            for part in Self::split_top_level_commas(gb_str) {
                let col = part.trim();
                if !Self::is_valid_identifier(col) {
                    return Err(Error::TransactionError(format!(
                        "Invalid GROUP BY column expression: {}",
                        col
                    )));
                }
                group_by.push(col.to_string());
            }
            remainder = remainder[..gb_pos].trim();
        }

        let (table_part, filter) =
            if let Some(where_pos) = Self::find_top_level_keyword(remainder, " WHERE ") {
                let t = remainder[..where_pos].trim();
                let cond_str = remainder[where_pos + 7..].trim();
                (t, Some(Self::parse_where_clause(cond_str)?))
            } else {
                (remainder, None)
            };

        let join_kw_info =
            if let Some(p) = Self::find_top_level_keyword(table_part, " LEFT OUTER JOIN ") {
                Some((p, 17, JoinType::LeftOuter))
            } else if let Some(p) = Self::find_top_level_keyword(table_part, " LEFT JOIN ") {
                Some((p, 11, JoinType::LeftOuter))
            } else if let Some(p) = Self::find_top_level_keyword(table_part, " INNER JOIN ") {
                Some((p, 12, JoinType::Inner))
            } else {
                Self::find_top_level_keyword(table_part, " JOIN ").map(|p| (p, 6, JoinType::Inner))
            };

        let (table_name, join) = if let Some((join_pos, kw_len, join_type)) = join_kw_info {
            let left_t = table_part[..join_pos].trim().to_string();
            let after_join = table_part[join_pos + kw_len..].trim();
            let on_pos = Self::find_top_level_keyword(after_join, " ON ").ok_or_else(|| {
                Error::TransactionError("Missing ON keyword in JOIN clause".to_string())
            })?;
            let right_t = after_join[..on_pos].trim().to_string();
            let on_clause = after_join[on_pos + 4..].trim();

            if !Self::is_valid_identifier(&left_t) {
                return Err(Error::TransactionError(format!(
                    "Invalid left table in JOIN: {}",
                    left_t
                )));
            }
            if !Self::is_valid_identifier(&right_t) {
                return Err(Error::TransactionError(format!(
                    "Invalid right table in JOIN: {}",
                    right_t
                )));
            }

            let mut join_ons = Vec::new();
            for part in on_clause.split(" AND ") {
                let part_trimmed = part.trim();
                let mut found_op = None;
                for (op_str, op) in [
                    ("!=", CompareOp::Ne),
                    (">=", CompareOp::Ge),
                    ("<=", CompareOp::Le),
                    ("=", CompareOp::Eq),
                    (">", CompareOp::Gt),
                    ("<", CompareOp::Lt),
                ] {
                    if let Some(pos) = Self::find_top_level_keyword(part_trimmed, op_str) {
                        let left_c = part_trimmed[..pos].trim().to_string();
                        let right_c = part_trimmed[pos + op_str.len()..].trim().to_string();
                        found_op = Some(JoinOn {
                            left_col: left_c,
                            right_col: right_c,
                            op,
                        });
                        break;
                    }
                }
                let join_on = found_op.ok_or_else(|| {
                    Error::TransactionError(format!(
                        "Invalid condition in JOIN ON clause: {}",
                        part_trimmed
                    ))
                })?;
                join_ons.push(join_on);
            }
            (
                left_t,
                Some(JoinClause {
                    join_type,
                    table_name: right_t,
                    on: join_ons,
                }),
            )
        } else {
            if table_part.split_whitespace().count() != 1 || !Self::is_valid_identifier(table_part)
            {
                return Err(Error::TransactionError(format!(
                    "Unexpected clause after FROM: {}",
                    table_part
                )));
            }
            (table_part.to_string(), None)
        };

        Ok(Statement::Select {
            table_name,
            projection,
            filter,
            group_by,
            having,
            order_by,
            order_by_list,
            limit,
            offset,
            distinct,
            join,
        })
    }

    fn parse_delete(sql: &str) -> Result<Statement> {
        let after_from = sql[12..].trim();
        if let Some(where_pos) = Self::find_top_level_keyword(after_from, " WHERE ") {
            let table_name = after_from[..where_pos].trim().to_string();
            let cond_str = after_from[where_pos + 7..].trim();
            let filter = Some(Self::parse_where_clause(cond_str)?);
            Ok(Statement::Delete { table_name, filter })
        } else {
            Ok(Statement::Delete {
                table_name: after_from.to_string(),
                filter: None,
            })
        }
    }

    fn find_top_level_logical_operator(input: &str, op_kw: &str) -> Result<Option<(usize, usize)>> {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut paren_depth: i32 = 0;
        let kw_len = op_kw.len();
        let bytes = input.as_bytes();

        for (idx, ch) in input.char_indices() {
            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            } else if !in_single_quote && !in_double_quote {
                if ch == '(' {
                    paren_depth += 1;
                } else if ch == ')' {
                    paren_depth -= 1;
                    if paren_depth < 0 {
                        return Err(Error::TransactionError(
                            "Unbalanced parentheses in WHERE clause".to_string(),
                        ));
                    }
                } else if paren_depth == 0
                    && input
                        .get(idx..idx + kw_len)
                        .is_some_and(|sub| sub.eq_ignore_ascii_case(op_kw))
                {
                    let valid_before =
                        idx > 0 && (bytes[idx - 1].is_ascii_whitespace() || bytes[idx - 1] == b')');
                    let after_idx = idx + kw_len;
                    let valid_after = after_idx < bytes.len()
                        && (bytes[after_idx].is_ascii_whitespace() || bytes[after_idx] == b'(');
                    if valid_before && valid_after {
                        return Ok(Some((idx, after_idx)));
                    }
                }
            }
        }

        if in_single_quote || in_double_quote {
            return Err(Error::TransactionError(
                "Unclosed string literal in WHERE clause".to_string(),
            ));
        }
        if paren_depth != 0 {
            return Err(Error::TransactionError(
                "Unbalanced parentheses in WHERE clause".to_string(),
            ));
        }
        Ok(None)
    }

    fn has_matching_outer_parens(input: &str) -> bool {
        if !input.starts_with('(') || !input.ends_with(')') {
            return false;
        }
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut paren_depth: i32 = 0;
        for (idx, ch) in input.char_indices() {
            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            } else if !in_single_quote && !in_double_quote {
                if ch == '(' {
                    paren_depth += 1;
                } else if ch == ')' {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        return idx == input.len() - 1;
                    }
                }
            }
        }
        false
    }

    fn is_valid_identifier(ident: &str) -> bool {
        if ident.is_empty() {
            return false;
        }
        for part in ident.split('.') {
            if part.is_empty() {
                return false;
            }
            let mut chars = part.chars();
            let first = chars.next().unwrap();
            if !(first.is_ascii_alphabetic() || first == '_') {
                return false;
            }
            if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return false;
            }
        }
        true
    }

    fn parse_where_clause(cond: &str) -> Result<Expr> {
        Self::parse_where_depth(cond, 0)
    }

    fn parse_where_depth(cond: &str, depth: usize) -> Result<Expr> {
        const MAX_WHERE_DEPTH: usize = 128;
        if depth > MAX_WHERE_DEPTH {
            return Err(Error::TransactionError(
                "WHERE clause too deeply nested".to_string(),
            ));
        }
        let trimmed = cond.trim();
        if trimmed.is_empty() {
            return Err(Error::TransactionError(
                "Empty expression in WHERE clause".to_string(),
            ));
        }

        // 1. Lowest precedence: OR
        if let Some((start, end)) = Self::find_top_level_logical_operator(trimmed, "OR")? {
            let left_str = trimmed[..start].trim();
            let right_str = trimmed[end..].trim();
            if left_str.is_empty() || right_str.is_empty() {
                return Err(Error::TransactionError(
                    "Missing operand for OR in WHERE clause".to_string(),
                ));
            }
            let left = Self::parse_where_depth(left_str, depth + 1)?;
            let right = Self::parse_where_depth(right_str, depth + 1)?;
            return Ok(Expr::Or(Box::new(left), Box::new(right)));
        }

        // 2. Next precedence: AND
        if let Some((start, end)) = Self::find_top_level_logical_operator(trimmed, "AND")? {
            let left_str = trimmed[..start].trim();
            let right_str = trimmed[end..].trim();
            if left_str.is_empty() || right_str.is_empty() {
                return Err(Error::TransactionError(
                    "Missing operand for AND in WHERE clause".to_string(),
                ));
            }
            let left = Self::parse_where_depth(left_str, depth + 1)?;
            let right = Self::parse_where_depth(right_str, depth + 1)?;
            return Ok(Expr::And(Box::new(left), Box::new(right)));
        }

        // 3. Matching outer parentheses
        if Self::has_matching_outer_parens(trimmed) {
            let inner = trimmed[1..trimmed.len() - 1].trim();
            if inner.is_empty() {
                return Err(Error::TransactionError(
                    "Empty parentheses in WHERE clause".to_string(),
                ));
            }
            return Self::parse_where_depth(inner, depth + 1);
        }

        // 4. IS NOT NULL / IS NULL
        let upper = trimmed.to_uppercase();
        if let Some(col_part) = upper.strip_suffix(" IS NOT NULL") {
            let col = trimmed[..col_part.len()].trim();
            if !Self::is_valid_identifier(col) && !Self::looks_like_aggregate(col) {
                return Err(Error::TransactionError(format!(
                    "Invalid column identifier in IS NOT NULL: {}",
                    col
                )));
            }
            return Ok(Expr::IsNotNull(Box::new(Expr::Column(col.to_string()))));
        }
        if let Some(col_part) = upper.strip_suffix(" IS NULL") {
            let col = trimmed[..col_part.len()].trim();
            if !Self::is_valid_identifier(col) && !Self::looks_like_aggregate(col) {
                return Err(Error::TransactionError(format!(
                    "Invalid column identifier in IS NULL: {}",
                    col
                )));
            }
            return Ok(Expr::IsNull(Box::new(Expr::Column(col.to_string()))));
        }

        // 5. Atomic comparison
        for (op_str, op) in [
            ("!=", CompareOp::Ne),
            (">=", CompareOp::Ge),
            ("<=", CompareOp::Le),
            ("=", CompareOp::Eq),
            (">", CompareOp::Gt),
            ("<", CompareOp::Lt),
        ] {
            if let Some(pos) = Self::find_top_level_keyword(trimmed, op_str) {
                let col = trimmed[..pos].trim();
                let rhs = trimmed[pos + op_str.len()..].trim();
                if !Self::is_valid_identifier(col) && !Self::looks_like_aggregate(col) {
                    return Err(Error::TransactionError(format!(
                        "WHERE comparison must be <column> <op> <literal>, invalid column '{}'",
                        col
                    )));
                }
                let lit = Self::parse_literal(rhs)?;
                if op == CompareOp::Eq {
                    return Ok(Expr::Eq(
                        Box::new(Expr::Column(col.to_string())),
                        Box::new(Expr::Literal(lit)),
                    ));
                }
                return Ok(Expr::Compare {
                    op,
                    left: Box::new(Expr::Column(col.to_string())),
                    right: Box::new(Expr::Literal(lit)),
                });
            }
        }
        Err(Error::TransactionError(
            "Expected comparison operator in WHERE clause".to_string(),
        ))
    }

    fn parse_literal(token: &str) -> Result<Value> {
        if token.eq_ignore_ascii_case("NULL") {
            return Ok(Value::Null);
        }
        if token.eq_ignore_ascii_case("TRUE") {
            return Ok(Value::Bool(true));
        }
        if token.eq_ignore_ascii_case("FALSE") {
            return Ok(Value::Bool(false));
        }
        if let Some(digits) = token.strip_prefix('$') {
            if !PARAMS_ALLOWED.with(|f| f.get()) {
                return Err(Error::TransactionError(
                    "Parameter placeholders ($n) need parameter binding (execute_sql_params)"
                        .to_string(),
                ));
            }
            if digits.is_empty()
                || digits.len() > 6
                || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(Error::TransactionError(format!(
                    "Invalid parameter placeholder: {}",
                    token
                )));
            }
            let n: usize = digits.parse().unwrap_or(0);
            if n == 0 {
                return Err(Error::TransactionError(
                    "Parameters are numbered from $1".to_string(),
                ));
            }
            return Ok(Value::Param(n - 1));
        }
        if token.starts_with('\'') || token.starts_with('"') {
            let quote = token.chars().next().unwrap();
            if token.len() < 2 || !token.ends_with(quote) {
                return Err(Error::TransactionError(format!(
                    "Invalid SQL literal: {}",
                    token
                )));
            }
            let inner = &token[1..token.len() - 1];
            let mut out = String::with_capacity(inner.len());
            let mut chars = inner.chars().peekable();
            while let Some(ch) = chars.next() {
                if ch == quote {
                    if chars.peek() == Some(&quote) {
                        chars.next();
                        out.push(quote);
                    } else {
                        return Err(Error::TransactionError(format!(
                            "Invalid SQL literal: {}",
                            token
                        )));
                    }
                } else {
                    out.push(ch);
                }
            }
            return Ok(Value::Text(out));
        }
        if let Ok(n) = token.parse::<i64>() {
            return Ok(Value::Int64(n));
        }
        Err(Error::TransactionError(format!(
            "Invalid SQL literal: {}",
            token
        )))
    }
}
