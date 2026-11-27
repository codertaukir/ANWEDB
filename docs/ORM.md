# mitrdb-orm: struct = schema

Write a struct; the table is created and kept in sync for you. No migration files.

```rust
use mitrdb_orm::*;

#[derive(Model, Debug)]
#[model(table = "users")]            // default: lowercase struct name + "s"
struct User {
    #[col(primary_key, auto)] id: i64,
    #[col(unique)] email: String,
    #[col(index)] name: String,
    age: Option<i32>,                // Option<T> = nullable
}

let (db, _) = Server::open("a.db", "a.wal", 64)?;   // db path, wal path, buffer pool frames
db.sync::<User>()?;                                 // create or update the table
```

## Types

`i64`, `i32`, `String`, `bool`, `Vec<u8>`, `Option<T>`, `Timestamp` (i64 microseconds since epoch), `f64`.

## `#[col(...)]` options

| Option | Meaning |
|---|---|
| `primary_key` | Primary key (one per struct). |
| `auto` | Auto-increment id (needs `primary_key`, type `i64`). Ids are never reused, even after deletes. |
| `unique` | Unique index. |
| `index` | Non-unique index. |
| `created_at` / `updated_at` | Set to now on insert; `updated_at` is refreshed on every update. |
| `references = "table.col"` | Foreign key (RESTRICT by default). The parent column must be its primary key. Add `on_delete = "cascade"` for ON DELETE CASCADE (cascade is not atomic). |
| `default = ..` | Schema DEFAULT (stored in `mitrdb_default`). |
| `check = ">= 0"` | CHECK constraint. Ops: `= != > < >= <=` and `like 'A%'`. Repeat `check` for several. |

## Insert

```rust
db.insert(&user)?;                  // rows affected
let id = db.insert_get_id(&user)?;  // returns the generated id
```

The value of an `auto` field is ignored (pass 0).

## Query

```rust
db.find(User::age.gt(18)).order_by(User::id).limit(10).offset(2).run()?;
db.all::<User>().run()?;
db.find_one(User::email.eq("a@x.com"))?;
db.find_by_id::<User, i64>(1)?;
db.count::<User>()?;
db.count_where(User::age.ge(18))?;
db.find(User::name.like("A%")).run()?;   // % = any run of chars, _ = one char
```

Conditions: `eq ne gt lt ge le`, `like` (String / Option<String> only), `in_list`, `between` (use with `filter_between`), `is_null`, `is_not_null`.

`filter(..)` calls are combined with AND. For OR use `filter_any` / `Cond.or()` (OR groups). Model-level options `unique_together` and `index` create composite indexes (max 4 columns; unique constraint and leftmost-prefix equality only).

## Update / Delete

```rust
db.update::<User>().set(User::name, "x").filter(User::id.eq(1)).run()?;
db.delete::<User>().filter(User::id.eq(1)).run()?;
// without a filter both return an error; call .all() to affect every row
// a filter or filter_any counts as a filter; an empty OR group (in_list([])) does NOT, so the table stays safe
```

## Aggregates and pagination

- `sum`, `avg`, `min`, `max` (int, timestamp, float). Integer `avg` truncates; float `sum`/`avg` are real f64.
- `group_count(col)` and `group_count_top(col, min_count, by_count_desc, top)`.
- `page_after(col, last, n)` = `col > last ORDER BY col LIMIT n`; `page_before(col, first, n)` is the reverse. Use the primary key (or another unique column) as cursor. On a NON-UNIQUE column rows that share the cursor value are skipped (measured: 35 of 50 rows seen).

## Relations

`belongs_to`, `has_many`, `load_many` are plain queries (no JOIN eager loading). `load_many` works in chunks of 50 ids (WHERE depth limit 128). Separate calls are not one snapshot unless wrapped in an explicit transaction.

## TextStore (long text)

`TEXT` and `BYTES` values up to 16 KB (16,384 bytes) are stored natively in the database via transactional overflow page chains. For larger payloads (e.g., 140 KB+ documents or media), use `TextStore` (`put` / `get` / `delete` / `put_and`): the text goes to an external file while the row keeps a reference (not atomic with the WAL, in-process only).

## TCP clients

Set `TCP_NODELAY` (`set_nodelay(true)`) and send length + payload in one write (431 us vs 41 ms per statement measured). Raw SQL INSERT/UPDATE/DELETE over TCP on tables with FK / CHECK / DEFAULT rules is refused; use the ORM.

## Concurrent sessions (Axum / async worker pools)

For multi-threaded web servers where concurrent requests run explicit transactions, bind a session-scoped ORM handle with `db.session_orm(session_id)` (or `.in_session(session_id)` on individual builders):

```rust
let sid = db.create_session();
let sdb = db.session_orm(sid);
db.execute_in_session(sid, "BEGIN")?;
let id = sdb.insert_get_id(&user)?;
let posts = sdb.find(Post::user_id.eq(id)).limit(10).run()?;
db.execute_in_session(sid, "COMMIT")?;
db.close_session(sid)?;
```

## Transactions and savepoints

```rust
db.execute_sql("BEGIN")?;
db.execute_sql("SAVEPOINT a")?;
db.execute_sql("ROLLBACK TO a")?;   // undo writes made after `a`
db.execute_sql("RELEASE a")?;
db.execute_sql("COMMIT")?;
```

## Concurrency

The engine is multi-threaded. Auto-id allocation and foreign-key checks are safe across
threads of one process (see `tests/auto_concurrent.rs`, `tests/fk_race.rs`).

## Example

`cargo run --release -p mitrdb-orm --example quickstart`

See also [LIMITATIONS.md](LIMITATIONS.md).
