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
| `references = "table.col"` | Foreign key (RESTRICT). The parent column must be its primary key. |
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

Conditions: `eq ne gt lt ge le`, and `like` (String / Option<String> only). Conditions are combined with AND.

## Update / Delete

```rust
db.update::<User>().set(User::name, "x").filter(User::id.eq(1)).run()?;
db.delete::<User>().filter(User::id.eq(1)).run()?;
// without a filter both return an error; call .all() to affect every row
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
