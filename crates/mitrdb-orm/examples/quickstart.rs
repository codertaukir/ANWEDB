use mitrdb_orm::*;

#[derive(Model, Debug)]
struct User {
    #[col(primary_key, auto)]
    id: i64,
    #[col(unique)]
    email: String,
    name: String,
    age: Option<i32>,
    #[col(check = ">= 0")]
    score: i64,
    #[col(created_at)]
    created_at: Timestamp,
    #[col(updated_at)]
    updated_at: Timestamp,
}

fn user(email: &str, name: &str, age: Option<i32>, score: i64) -> User {
    User {
        id: 0,
        email: email.into(),
        name: name.into(),
        age,
        score,
        created_at: Timestamp(0),
        updated_at: Timestamp(0),
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("mitrdb_quickstart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("q.db"), dir.join("q.wal"), 64).unwrap();

    db.sync::<User>().unwrap(); // create the table, or bring it in line with the struct

    let id = db
        .insert_get_id(&user("a@x.com", "Amit", Some(30), 10))
        .unwrap();
    db.insert(&user("b@x.com", "Anil", Some(17), 5)).unwrap();
    println!("new id = {id}");

    let adults = db.find(User::age.gt(18)).order_by(User::id).run().unwrap();
    println!("adults = {adults:?}");

    let a_names = db.find(User::name.like("A%")).run().unwrap();
    println!("names starting with A = {}", a_names.len());

    db.update::<User>()
        .set(User::name, "Amit K")
        .filter(User::id.eq(id))
        .run()
        .unwrap();
    println!(
        "after update = {:?}",
        db.find_by_id::<User, i64>(id).unwrap()
    );

    // CHECK: a negative score is rejected
    println!(
        "negative score -> {:?}",
        db.insert(&user("c@x.com", "C", None, -1)).is_err()
    );

    // savepoint
    db.execute_sql("BEGIN").unwrap();
    db.insert(&user("d@x.com", "D", None, 1)).unwrap();
    db.execute_sql("SAVEPOINT s1").unwrap();
    db.insert(&user("e@x.com", "E", None, 1)).unwrap();
    db.execute_sql("ROLLBACK TO s1").unwrap();
    db.execute_sql("COMMIT").unwrap();
    println!("total users = {}", db.count::<User>().unwrap());
}
