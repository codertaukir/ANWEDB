use mitrdb_orm::*;

#[derive(Debug, PartialEq)]
struct User {
    id: i64,
    email: String,
    age: Option<i32>,
}

#[allow(non_upper_case_globals)]
impl User {
    const id: Col<User, i64> = Col::new("id");
    const email: Col<User, String> = Col::new("email");
    const age: Col<User, Option<i32>> = Col::new("age");
}

impl Model for User {
    const TABLE: &'static str = "users";
    const COLUMNS: &'static [ColumnMeta] = &[
        ColumnMeta::of::<i64>("id").primary_key(),
        ColumnMeta::of::<String>("email").unique(),
        ColumnMeta::of::<Option<i32>>("age"),
    ];
    fn to_row(&self) -> Vec<(&'static str, Value)> {
        vec![
            ("id", self.id.to_value()),
            ("email", self.email.to_value()),
            ("age", self.age.to_value()),
        ]
    }
    fn from_row(r: &RowRef) -> Result<Self> {
        Ok(User {
            id: r.get("id")?,
            email: r.get("email")?,
            age: r.get("age")?,
        })
    }
}

fn u(id: i64, email: &str, age: Option<i32>) -> User {
    User {
        id,
        email: email.to_string(),
        age,
    }
}

#[test]
fn struct_as_schema_roundtrip() {
    let dir = std::env::temp_dir().join(format!("mitrdb_orm_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (db, _) = Server::open(dir.join("t.db"), dir.join("t.wal"), 64).unwrap();

    println!("spec: {}", spec_of(User::COLUMNS));
    db.sync::<User>().unwrap();
    db.sync::<User>().unwrap(); // dobara chalane par bhi theek

    db.insert(&u(1, "a@x.com", Some(25))).unwrap();
    db.insert(&u(2, "b@x.com", Some(30))).unwrap();
    db.insert(&u(3, "c@x.com", Some(40))).unwrap();
    db.insert(&u(4, "d@x.com", None)).unwrap();

    let all = db.all::<User>().order_by(User::id).run().unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(all[3], u(4, "d@x.com", None));

    let older = db
        .find(User::age.gt(26))
        .order_by_desc(User::id)
        .run()
        .unwrap();
    assert_eq!(
        older,
        vec![u(3, "c@x.com", Some(40)), u(2, "b@x.com", Some(30))]
    );

    let one = db.find(User::email.eq("a@x.com")).run().unwrap();
    assert_eq!(one, vec![u(1, "a@x.com", Some(25))]);

    let page = db
        .all::<User>()
        .order_by(User::id)
        .limit(2)
        .offset(1)
        .run()
        .unwrap();
    assert_eq!(page.iter().map(|x| x.id).collect::<Vec<_>>(), vec![2, 3]);

    let _ = std::fs::remove_dir_all(&dir);
}
