use mitrdb_server::Server;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

fn read_log(root: &str) -> Vec<(char, char, i64)> {
    std::fs::read_to_string(format!("{root}/ddl.log"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let mut it = l.split(' ');
            let k = it.next()?.chars().next()?;
            let c = it.next()?.chars().next()?;
            let n = it.next()?.parse().ok()?;
            Some((k, c, n))
        })
        .collect()
}

fn op(s: &Server, f: &mut std::fs::File, c: char, n: i64, sql: &str, must: bool) {
    writeln!(f, "P {c} {n}").unwrap();
    f.sync_data().unwrap();
    let r = s.execute_sql(sql);
    if must {
        if let Err(e) = &r {
            eprintln!("DDL_ERR op={c} n={n} sql={sql} err={e:?}");
            std::process::exit(3);
        }
    }
    writeln!(f, "A {c} {n}").unwrap();
    f.sync_data().unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (mode, root) = (a[1].as_str(), a[2].clone());
    std::fs::create_dir_all(&root).unwrap();
    let s = Server::open(format!("{root}/t.db"), format!("{root}/t.wal"), 256)
        .unwrap()
        .0;
    if mode == "run" {
        let mut n = read_log(&root)
            .iter()
            .map(|x| x.2)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{root}/ddl.log"))
            .unwrap();
        if let Some(cs) = std::env::var("PL_CKPT_SECS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
        {
            let s = std::sync::Arc::new(s);
            let s2 = s.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs_f64(cs));
                    let _ = s2.checkpoint();
                }
            });
            run_loop(&s, &mut f, &mut n);
        }
        run_loop(&s, &mut f, &mut n);
    }
    let mut ev: HashMap<i64, Vec<(char, char)>> = HashMap::new();
    for (k, c, n) in read_log(&root) {
        ev.entry(n).or_default().push((k, c));
    }
    let maxn = ev.keys().copied().max().unwrap_or(-1);
    let (mut bad, mut checked) = (0, 0);
    for (n, e) in &ev {
        checked += 1;
        let t = format!("d{n}");
        let acked = |c: char| e.iter().any(|x| x.0 == 'A' && x.1 == c);
        let pend = match e.last() {
            Some(&('P', c)) => Some(c),
            _ => None,
        };
        let cnt = s.table(&t).find().count();
        let exists = cnt.is_ok();
        let mut why = String::new();
        if acked('D') {
            if exists {
                why = "dropped table still exists".into();
            }
        } else if acked('C') {
            if !exists && pend != Some('D') {
                why = "acked table missing".into();
            }
        } else if exists && pend != Some('C') {
            why = "phantom table".into();
        }
        if why.is_empty() && exists && !acked('D') {
            let c = cnt.unwrap();
            let ok = if acked('I') {
                c == 5
            } else if pend == Some('I') {
                c == 0 || c == 5
            } else {
                c == 0
            };
            if !ok {
                why = format!("rows={c}");
            }
            if acked('A') && s.table(&t).find().columns(&["w"]).run().is_err() {
                why = "acked ADD COLUMN w missing".into();
            }
        }
        if !why.is_empty() {
            bad += 1;
            eprintln!("DDL_BAD table={t} {why} events={e:?}");
        }
    }
    let mut phantom = 0;
    for n in (maxn + 1)..(maxn + 6) {
        if s.table(&format!("d{n}")).find().count().is_ok() {
            phantom += 1;
        }
    }
    let u = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 1_000_000_000;
    let z = format!("z{u}");
    let mut fail = 0;
    if s.execute_sql(&format!("CREATE TABLE {z} (id INT PRIMARY KEY, v INT)"))
        .is_err()
    {
        fail += 1;
    }
    if s.execute_sql(&format!("INSERT INTO {z} VALUES (1, 1)"))
        .is_err()
    {
        fail += 1;
    }
    if s.table(&z).find().count().ok() != Some(1) {
        fail += 1;
    }
    println!("DDL verify tables={checked} bad={bad} phantom={phantom} after_fail={fail}");
    if bad + phantom + fail > 0 {
        std::process::exit(1);
    }
}

fn run_loop(s: &Server, f: &mut std::fs::File, n: &mut i64) -> ! {
    loop {
        let t = format!("d{n}");
        op(
            s,
            f,
            'C',
            *n,
            &format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"),
            true,
        );
        op(
            s,
            f,
            'I',
            *n,
            &format!("INSERT INTO {t} VALUES (0,{n}),(1,{n}),(2,{n}),(3,{n}),(4,{n})"),
            true,
        );
        op(
            s,
            f,
            'X',
            *n,
            &format!("CREATE INDEX ix{n} ON {t} (v)"),
            true,
        );
        op(
            s,
            f,
            'A',
            *n,
            &format!("ALTER TABLE {t} ADD COLUMN w INT"),
            true,
        );
        if *n % 3 == 0 && *n >= 2 {
            let m = *n - 2;
            op(s, f, 'D', m, &format!("DROP TABLE d{m}"), false);
        }
        println!("RUNNING n={n}");
        *n += 1;
    }
}
