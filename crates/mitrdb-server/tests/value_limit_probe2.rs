use mitrdb_server::{Server, val};

fn open(tag: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("mitrdb_vl2_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Server::open(dir.join("t.db"), dir.join("t.wal"), 64)
        .unwrap()
        .0
}

#[test]
fn value_limit_probe2() {
    let s = open("p2");
    let t = s.table("one");
    t.sync("id int primary key, data text").unwrap();
    let mut id = 0i64;

    // 1) Hindi: 3 bytes/char. chars vs bytes
    for n in (1000usize..=1500)
        .step_by(25)
        .chain([1330, 1331, 1332, 1333, 1334, 1335])
    {
        id += 1;
        let r = t.insert(&[("id", val(id)), ("data", val("अ".repeat(n)))]);
        eprintln!("VL hindi chars={n} bytes={} ok={}", n * 3, r.is_ok());
    }

    // 2) ASCII exact threshold (bisect 3900 ok .. 4050 fail)
    let (mut lo, mut hi) = (3900usize, 4050usize);
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        id += 1;
        let r = t.insert(&[("id", val(id)), ("data", val("x".repeat(mid)))]);
        if r.is_ok() { lo = mid } else { hi = mid }
    }
    eprintln!("VL ascii_threshold max_ok={lo} first_fail={hi}");

    // 3) Hindi exact threshold in chars (1300 ok .. 1400 fail)
    let (mut lo, mut hi) = (1300usize, 1400usize);
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        id += 1;
        let r = t.insert(&[("id", val(id)), ("data", val("अ".repeat(mid)))]);
        if r.is_ok() { lo = mid } else { hi = mid }
    }
    eprintln!(
        "VL hindi_threshold max_ok_chars={lo} first_fail_chars={hi} bytes_ok={}",
        lo * 3
    );

    // 4) Oversize error text (Debug + Display)
    for n in [4100usize, 5000, 100_000] {
        id += 1;
        let r = t.insert(&[("id", val(id)), ("data", val("x".repeat(n)))]);
        match r {
            Ok(_) => eprintln!("VL err n={n} UNEXPECTED_OK"),
            Err(e) => eprintln!("VL err n={n} debug={:?} display={}", e, e),
        }
    }

    // 5) Row still absent / table still usable after failure
    id += 1;
    let r = t.insert(&[("id", val(id)), ("data", val("ok"))]);
    eprintln!("VL after_fail small_insert_ok={}", r.is_ok());
}
