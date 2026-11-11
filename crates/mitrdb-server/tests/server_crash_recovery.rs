#![allow(
    clippy::ptr_arg,
    clippy::collapsible_if,
    clippy::type_complexity,
    unused_imports
)]
use mitrdb_server::{Server, val};
use std::io::Write;
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mitrdb_scr_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// Server/BufferPool par Drop nahi hai: drop = bina checkpoint/flush ke band = crash jaisa.
fn open(d: &std::path::Path) -> Server {
    Server::open(d.join("t.db"), d.join("t.wal"), 64).unwrap().0
}

fn count(s: &Server, g: Option<i64>) -> usize {
    let t = s.table("t");
    match g {
        Some(g) => t.find().filter("g", "=", val(g)).count().unwrap(),
        None => t.find().count().unwrap(),
    }
}

fn setup(s: &Server) {
    s.table("t")
        .sync("id int primary key, g int index")
        .unwrap();
}

fn insert_range(s: &Server, from: i64, to: i64, g: i64) {
    let t = s.table("t");
    s.execute_sql("BEGIN").unwrap();
    for i in from..to {
        t.insert(&[("id", val(i)), ("g", val(g))]).unwrap();
    }
    s.execute_sql("COMMIT").unwrap();
}

#[test]
fn committed_data_survives_crash_without_checkpoint() {
    let d = dir("commit");
    {
        let s = open(&d);
        setup(&s);
        insert_range(&s, 0, 500, 1);
    } // crash
    let s = open(&d);
    assert_eq!(count(&s, None), 500);
    assert_eq!(count(&s, Some(1)), 500);
}

#[test]
fn uncommitted_txn_is_not_visible_after_crash() {
    let d = dir("uncommitted");
    {
        let s = open(&d);
        setup(&s);
        insert_range(&s, 0, 100, 1);
        let t = s.table("t");
        s.execute_sql("BEGIN").unwrap();
        for i in 100..1500i64 {
            t.insert(&[("id", val(i)), ("g", val(2))]).unwrap();
        }
        // no COMMIT: crash
    }
    let s = open(&d);
    assert_eq!(count(&s, None), 100);
    assert_eq!(count(&s, Some(2)), 0);
    assert_eq!(count(&s, Some(1)), 100);
}

#[test]
fn chunked_index_survives_crash_and_uncommitted_change_is_undone() {
    let d = dir("chunked");
    {
        let s = open(&d);
        setup(&s);
        insert_range(&s, 0, 2000, 7);
        let t = s.table("t");
        assert_eq!(t.delete().filter("id", "<", val(500)).run().unwrap(), 500);
    } // crash 1
    {
        let s = open(&d);
        assert_eq!(count(&s, None), 1500);
        assert_eq!(count(&s, Some(7)), 1500);
        let t = s.table("t");
        s.execute_sql("BEGIN").unwrap();
        t.update()
            .set("g", val(8))
            .filter("id", "<", val(1000))
            .run()
            .unwrap();
        // no COMMIT: crash 2
    }
    let s = open(&d);
    assert_eq!(count(&s, None), 1500);
    assert_eq!(count(&s, Some(7)), 1500);
    assert_eq!(count(&s, Some(8)), 0);
    let t = s.table("t");
    assert_eq!(t.find().filter("g", ">", val(0)).count().unwrap(), 1500);
}

#[test]
fn torn_wal_tail_is_truncated_and_data_kept() {
    let d = dir("torn");
    {
        let s = open(&d);
        setup(&s);
        insert_range(&s, 0, 300, 3);
    }
    let mut wals: Vec<PathBuf> = std::fs::read_dir(&d)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_file() && p.file_name().unwrap().to_string_lossy().contains("wal"))
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .collect();
    wals.sort();
    assert!(
        !wals.is_empty(),
        "WAL file nahi mili: {:?}",
        std::fs::read_dir(&d).unwrap().collect::<Vec<_>>()
    );
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(wals.last().unwrap())
        .unwrap();
    f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03])
        .unwrap();
    f.sync_all().unwrap();
    drop(f);
    let s = open(&d);
    assert_eq!(count(&s, None), 300);
    insert_range(&s, 300, 350, 3); // recovery ke baad write bhi chalna chahiye
    assert_eq!(count(&s, Some(3)), 350);
}

#[test]
fn repeated_crash_restart_with_checkpoint_in_middle() {
    let d = dir("repeat");
    {
        let s = open(&d);
        setup(&s);
        insert_range(&s, 0, 400, 5);
        s.checkpoint().unwrap();
        insert_range(&s, 400, 800, 5);
    }
    for _ in 0..3 {
        let s = open(&d); // har baar recovery, phir crash
        assert_eq!(count(&s, None), 800);
        assert_eq!(count(&s, Some(5)), 800);
    }
    let s = open(&d);
    s.checkpoint().unwrap();
    insert_range(&s, 800, 900, 5);
    drop(s);
    let s = open(&d);
    assert_eq!(count(&s, Some(5)), 900);
}
