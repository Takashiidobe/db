use std::collections::BTreeMap;
use std::env::args;
use std::fs::{self, OpenOptions};

use db::db::{deserialize, DB};

use db::wal::{deserialize_wal, WALRecord, WAL};
use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor, EditMode, Result};

fn main() -> Result<()> {
    let args: Vec<_> = args().collect();
    let file_name = if args.len() > 1 {
        args[1].to_string()
    } else {
        "test".to_string()
    };

    let mut rl = DefaultEditor::with_config(Config::builder().edit_mode(EditMode::Vi).build())?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    let db_file_name = format!("{file_name}.1.db");
    let wal_file_name = format!("{file_name}.1.wal");

    let mut db = if fs::exists(&db_file_name).unwrap() {
        let page_bytes = fs::read(&db_file_name).unwrap();
        let pages = deserialize(page_bytes);
        let wal_bytes = fs::read(&wal_file_name).unwrap();
        let wal_records = deserialize_wal(&wal_bytes);

        let mut wal_cache = BTreeMap::new();

        for record in wal_records {
            match record {
                WALRecord::Insert(id, val) => {
                    wal_cache.insert(id, val);
                }
                WALRecord::Delete(id) => {
                    wal_cache.remove(&id);
                }
            }
        }

        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_file_name)
            .unwrap();
        let wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(wal_file_name)
            .unwrap();
        DB {
            pages,
            file: db_file,
            wal: WAL {
                file: wal_file,
                records: wal_cache,
            },
            epoch: 1,
        }
    } else {
        DB::new(&file_name)
    };

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                if line.starts_with("insert ") {
                    let copy = line.strip_prefix("insert ").unwrap();
                    let nums: Vec<u32> = copy.split(", ").map(|x| x.parse().unwrap()).collect();
                    db.insert(nums[0].try_into().unwrap(), nums[1]);
                }
                if line.starts_with("get ") {
                    let copy = line.strip_prefix("get ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.get(id.try_into().unwrap()) {
                        println!("{val}");
                    } else {
                        println!("Key {id} not found.");
                    }
                }
                if line.starts_with("delete ") {
                    let copy = line.strip_prefix("delete ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.remove(id.try_into().unwrap()) {
                        println!("removed: {val}");
                    } else {
                        println!("Key {id} not found.");
                    }
                }
                if line.starts_with("show") {
                    println!("{:?}", db.pages);
                }
                if line.trim() == "exit" {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) | Err(_) => {
                break;
            }
        }
    }
    drop(db);
    rl.save_history("history.txt")
}
