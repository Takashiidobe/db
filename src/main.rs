use std::collections::BTreeMap;
use std::env::args;
use std::fs::{self, OpenOptions};

use db::db::{deserialize, DB};

use db::row::{schema_from_bytes, RowType, RowVal, Schema};
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
    let schema_file_name = format!("{file_name}.1.schema");

    let mut db = if fs::exists(&db_file_name).unwrap() {
        let schema_bytes = fs::read(&schema_file_name).unwrap();
        let schema = schema_from_bytes(&schema_bytes);
        let schema_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(schema_file_name)
            .unwrap();
        let schema = Schema {
            schema,
            file: schema_file,
        };

        let page_bytes = fs::read(&db_file_name).unwrap();
        let pages = deserialize(page_bytes, &schema.schema);

        let wal_bytes = fs::read(&wal_file_name).unwrap();
        let wal_records = deserialize_wal(&wal_bytes, &schema.schema);

        let mut wal_cache = BTreeMap::new();

        for record in &wal_records {
            match record {
                WALRecord::Insert(id, val) => {
                    wal_cache.insert(*id, val.to_vec());
                }
                WALRecord::Delete(id) => {
                    wal_cache.remove(id);
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
        let mut db = DB {
            pages,
            file: db_file,
            wal: WAL {
                file: wal_file,
                records: wal_cache,
            },
            epoch: 1,
            schema,
        };
        db.sync();

        db
    } else {
        let schema_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(schema_file_name)
            .unwrap();
        let schema = Schema {
            schema: vec![RowType::Id, RowType::U32, RowType::Bytes, RowType::Bool],
            file: schema_file,
        };

        DB::new(&file_name, schema)
    };

    let help_string = r#"Commands:
Insert takes two u32s, comma delimited, and inserts them into the DB:
insert $id, $val
Get takes a u32, the id of the tuple to fetch:
get $id
Delete takes a u32, the id of the tuple to delete:
delete $id
Sync merges the WAL and pages together, and saves to disk. The WAL is then cleared.
sync (clears the WAL and saves the DB to disk).
Show shows the state of the database.
show (shows database info)
Exit quits the repl. This can also be done with CTRL-C or CTRL-D.
exit (quits the repl)"#;

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                if line.trim() == "?" {
                    println!("{}", help_string);
                }
                if line.starts_with("insert ") {
                    let copy = line.strip_prefix("insert ").unwrap();
                    let vals: Vec<&str> = copy.split(", ").collect();
                    let id = vals[0].parse().unwrap();
                    let vals = parse_vals(&vals[1..]);
                    if verify_insert(&vals, &db.schema.schema) {
                        db.insert(id, &vals);
                    } else {
                        println!("Schema did not match, rejecting insert.");
                    }
                }
                if line.starts_with("get ") {
                    let copy = line.strip_prefix("get ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.get(id.try_into().unwrap()) {
                        let mut res = String::new();
                        res.push_str(&format!("{id}: ["));
                        for v in val {
                            res.push_str(&v.to_string());
                            res.push_str(", ");
                        }
                        res.pop();
                        res.pop();
                        res.push(']');
                        println!("{}", res);
                    } else {
                        println!("Key {id} not found.");
                    }
                }
                if line.starts_with("delete ") {
                    let copy = line.strip_prefix("delete ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.remove(id.try_into().unwrap()) {
                        let mut res = String::new();
                        res.push_str(&format!("Removing {id}: ["));
                        for v in val {
                            res.push_str(&v.to_string());
                            res.push_str(", ");
                        }
                        res.pop();
                        res.pop();
                        res.push(']');
                        println!("{}", res);
                    } else {
                        println!("Key {id} not found.");
                    }
                }
                if line.starts_with("show") {
                    println!("Pages: ");
                    println!("{:?}", db.pages);
                    println!("WAL: ");
                    println!("{:?}", db.wal);
                    println!("Schema: ");
                    println!("{:?}", db.schema);
                }
                if line.starts_with("sync") {
                    db.sync();
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

pub fn verify_insert(vals: &[RowVal], schema: &[RowType]) -> bool {
    if vals.len() != schema.len() - 1 {
        return false;
    }
    for i in 0..vals.len() {
        match (&vals[i], &schema[i + 1]) {
            (RowVal::Id(_), RowType::Id)
            | (RowVal::U32(_), RowType::U32)
            | (RowVal::Bytes(_), RowType::Bytes)
            | (RowVal::Bool(_), RowType::Bool) => continue,
            _ => return false,
        }
    }
    true
}

pub fn parse_vals(vals: &[&str]) -> Vec<RowVal> {
    let mut res = vec![];
    for val in vals {
        let trimmed = val.trim();
        // string
        if trimmed.starts_with('"') {
            let bytes = trimmed
                .strip_prefix('"')
                .unwrap()
                .strip_suffix('"')
                .unwrap();
            res.push(RowVal::Bytes(bytes.into()));
        } else if trimmed == "false" {
            res.push(RowVal::Bool(false));
        } else if trimmed == "true" {
            res.push(RowVal::Bool(true));
        } else {
            res.push(RowVal::U32(trimmed.parse().unwrap()));
        }
    }
    res
}
