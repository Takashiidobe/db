use std::env::args;
use std::fs;

use db::db::{deserialize, DB};

use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor, EditMode, Result};

fn main() -> Result<()> {
    let args: Vec<_> = args().collect();
    let file_name = if args.len() > 1 {
        args[1].to_string()
    } else {
        "file.out".to_string()
    };

    let mut rl = DefaultEditor::with_config(Config::builder().edit_mode(EditMode::Vi).build())?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let mut db = if fs::exists(&file_name).unwrap() {
        let bytes = fs::read(&file_name).unwrap();
        let pages = deserialize(bytes);
        DB { pages, file_name }
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
                    db.insert(nums[0], nums[1]);
                }
                if line.starts_with("get ") {
                    let copy = line.strip_prefix("get ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.get(id) {
                        println!("{val}");
                    } else {
                        println!("Key {id} not found.");
                    }
                }
                if line.starts_with("delete ") {
                    let copy = line.strip_prefix("delete ").unwrap();
                    let id: u32 = copy.parse().unwrap();
                    if let Some(val) = db.remove(id) {
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
