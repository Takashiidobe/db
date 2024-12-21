use db::db::DB;

use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor, EditMode, Result};

fn main() -> Result<()> {
    let mut rl = DefaultEditor::with_config(Config::builder().edit_mode(EditMode::Vi).build())?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let mut db = DB::new("file.out");
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                if line.starts_with("insert ") {
                    let copy = line.strip_prefix("insert ").unwrap();
                    let nums: Vec<u32> = copy.split(", ").map(|x| x.parse().unwrap()).collect();
                    db.insert(nums[0], nums[1]);
                    dbg!(&db);
                }
                println!("Line: {}", line);
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) | Err(_) => {
                break;
            }
        }
    }
    drop(db);
    rl.save_history("history.txt")
}
