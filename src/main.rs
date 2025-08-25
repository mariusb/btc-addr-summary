
use rusqlite::{params, Connection, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use regex::Regex;
use chrono::NaiveDateTime;

fn main() -> Result<()> {
    let conn = Connection::open("summary.db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS summary (
            id INTEGER PRIMARY KEY,
            datetime TEXT NOT NULL UNIQUE,
            date TEXT NOT NULL,
            with_balance INTEGER NOT NULL,
            without_balance INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS processed_lines (
            id INTEGER PRIMARY KEY,
            line_hash TEXT NOT NULL UNIQUE
        )",
        [],
    )?;

    let file = File::open("summary.log").unwrap();
    let reader = BufReader::new(file);

    let date_regex = Regex::new(r"Summary for (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})").unwrap();
    let with_balance_regex = Regex::new(r"Total with balance: (\d+)").unwrap();
    let without_balance_regex = Regex::new(r"Total without balance: (\d+)").unwrap();

    let mut lines = reader.lines().peekable();

    while let Some(Ok(line1)) = lines.next() {
        let line_hash = format!("{:x}", md5::compute(&line1));
        let mut stmt = conn.prepare("SELECT 1 FROM processed_lines WHERE line_hash = ?1")?;
        let exists = stmt.exists(params![line_hash])?;

        if !exists {
            if let Some(caps) = date_regex.captures(&line1) {
                if let (Some(Ok(line2)), Some(Ok(line3))) = (lines.next(), lines.next()) {
                    if with_balance_regex.is_match(&line2) && without_balance_regex.is_match(&line3) {
                        let datetime_str = &caps[1];
                        let datetime = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                        let previous_day_date = (datetime.date() - chrono::Duration::days(1)).to_string();

                        let with_balance = with_balance_regex.captures(&line2).and_then(|c| c[1].parse::<i32>().ok()).unwrap_or(0);
                        let without_balance = without_balance_regex.captures(&line3).and_then(|c| c[1].parse::<i32>().ok()).unwrap_or(0);

                        conn.execute(
                            "INSERT OR IGNORE INTO summary (datetime, date, with_balance, without_balance) VALUES (?1, ?2, ?3, ?4)",
                            params![datetime.to_string(), previous_day_date, with_balance, without_balance],
                        )?;

                        conn.execute(
                            "INSERT INTO processed_lines (line_hash) VALUES (?1)",
                            params![line_hash],
                        )?;
                    }
                } else {
                    // This is a single summary line, we can ignore it or handle it differently
                    // For now, we just ignore it.
                }
            }
        }
    }

    println!("Processing complete.");

    Ok(())
}
