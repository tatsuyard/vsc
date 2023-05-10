use clap::{App, Arg};
use csv::ReaderBuilder;
use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::{Arc, Mutex};

fn main() {
    match run() {
        Ok(_) => (),
        Err(e) => eprintln!("エラーが発生しました: {}", e),
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let matches = App::new("My CSV Search App")
        .version("1.0")
        .author("Your Name <your.email@example.com>")
        .about("Searches for a string in a CSV file")
        .arg(
            Arg::with_name("csv_file")
                .help("CSVファイルのパス")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("search_string")
                .help("検索文字列")
                .required(true)
                .index(2),
        )
        .get_matches();

    let csv_file = matches.value_of("csv_file").unwrap();
    let search_string = matches.value_of("search_string").unwrap();

    let file = File::open(csv_file)?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;

    let records = Arc::new(Mutex::new(Vec::new()));
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(&buffer[..]);

    rdr.records().par_bridge().for_each(|result| {
        if let Ok(record) = result {
            if record.iter().any(|field| field.contains(search_string)) {
                print!("{} ", record.len());
                records.lock().unwrap().push(record);
            }
        }
    });

    let records = Arc::try_unwrap(records).unwrap().into_inner().unwrap();
    for record in records {
        let formatted = record.iter().collect::<Vec<&str>>().join(",");
        println!("{}", formatted);
    }

    Ok(())
}
