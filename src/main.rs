use csv::ReaderBuilder;
use rayon::prelude::*;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::{Arc, Mutex};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let csv_file = &args[1];
    let search_string = &args[2];

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
        println!("{:?}", record);
    }

    Ok(())
}
