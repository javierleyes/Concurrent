use std::env;
use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;
use serde::Serialize;
use std::io::Write;
use std::collections::HashMap;

#[derive(Serialize)]
struct killer {
    padron: u32,
    top_killers: u8,
    top_weapons: bool,
}

#[derive(Serialize)]
struct weapon {
    padron: u32,
    top_killers: u8,
    top_weapons: bool,
}

#[derive(Serialize)]
struct output_json {
    padron: u32,
    top_killers: u8,
    top_weapons: bool,
}

// fn main() -> std::io::Result<()> {
//     let output_json = output_json {
//         padron: 94455,
//         top_killers: 30,
//         top_weapons: false,
//     };

//     // Serialize the struct to a JSON string
//     let json = serde_json::to_string(&output_json).expect("Failed to serialize");

//     // Write the JSON string to a file
//     let mut file = File::create("person.json")?;
//     file.write_all(json.as_bytes())?;

//     println!("JSON written to person.json");

//     Ok(())
// }

fn read_csv(file_path: &str) -> Result<csv::Reader<File>, Box<dyn Error>> {
    let file = File::open(file_path)?;

    // Create a CSV reader
    let rdr = ReaderBuilder::new().from_reader(file);

    Ok(rdr)
}

fn get_arguments() -> (String, String, String) {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!("Usage: cargo run <input-path> <num-threads> <output-file-name>");
        std::process::exit(1);
    }

    let file_path = args[1].clone();
    let amount_workers = args[2].clone();
    let output_json_name = args[3].clone();

    (file_path, amount_workers, output_json_name)
}

fn main() -> Result<(), Box<dyn Error>> {
    let (file_path, amount_workers, output_json_name) = get_arguments();

    println!("File path: {}", file_path);
    println!("Amount of workers: {}", amount_workers);
    println!("Output JSON name: {}", output_json_name);

    let file = File::open(file_path)?;

    // Create a mutable CSV reader
    let mut rdr = ReaderBuilder::new().from_reader(file);

    // Count the total of kills.
    let total_kills = rdr.records().count();
    println!("Total kills: {}", total_kills);

    // Go back to the start of the file.
    rdr.seek(csv::Position::new())?;

    let mut scores = HashMap::new();

    rdr.records().next();
    for result in rdr.records() {
        let record = result?;
        let key = record.get(0).unwrap().to_string();
        if scores.contains_key(&key) {
            let current_score = scores.get(&key).unwrap();
            scores.insert(key.clone(), current_score + 1);
        } else {
            scores.insert(key.clone(), 1);
        }
        // println!("{:?}", record);
    }

    println!("{:?}", scores);

    Ok(())
}