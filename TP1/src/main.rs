use std::env;
use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;
use serde::Serialize;
use std::io::Write;
use std::collections::HashMap;
use std::cmp::Ordering;

#[derive(Serialize, Debug)]
struct row_weapon {
    name: String,
    amount_deaths: u32,
    accumulator_distance: f32,
}

#[derive(Serialize)]
struct weapon {
    name: String,
    amount_deaths: u32,
    percentage_deaths: f32,
    average_distance: f32,
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

    let mut weapons: HashMap<String, row_weapon> = HashMap::new();

    rdr.records().next();
    for result in rdr.records() {
        let record = result?;
        let weapon_name = record.get(0).unwrap().to_string();

        if weapons.contains_key(&weapon_name) {
            if let Some(current_weapon) = weapons.get_mut(&weapon_name) {
                current_weapon.amount_deaths += 1;
                current_weapon.accumulator_distance += {
                    let kill_x_position = record.get(3).and_then(|s| s.parse::<f32>().ok());
                    let kill_y_position = record.get(4).and_then(|s| s.parse::<f32>().ok());
                    let victim_x_position = record.get(10).and_then(|s| s.parse::<f32>().ok());
                    let victim_y_position = record.get(11).and_then(|s| s.parse::<f32>().ok());
                
                    match (kill_x_position, kill_y_position, victim_x_position, victim_y_position) {
                        (Some(kill_x_position), Some(kill_y_position), Some(victim_x_position), Some(victim_y_position)) => {
                            let distance = ((kill_x_position - victim_x_position).powi(2) + (kill_y_position - victim_y_position).powi(2)).sqrt();
                            format!("{:.2}", distance).parse::<f32>().unwrap_or(0.0)
                        }
                        _ => 0.0,
                    }
                };
            }

        } else {

            let row_weapon = row_weapon {
                name: weapon_name.clone(),
                amount_deaths: 1,
                accumulator_distance: {
                    let kill_x_position = record.get(3).and_then(|s| s.parse::<f32>().ok());
                    let kill_y_position = record.get(4).and_then(|s| s.parse::<f32>().ok());
                    let victim_x_position = record.get(10).and_then(|s| s.parse::<f32>().ok());
                    let victim_y_position = record.get(11).and_then(|s| s.parse::<f32>().ok());
                
                    match (kill_x_position, kill_y_position, victim_x_position, victim_y_position) {
                        (Some(kill_x_position), Some(kill_y_position), Some(victim_x_position), Some(victim_y_position)) => {
                            let distance = ((kill_x_position - victim_x_position).powi(2) + (kill_y_position - victim_y_position).powi(2)).sqrt();
                            format!("{:.2}", distance).parse::<f32>().unwrap_or(0.0)
                        }
                        _ => 0.0,
                    }
                },
            };

            weapons.insert(weapon_name.clone(), row_weapon);
        }
    }

    let mut top_weapons: Vec<weapon> = Vec::new();

    for (key, value) in weapons.iter() {
        let average_distance = value.accumulator_distance / value.amount_deaths as f32;
        println!("Weapon: {}, Amount of deaths: {}, Average distance: {:.2}", key, value.amount_deaths, average_distance);

        let weapon = weapon {
            name: value.name.clone(),
            amount_deaths: value.amount_deaths,
            percentage_deaths: format!("{:.2}", (value.amount_deaths as f32 / total_kills as f32) * 100.0).parse::<f32>().unwrap(),
            average_distance: average_distance,
        };

        top_weapons.push(weapon);
    }

    // Sort the weapons by the amount of kills (descending), then by the name (ascending).
    top_weapons.sort_by(|a, b| {
        b.amount_deaths.cmp(&a.amount_deaths).then_with(|| a.name.cmp(&b.name))
    });

    println!("\nList of top weapons\n");

    // Get the top 10.
    for weapon in top_weapons.iter().take(10) {
        println!("Weapon: {}, Percentage of deaths: {}, Average distance: {:.2}", weapon.name, weapon.percentage_deaths, weapon.average_distance);
    }

    Ok(())
}