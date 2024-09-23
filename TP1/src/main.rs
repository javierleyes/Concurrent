mod models;

use models::{
    Killer, KillerStats, OutputJson, RowKiller, RowWeapon, Top10Results, Weapon, WeaponStats,
};

use csv::ReaderBuilder;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::Write;
use std::io::{self};
use std::time::Instant;

fn get_arguments() -> (String, usize, String) {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!("Usage: cargo run <input-path> <num-threads> <output-file-name>");
        std::process::exit(1);
    }

    let file_path = args[1].clone();
    let output_json_name = args[3].clone();
    let amount_workers = match args[2].parse::<usize>() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Error: <num-threads> must be a valid integer.");
            std::process::exit(1);
        }
    };

    (file_path, amount_workers, output_json_name)
}

fn get_files_in_directory(dir_path: &str) -> io::Result<Vec<String>> {
    let mut files: Vec<String> = Vec::new();

    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        let path_str = match path.to_str() {
            Some(s) => s.to_string(),
            None => {
                eprintln!("Error: invalid path.");
                std::process::exit(1);
            }
        };

        files.push(path_str);
    }

    Ok(files)
}

fn calculate_distance(
    kill_x_position: Option<f32>,
    kill_y_position: Option<f32>,
    victim_x_position: Option<f32>,
    victim_y_position: Option<f32>,
) -> f32 {
    match (
        kill_x_position,
        kill_y_position,
        victim_x_position,
        victim_y_position,
    ) {
        (
            Some(kill_x_position),
            Some(kill_y_position),
            Some(victim_x_position),
            Some(victim_y_position),
        ) => {
            let distance = ((kill_x_position - victim_x_position).powi(2)
                + (kill_y_position - victim_y_position).powi(2))
            .sqrt();
            match format!("{:.2}", distance).parse::<f32>() {
                Ok(value) => value,
                Err(_) => 0.0,
            }
        }
        _ => 0.0,
    }
}

fn calculate_top_weapons(
    weapons: &HashMap<String, RowWeapon>,
    total_kills: usize,
) -> Result<Vec<Weapon>, Box<dyn std::error::Error>> {
    let mut top_weapons: Vec<Weapon> = Vec::new();

    for (_key, value) in weapons.iter() {
        let average_distance = format!(
            "{:.2}",
            value.accumulator_distance / value.amount_deaths as f32
        )
        .parse::<f32>()?;

        let weapon = Weapon {
            name: value.name.clone(),
            amount_deaths: value.amount_deaths,
            deaths_percentage: format!(
                "{:.2}",
                (value.amount_deaths as f64 / total_kills as f64) * 100.0
            )
            .parse::<f32>()?,
            average_distance: average_distance,
        };

        top_weapons.push(weapon);
    }

    // Sort the weapons by the amount of kills (descending), then by the name (ascending).
    top_weapons.sort_by(|a, b| {
        b.amount_deaths
            .cmp(&a.amount_deaths)
            .then_with(|| a.name.cmp(&b.name))
    });

    let top_10_weapons: Vec<_> = top_weapons.iter().take(10).cloned().collect();

    Ok(top_10_weapons)
}

fn calculate_top_killers(
    killers: &std::collections::HashMap<String, RowKiller>,
) -> Result<Vec<Killer>, Box<dyn std::error::Error>> {
    let mut top_killers: Vec<Killer> = Vec::new();

    for (_key, value) in killers.iter() {
        // Get the top 3 weapons of the killer, sort by the amount of kills (descending), then by name.
        let mut top_weapons_sorted: Vec<(String, u32)> = value
            .weapons
            .iter()
            .map(|(name, &amount)| (name.clone(), amount))
            .collect();

        top_weapons_sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        top_weapons_sorted = top_weapons_sorted.into_iter().take(3).collect();

        let weapons_percentage: Vec<(String, f32)> = top_weapons_sorted
            .iter()
            .map(|(weapon, amount)| {
                let percentage_str = format!(
                    "{:.2}",
                    (*amount as f32 / value.amount_deaths as f32) * 100.0
                );
                let percentage = match percentage_str.parse::<f32>() {
                    Ok(value) => value,
                    Err(_) => {
                        eprintln!("Error: Failed to parse percentage string to f32.");
                        0.0
                    }
                };
                (weapon.clone(), percentage)
            })
            .collect();

        let killer = Killer {
            name: value.name.clone(),
            deaths: value.amount_deaths,
            weapons: weapons_percentage,
        };

        top_killers.push(killer);
    }

    // Sort the killers by the amount of kills (descending), then by the name (ascending).
    top_killers.sort_by(|a, b| b.deaths.cmp(&a.deaths).then_with(|| a.name.cmp(&b.name)));
    let top_10_killers: Vec<_> = top_killers.iter().take(10).cloned().collect();

    Ok(top_10_killers)
}

fn process_weapon_record(
    record: &csv::StringRecord,
    weapons: &mut std::collections::HashMap<String, RowWeapon>,
) {
    let weapon_name = match record.get(0) {
        Some(name) => name.to_string(),
        None => {
            eprintln!("Error: Failed to get the weapon name from the record.");
            std::process::exit(1);
        }
    };

    let kill_x_position = record.get(3).and_then(|s| s.parse::<f32>().ok());
    let kill_y_position = record.get(4).and_then(|s| s.parse::<f32>().ok());
    let victim_x_position = record.get(10).and_then(|s| s.parse::<f32>().ok());
    let victim_y_position = record.get(11).and_then(|s| s.parse::<f32>().ok());

    if weapons.contains_key(&weapon_name) {
        if let Some(current_weapon) = weapons.get_mut(&weapon_name) {
            current_weapon.amount_deaths += 1;
            current_weapon.accumulator_distance += calculate_distance(
                kill_x_position,
                kill_y_position,
                victim_x_position,
                victim_y_position,
            );
        }
    } else {
        let row_weapon = RowWeapon {
            name: weapon_name.clone(),
            amount_deaths: 1,
            accumulator_distance: calculate_distance(
                kill_x_position,
                kill_y_position,
                victim_x_position,
                victim_y_position,
            ),
        };

        weapons.insert(weapon_name.clone(), row_weapon);
    }
}

fn process_killer_record(
    record: &csv::StringRecord,
    killers: &mut std::collections::HashMap<String, RowKiller>,
) {
    let killer_name = match record.get(1) {
        Some(name) => name.to_string(),
        None => {
            eprintln!("Error: Failed to get the killer name from the record.");
            std::process::exit(1);
        }
    };

    let weapon_name = match record.get(0) {
        Some(name) => name.to_string(),
        None => {
            eprintln!("Error: Failed to get the weapon name from the record.");
            std::process::exit(1);
        }
    };

    if killers.contains_key(&killer_name) {
        if let Some(current_killer) = killers.get_mut(&killer_name) {
            current_killer.amount_deaths += 1;

            if current_killer.weapons.contains_key(&weapon_name) {
                if let Some(current_weapon) = current_killer.weapons.get_mut(&weapon_name) {
                    *current_weapon += 1;
                }
            } else {
                current_killer.weapons.insert(weapon_name.clone(), 1);
            }
        }
    } else {
        let mut weapons: HashMap<String, u32> = HashMap::new();
        weapons.insert(weapon_name.clone(), 1);

        let row_killer = RowKiller {
            name: killer_name.clone(),
            amount_deaths: 1,
            weapons,
        };

        killers.insert(killer_name.clone(), row_killer);
    }
}

fn process_files(paths: Vec<String>) -> Result<Top10Results, Box<dyn std::error::Error>> {
    let (total_kills, weapons, killers) = paths
        .par_iter()
        .map(|file_path| {
            let file = match File::open(file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Error: Failed to open file: {}", e);
                    std::process::exit(1);
                }
            };

            // Create a mutable CSV reader
            let mut rdr = ReaderBuilder::new().from_reader(file);

            // Count the total of kills.
            let total_kills =
                match <usize as std::convert::TryInto<usize>>::try_into(rdr.records().count()) {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Error: Failed to convert count to usize: {}", e);
                        std::process::exit(1);
                    }
                };

            // Go back to the start of the file.
            match rdr.seek(csv::Position::new()) {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("Error: Failed to seek to the position: {}", e);
                    std::process::exit(1);
                }
            }

            rdr.records().next();

            let mut weapons = std::collections::HashMap::new();
            let mut killers = std::collections::HashMap::new();

            for result in rdr.records() {
                let record = match result {
                    Ok(rec) => rec,
                    Err(e) => {
                        eprintln!("Error: Failed to get the record: {}", e);
                        std::process::exit(1);
                    }
                };

                process_weapon_record(&record, &mut weapons);
                process_killer_record(&record, &mut killers);
            }

            (total_kills, weapons, killers)
        })
        .reduce(
            || {
                (
                    0,
                    std::collections::HashMap::new(),
                    std::collections::HashMap::new(),
                )
            },
            |(total_kills1, mut weapons1, mut killers1), (total_kills2, weapons2, killers2)| {
                // Combine the results
                let total_kills = total_kills1 + total_kills2;

                for (key, value) in weapons2 {
                    if weapons1.contains_key(&key) {
                        if let Some(current_weapon) = weapons1.get_mut(&key) {
                            current_weapon.amount_deaths += value.amount_deaths;
                            current_weapon.accumulator_distance += value.accumulator_distance;
                        }
                    } else {
                        let row_weapon = RowWeapon {
                            name: key.clone(),
                            amount_deaths: value.amount_deaths,
                            accumulator_distance: value.accumulator_distance,
                        };
                        weapons1.insert(key.clone(), row_weapon);
                    }
                }

                for (killer_name, killer_value) in killers2 {
                    if killers1.contains_key(&killer_name) {
                        if let Some(current_killer) = killers1.get_mut(&killer_name) {
                            current_killer.amount_deaths += killer_value.amount_deaths;

                            for (weapon_name, weapon_value) in killer_value.weapons {
                                if current_killer.weapons.contains_key(&weapon_name) {
                                    if let Some(current_weapon) =
                                        current_killer.weapons.get_mut(&weapon_name)
                                    {
                                        *current_weapon += weapon_value;
                                    }
                                } else {
                                    current_killer
                                        .weapons
                                        .insert(weapon_name.clone(), weapon_value);
                                }
                            }
                        }
                    } else {
                        let mut weapons: HashMap<String, u32> = HashMap::new();

                        for (weapon_name, weapon_value) in killer_value.weapons {
                            weapons.insert(weapon_name.clone(), weapon_value);
                        }

                        let row_killer = RowKiller {
                            name: killer_name.clone(),
                            amount_deaths: killer_value.amount_deaths,
                            weapons,
                        };

                        killers1.insert(killer_name.clone(), row_killer);
                    }
                }

                (total_kills, weapons1, killers1)
            },
        );

    let top_10_weapons = calculate_top_weapons(&weapons, total_kills)?;
    let top_10_killers = calculate_top_killers(&killers)?;

    Ok(Top10Results {
        weapons: top_10_weapons,
        killers: top_10_killers,
    })
}

fn create_output_json(top_weapons: &[Weapon], top_killers: &[Killer]) -> OutputJson {
    OutputJson {
        padron: 94455,
        top_killers: top_killers
            .iter()
            .map(|killer| {
                (
                    killer.name.clone(),
                    KillerStats {
                        deaths: killer.deaths,
                        weapons_percentage: killer.weapons.iter().cloned().collect(),
                    },
                )
            })
            .collect(),
        top_weapons: top_weapons
            .iter()
            .map(|weapon| {
                (
                    weapon.name.clone(),
                    WeaponStats {
                        deaths_percentage: weapon.deaths_percentage,
                        average_distance: weapon.average_distance,
                    },
                )
            })
            .collect(),
    }
}

fn write_json_to_file(output_json: &OutputJson, output_json_name: &str) -> std::io::Result<()> {
    // Serialize the struct to a JSON string.
    let json = serde_json::to_string_pretty(&output_json).expect("Failed to serialize");

    // Write the JSON string to a file.
    let mut file = File::create(output_json_name)?;

    // Write the JSON string to a file.
    file.write_all(json.as_bytes())?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let (file_path, amount_workers, output_json_name) = get_arguments();

    // Set the number of threads for Rayon.
    match ThreadPoolBuilder::new()
        .num_threads(amount_workers)
        .build_global()
    {
        Ok(_) => (),
        Err(e) => {
            eprintln!("Error: Failed to build global thread pool: {}", e);
            std::process::exit(1);
        }
    }

    let files: Vec<String> = get_files_in_directory(&file_path)?;

    let top_10_results = process_files(files)?;

    let output_json = create_output_json(&top_10_results.weapons, &top_10_results.killers);

    match write_json_to_file(&output_json, &output_json_name) {
        Ok(_) => println!("JSON written to file successfully."),
        Err(e) => eprintln!("Failed to write JSON to file: {}", e),
    }

    println!("{:?}", start.elapsed());

    Ok(())
}
