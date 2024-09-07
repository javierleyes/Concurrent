use csv::ReaderBuilder;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Write;

#[derive(Serialize, Debug)]
struct RowWeapon {
    name: String,
    amount_deaths: u32,
    accumulator_distance: f32,
}

#[derive(Serialize, Clone)]
struct Weapon {
    name: String,
    amount_deaths: u32,
    deaths_percentage: f32,
    average_distance: f32,
}

#[derive(Serialize)]
struct WeaponStats {
    deaths_percentage: f32,
    average_distance: f32,
}

#[derive(Serialize, Debug)]
struct RowKiller {
    name: String,
    amount_deaths: u32,
    weapons: HashMap<String, u32>,
}

#[derive(Serialize, Clone, Debug)]
struct Killer {
    name: String,
    deaths: u32,
    weapons: Vec<(String, f32)>,
}

#[derive(Serialize)]
struct KillerStats {
    deaths: u32,
    weapons_percentage: HashMap<String, f32>,
}

// TODO: Implement weapons_percentage.

#[derive(Serialize)]
struct OutputJson {
    padron: u32,
    top_killers: HashMap<String, KillerStats>,
    top_weapons: HashMap<String, WeaponStats>,
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
            format!("{:.2}", distance).parse::<f32>().unwrap_or(0.0)
        }
        _ => 0.0,
    }
}

fn process_weapons(file_path: &str) -> Result<Vec<Weapon>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;

    // Create a mutable CSV reader
    let mut rdr = ReaderBuilder::new().from_reader(file);

    // Count the total of kills.
    let total_kills = rdr.records().count();

    // Go back to the start of the file.
    rdr.seek(csv::Position::new())?;

    let mut weapons: HashMap<String, RowWeapon> = HashMap::new();

    rdr.records().next();
    for result in rdr.records() {
        let record = result?;
        let weapon_name = record.get(0).unwrap().to_string();

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

    let mut top_weapons: Vec<Weapon> = Vec::new();

    for (_key, value) in weapons.iter() {
        let average_distance = format!(
            "{:.2}",
            value.accumulator_distance / value.amount_deaths as f32
        )
        .parse::<f32>()
        .unwrap();

        let weapon = Weapon {
            name: value.name.clone(),
            amount_deaths: value.amount_deaths,
            deaths_percentage: format!(
                "{:.2}",
                (value.amount_deaths as f32 / total_kills as f32) * 100.0
            )
            .parse::<f32>()
            .unwrap(),
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

fn process_killers(file_path: &str) -> Result<Vec<Killer>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;

    // Create a mutable CSV reader
    let mut rdr = ReaderBuilder::new().from_reader(file);

    // Go back to the start of the file.
    rdr.seek(csv::Position::new())?;

    let mut killers: HashMap<String, RowKiller> = HashMap::new();

    rdr.records().next();
    for result in rdr.records() {
        let record = result?;
        let killer_name = record.get(1).unwrap().to_string();
        let weapon_name = record.get(0).unwrap().to_string();

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

    let mut top_killers: Vec<Killer> = Vec::new();

    for (_key, value) in killers.iter() {
        // Get the top 3 weapon of the killer, sort by the amount of kills (descending), then by name.
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
                (
                    weapon.clone(),
                    format!(
                        "{:.2}",
                        (*amount as f32 / value.amount_deaths as f32) * 100.0
                    )
                    .parse::<f32>()
                    .unwrap(),
                )
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
    let (file_path, _amount_workers, output_json_name) = get_arguments();

    let top_10_weapons = process_weapons(&file_path)?;

    let top_10_killers = process_killers(&file_path)?;

    let output_json = create_output_json(&top_10_weapons, &top_10_killers);

    write_json_to_file(&output_json, &output_json_name)?;

    Ok(())
}
