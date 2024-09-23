use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize, Debug)]
pub struct RowWeapon {
    pub name: String,
    pub amount_deaths: u32,
    pub accumulator_distance: f32,
}

#[derive(Serialize, Clone)]
pub struct Weapon {
    pub name: String,
    pub amount_deaths: u32,
    pub deaths_percentage: f32,
    pub average_distance: f32,
}

#[derive(Serialize)]
pub struct WeaponStats {
    pub deaths_percentage: f32,
    pub average_distance: f32,
}

#[derive(Serialize, Debug)]
pub struct RowKiller {
    pub name: String,
    pub amount_deaths: u32,
    pub weapons: HashMap<String, u32>,
}

#[derive(Serialize, Clone, Debug)]
pub struct Killer {
    pub name: String,
    pub deaths: u32,
    pub weapons: Vec<(String, f32)>,
}

#[derive(Serialize)]
pub struct KillerStats {
    pub deaths: u32,
    pub weapons_percentage: HashMap<String, f32>,
}

#[derive(Serialize)]
pub struct OutputJson {
    pub padron: u32,
    pub top_killers: HashMap<String, KillerStats>,
    pub top_weapons: HashMap<String, WeaponStats>,
}

#[derive(Serialize)]
pub struct Top10Results {
    pub weapons: Vec<Weapon>,
    pub killers: Vec<Killer>,
}
