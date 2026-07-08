use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use cpal::traits::{DeviceTrait, HostTrait};

#[derive(Serialize, Deserialize, Clone)]
pub struct SoundConfig {
    pub title: String,
    pub path: String,
    pub duration: String,
    pub hotkey: Option<String>,
    #[serde(default)]
    pub play_count: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CategoryConfig {
    pub name: String,
    pub sounds: Vec<SoundConfig>,
    #[serde(default = "default_category_icon")]
    pub icon: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub is_first_run: bool,
    pub selected_input: String,
    pub selected_output: String,
    pub selected_monitoring: String,
    pub volume_mic: f32,
    pub volume_headphones: f32,
    #[serde(default = "default_volume_physical_mic")]
    pub volume_physical_mic: f32,
    pub categories: Vec<CategoryConfig>,
    #[serde(default = "default_accent_color")]
    pub accent_color: String,
    #[serde(default = "default_font_size")]
    pub font_size: f32,
    #[serde(default = "default_true")]
    pub verify_config_startup: bool,
    #[serde(default = "default_true")]
    pub disable_drm_check: bool,
    #[serde(default = "default_true")]
    pub block_echo: bool,
    #[serde(default = "default_true")]
    pub mute_mic_during_playback: bool,
    #[serde(default = "default_true")]
    pub enable_global_hotkeys: bool,
    #[serde(default = "default_true")]
    pub enable_discord_rpc: bool,
}

fn default_accent_color() -> String {
    "Blue".to_string()
}
fn default_font_size() -> f32 {
    14.0
}
fn default_true() -> bool {
    true
}
fn default_volume_physical_mic() -> f32 {
    1.0
}
fn default_category_icon() -> String {
    "📁".to_string()
}

impl Default for AppConfig {
    fn default() -> Self {
        let host = cpal::default_host();
        let default_input = host
            .default_input_device()
            .and_then(|d| d.name().ok())
            .unwrap_or_default();
        let default_output = host
            .default_output_device()
            .and_then(|d| d.name().ok())
            .unwrap_or_default();

        let cable_output = host
            .output_devices()
            .map(|devices| {
                devices
                    .filter_map(|d| d.name().ok())
                    .find(|name| {
                        let n = name.to_lowercase();
                        n.contains("cable") || n.contains("vb-audio")
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        Self {
            is_first_run: true,
            selected_input: default_input,
            selected_output: cable_output,
            selected_monitoring: default_output,
            volume_mic: 0.8,
            volume_headphones: 0.5,
            volume_physical_mic: 1.0,
            categories: vec![
                CategoryConfig {
                    name: "All Sounds".to_string(),
                    sounds: vec![],
                    icon: "🏠".to_string(),
                },
            ],
            accent_color: default_accent_color(),
            font_size: default_font_size(),
            verify_config_startup: default_true(),
            disable_drm_check: default_true(),
            block_echo: default_true(),
            mute_mic_during_playback: default_true(),
            enable_global_hotkeys: default_true(),
            enable_discord_rpc: default_true(),
        }
    }
}

pub fn get_exe_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}

pub fn load_config() -> AppConfig {
    let path = get_exe_dir().join("soundpad_config.json");
    if let Ok(file) = File::open(&path) {
        if let Ok(config) = serde_json::from_reader(BufReader::new(file)) {
            return config;
        }
    }
    AppConfig::default()
}

pub fn save_config(config: &AppConfig) {
    let path = get_exe_dir().join("soundpad_config.json");
    if let Ok(mut file) = File::create(&path) {
        if let Ok(json) = serde_json::to_string_pretty(config) {
            let _ = file.write_all(json.as_bytes());
        }
    }
}