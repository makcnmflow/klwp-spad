use crate::audio::{
    find_virtual_cable_microphone, find_virtual_cable_output_name, get_duration_seconds,
    get_duration_str, load_decoder_stream, start_audio_streams, ActiveSound, AudioState,
};
use crate::config::{get_exe_dir, load_config, save_config, AppConfig, CategoryConfig, SoundConfig};
use crate::discord::{spawn_discord_rpc_thread, DiscordMsg};
use crate::utils::{
    parse_soundpad_protocol, parse_voicemod_protocol, set_default_windows_microphone,
    try_convert_with_ffmpeg, url_decode,
};

use cpal::traits::{DeviceTrait, HostTrait};
use eframe::egui;
use egui_phosphor::regular;
use global_hotkey::{hotkey::HotKey, GlobalHotKeyEvent, GlobalHotKeyManager};
use ringbuf::HeapRb;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

const AVAILABLE_ICONS: &[&str] = &[
    "📁", "🏠", "🎮", "🎵", "🔥", "😂", "👑", "🎙", "📢", "👾", "👽", "🐱", "🐶", "🍕", "🎬", "✨"
];

#[derive(Clone, PartialEq)]
pub(crate) enum SortColumn {
    Number,
    Title,
    Duration,
}

#[derive(Clone, PartialEq)]
pub enum SettingsTab {
    Devices,
    Hotkeys,
    Audio,
    Appearance,
    Categories,
    About,
}

pub struct RecordingState {
    pub sound_idx: usize,
    pub recorded_combination: Option<String>,
}

pub enum DownloadResult {
    Success { sound: SoundConfig, category_idx: usize },
    Error(String),
}

#[derive(Clone)]
pub struct QueuedDownload {
    pub url: String,
    pub is_voicemod: bool,
    pub category_idx: usize,
    pub retry_count: u32,
}

pub struct SoundpadApp {
    pub input_devices: Vec<String>,
    pub output_devices: Vec<String>,
    pub monitoring_devices: Vec<String>,
    pub selected_sound_idx: Option<usize>,
    pub selected_category_idx: usize,
    pub seek_slider_value: f32,
    pub new_category_name: String,
    pub input_stream: Option<cpal::Stream>,
    pub output_stream: Option<cpal::Stream>,
    pub monitoring_stream: Option<cpal::Stream>,
    pub output_sample_rate: u32,
    pub monitoring_sample_rate: u32,
    pub audio_state: Arc<Mutex<AudioState>>,
    pub status_message: String,
    pub logs: Vec<String>,
    pub show_logs: bool,
    pub config: AppConfig,
    pub show_settings: bool,
    pub settings_tab: SettingsTab,
    pub hotkey_manager: GlobalHotKeyManager,
    pub registered_hotkeys: Vec<HotKey>,
    pub recording_state: Option<RecordingState>,
    pub hotkey_options_idx: Option<usize>,
    pub url_queue: Arc<Mutex<Vec<String>>>,
    pub new_sounds_rx: std::sync::mpsc::Receiver<DownloadResult>,
    pub new_sounds_tx: std::sync::mpsc::Sender<DownloadResult>,
    pub download_queue: Vec<QueuedDownload>,
    pub current_download: Option<QueuedDownload>,
    pub download_progress: f32,
    pub download_progress_shared: Option<Arc<Mutex<f32>>>,
    pub discord_tx: std::sync::mpsc::Sender<DiscordMsg>,
    pub update_rx: std::sync::mpsc::Receiver<String>,
    pub update_available: Option<String>,
    pub decoder_pool: ThreadPool,
    pub sound_rename_cmd: Option<(usize, String)>,
    pub sort_column: Option<SortColumn>,
    pub sort_ascending: bool,
    pub sorted_indices: Vec<usize>,
    pub pending_apply: bool,
}

impl SoundpadApp {
    pub fn new_with_ipc(
        url_queue: Arc<Mutex<Vec<String>>>,
        new_sounds_rx: std::sync::mpsc::Receiver<DownloadResult>,
        new_sounds_tx: std::sync::mpsc::Sender<DownloadResult>,
    ) -> Self {
        let mut config = load_config();

        if config.categories.get(0).map_or(false, |c| c.icon.is_empty()) {
            if let Some(cat) = config.categories.get_mut(0) { cat.icon = "📁".to_string(); }
        }
        if config.categories.get(1).map_or(false, |c| c.icon.is_empty()) {
            if let Some(cat) = config.categories.get_mut(1) { cat.icon = "🎮".to_string(); }
        }

        let host = cpal::default_host();
        let input_devices = host.input_devices()
            .map(|devices| devices.filter_map(|d| d.name().ok()).collect::<Vec<String>>())
            .unwrap_or_default();

        let output_devices = host.output_devices()
            .map(|devices| devices.filter_map(|d| d.name().ok()).collect::<Vec<String>>())
            .unwrap_or_default();

        let mut monitoring_devices = vec!["[Disabled]".to_string()];
        for dev in &output_devices {
            if !monitoring_devices.contains(dev) {
                monitoring_devices.push(dev.clone());
            }
        }

        let show_settings = config.is_first_run;

        let audio_state = Arc::new(Mutex::new(AudioState {
            active_sound: None,
            volume_mic: config.volume_mic,
            volume_headphones: config.volume_headphones,
            volume_physical_mic: config.volume_physical_mic,
            is_paused: false,
            mute_mic_during_playback: config.mute_mic_during_playback,
            current_sample_index: 0,
            total_samples: 0,
            sample_rate: 44100,
        }));

        let hotkey_manager = GlobalHotKeyManager::new().unwrap();
        let mut registered_hotkeys = vec![];

        if config.enable_global_hotkeys {
            for category in &config.categories {
                for sound in &category.sounds {
                    if let Some(ref hk_str) = sound.hotkey {
                        if let Ok(hotkey) = hk_str.parse::<HotKey>() {
                            let _ = hotkey_manager.register(hotkey);
                            registered_hotkeys.push(hotkey);
                        }
                    }
                }
            }
        }

        let discord_tx = spawn_discord_rpc_thread();

        let (update_tx, update_rx) = std::sync::mpsc::channel::<String>();
        let update_tx_clone = update_tx.clone();

        std::thread::spawn(move || {
            let client = reqwest::blocking::Client::new();
            let resp = client
                .get("https://api.github.com/repos/makcnmflow/klwp-spad/releases/latest")
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "klwp-spad")
                .send();

            if let Ok(response) = resp {
                if response.status().is_success() {
                    let json_str = response.text().unwrap_or_default();
                    
                    #[derive(serde::Deserialize)]
                    struct GitHubRelease {
                        tag_name: String,
                    }

                    if let Ok(release) = serde_json::from_str::<GitHubRelease>(&json_str) {
                        let current_ver = env!("APP_VERSION");
                        let latest_tag = &release.tag_name;
                        if is_newer_version(current_ver, latest_tag) {
                            let _ = update_tx_clone.send(release.tag_name);
                        }
                    }
                }
            }
        });

        let mut app = Self {
            input_devices,
            output_devices,
            monitoring_devices,
            selected_sound_idx: None,
            selected_category_idx: 0,
            seek_slider_value: 0.0,
            new_category_name: String::new(),
            input_stream: None,
            output_stream: None,
            monitoring_stream: None,
            output_sample_rate: 44100,
            monitoring_sample_rate: 44100,
            audio_state,
            status_message: String::new(),
            logs: vec![],
            show_logs: false,
            config,
            show_settings,
            settings_tab: SettingsTab::Devices,
            hotkey_manager,
            registered_hotkeys,
            recording_state: None,
            hotkey_options_idx: None,
            url_queue,
            new_sounds_rx,
            new_sounds_tx,
            download_queue: Vec::new(),
            current_download: None,
            download_progress: 0.0,
            download_progress_shared: None,
            discord_tx,
            update_rx,
            update_available: None,
            decoder_pool: ThreadPool::new(2),
            sound_rename_cmd: None,
            sort_column: None,
            sort_ascending: true,
            sorted_indices: Vec::new(),
            pending_apply: false,
        };

        app.log_info("System initialized successfully.");

        app.update_sorted_indices();

        let _ = app.discord_tx.send(DiscordMsg::UpdateStatus {
            enabled: app.config.enable_discord_rpc,
        });

        if !app.config.is_first_run {
            app.start_streaming();
            let auto_cable_mic = find_virtual_cable_microphone(&app.config.selected_output, &app.input_devices);
            set_default_windows_microphone(&auto_cable_mic);
        } else {
            app.status_message = "Please complete the initial device setup.".to_string();
            app.log_warn("Awaiting first-run configuration setup...");
        }

        app
    }

    fn add_log(&mut self, formatted_msg: &str) {
        self.logs.push(formatted_msg.to_string());
        if self.logs.len() > 100 {
            self.logs.remove(0);
        }
    }

    fn log_info(&mut self, msg: &str) {
        self.add_log(&format!("[INFO] {}", msg));
    }

    fn log_warn(&mut self, msg: &str) {
        self.add_log(&format!("[WARN] {}", msg));
    }

    fn log_error(&mut self, msg: &str) {
        self.add_log(&format!("[ERROR] {}", msg));
    }

    fn save_app_config(&self) {
        save_config(&self.config);
    }

    fn update_global_hotkeys(&mut self) {
        let _ = self.hotkey_manager.unregister_all(&self.registered_hotkeys);
        self.registered_hotkeys.clear();

        if !self.config.enable_global_hotkeys {
            self.log_info("Global hotkeys are disabled in settings.");
            return;
        }

        let mut failed_hotkeys = Vec::new();

        for (cat_idx, category) in self.config.categories.iter().enumerate() {
            for (sound_idx, sound) in category.sounds.iter().enumerate() {
                if let Some(ref hk_str) = sound.hotkey {
                    if let Ok(hotkey) = hk_str.parse::<HotKey>() {
                        if self.hotkey_manager.register(hotkey).is_ok() {
                            self.registered_hotkeys.push(hotkey);
                        } else {
                            failed_hotkeys.push((hk_str.clone(), cat_idx, sound_idx));
                        }
                    }
                }
            }
        }

        for (hk_str, cat_idx, sound_idx) in failed_hotkeys {
            let cat_name = self.config.categories[cat_idx].name.clone();
            self.log_error(&format!(
                "Failed to register keyboard shortcut: '{}' in category '{}' for sound file #{}",
                hk_str, cat_name, sound_idx + 1
            ));
        }

        self.log_info(&format!("Registered {} global hotkeys.", self.registered_hotkeys.len()));
    }

    fn start_streaming(&mut self) {
        self.input_stream = None;
        self.output_stream = None;
        self.monitoring_stream = None;

        let host = cpal::default_host();

        if self.config.selected_output.is_empty() {
            let devices: Vec<String> = host
                .output_devices()
                .map(|d| d.filter_map(|d| d.name().ok()).collect())
                .unwrap_or_default();
            let auto = find_virtual_cable_output_name(&devices);
            if !auto.is_empty() {
                self.config.selected_output = auto;
            }
        }

        match start_audio_streams(
            &host,
            &self.config.selected_input,
            &self.config.selected_output,
            &self.config.selected_monitoring,
            Arc::clone(&self.audio_state),
        ) {
            Ok((in_stream, out_stream, mon_stream, out_rate, mon_rate)) => {
                self.input_stream = Some(in_stream);
                self.output_stream = Some(out_stream);
                self.monitoring_stream = mon_stream;
                self.output_sample_rate = out_rate;
                self.monitoring_sample_rate = mon_rate;
                self.status_message = "Audio streaming active.".to_string();
                self.log_info("Audio streams successfully connected to virtual audio device.");
            }
            Err(e) => {
                self.status_message = format!("Setup error: {}", e);
                self.log_error(&format!("Critical failure starting audio engine loop. Error: {}", e));
            }
        }
    }

    fn play_sound_at_index(&mut self, idx: usize) {
        self.play_sound_at_index_with_offset(idx, None);
    }

    fn play_sound_at_index_with_offset(&mut self, idx: usize, start_seconds: Option<f32>) {
        if self.selected_category_idx >= self.config.categories.len() { return; }

        {
            let cat = &mut self.config.categories[self.selected_category_idx];
            if idx >= cat.sounds.len() { return; }
            cat.sounds[idx].play_count += 1;
        }

        if self.output_stream.is_none() {
            self.start_streaming();
            if self.output_stream.is_none() {
                return;
            }
        }

        let path = self.config.categories[self.selected_category_idx].sounds[idx].path.clone();
        let title = self.config.categories[self.selected_category_idx].sounds[idx].title.clone();

        self.log_info(&format!("Playing sound (streaming): '{}'", title));

        let rate_mic = self.output_sample_rate;
        let rate_head = self.monitoring_sample_rate;

        let duration_secs = if self.config.categories[self.selected_category_idx].sounds[idx].duration_secs > 0.0 {
            self.config.categories[self.selected_category_idx].sounds[idx].duration_secs
        } else {
            let secs = get_duration_seconds(&path);
            self.config.categories[self.selected_category_idx].sounds[idx].duration_secs = secs;
            secs
        };
        let total_samples = (duration_secs * rate_mic as f32) as usize;

        let rb_mic = HeapRb::<f32>::new(32768);
        let (mut prod_mic, cons_mic) = rb_mic.split();

        let rb_head = HeapRb::<f32>::new(32768);
        let (mut prod_head, cons_head) = rb_head.split();

        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_signal_clone = Arc::clone(&stop_signal);

        let finished_decoding = Arc::new(AtomicBool::new(false));
        let finished_decoding_clone = Arc::clone(&finished_decoding);

        let path_clone = path.clone();
        self.decoder_pool.execute(move || {
            let source_mic = match load_decoder_stream(&path_clone, rate_mic) {
                Ok(s) => s,
                Err(_) => return,
            };
            let source_head = match load_decoder_stream(&path_clone, rate_head) {
                Ok(s) => s,
                Err(_) => return,
            };

            let mut mic_iter = source_mic;
            let mut head_iter = source_head;

            if let Some(start_sec) = start_seconds {
                let skip_mic = (start_sec * rate_mic as f32) as usize;
                let skip_head = (start_sec * rate_head as f32) as usize;
                for _ in 0..skip_mic { mic_iter.next(); }
                for _ in 0..skip_head { head_iter.next(); }
            }

            let mut mic_done = false;
            let mut head_done = false;

            while !mic_done || !head_done {
                if stop_signal_clone.load(Ordering::Relaxed) {
                    break;
                }

                let mut pushed_something = false;

                for _ in 0..512 {
                    if !mic_done {
                        if prod_mic.is_full() {
                            break;
                        } else if let Some(sample) = mic_iter.next() {
                            if prod_mic.push(sample).is_err() {
                                return;
                            }
                            pushed_something = true;
                        } else {
                            mic_done = true;
                        }
                    }
                }

                for _ in 0..512 {
                    if !head_done {
                        if prod_head.is_full() {
                            break;
                        } else if let Some(sample) = head_iter.next() {
                            if prod_head.push(sample).is_err() {
                                return;
                            }
                            pushed_something = true;
                        } else {
                            head_done = true;
                        }
                    }
                }

                if !pushed_something {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }

            finished_decoding_clone.store(true, Ordering::Relaxed);
        });

        let mut state = self.audio_state.lock().unwrap();
        state.is_paused = false;

        let start_sample = start_seconds
            .map(|s| (s * rate_mic as f32) as usize)
            .unwrap_or(0);
        state.current_sample_index = start_sample;
        state.total_samples = total_samples;
        state.sample_rate = rate_mic;

        if let Some(ref old_sound) = state.active_sound {
            old_sound.stop_signal.store(true, Ordering::Relaxed);
        }

        state.active_sound = Some(ActiveSound {
            consumer_mic: cons_mic,
            consumer_headphones: cons_head,
            stop_signal,
            finished_decoding,
        });
    }

    fn stop_sound(&mut self) {
        {
            let mut state = self.audio_state.lock().unwrap();
            if let Some(ref sound) = state.active_sound {
                sound.stop_signal.store(true, Ordering::Relaxed);
            }
            state.active_sound = None;
            state.is_paused = false;
            state.current_sample_index = 0;
            state.total_samples = 0;
        }
        self.log_info("Playback stopped.");
    }

    fn toggle_pause(&mut self) {
        let is_paused = {
            let mut state = self.audio_state.lock().unwrap();
            state.is_paused = !state.is_paused;
            state.is_paused
        };
        self.log_info(if is_paused { "Playback paused." } else { "Playback resumed." });
    }

    fn update_sorted_indices(&mut self) {
        if self.selected_category_idx >= self.config.categories.len() { return; }
        let cat = &self.config.categories[self.selected_category_idx];
        let n = cat.sounds.len();
        let mut indices: Vec<usize> = (0..n).collect();
        if let Some(ref col) = self.sort_column {
            match col {
                SortColumn::Number => {
                    if !self.sort_ascending {
                        indices.reverse();
                    }
                }
                SortColumn::Title => {
                    indices.sort_by(|&a, &b| {
                        let cmp = cat.sounds[a].title.to_lowercase().cmp(&cat.sounds[b].title.to_lowercase());
                        if self.sort_ascending { cmp } else { cmp.reverse() }
                    });
                }
                SortColumn::Duration => {
                    indices.sort_by(|&a, &b| {
                        let cmp = cat.sounds[a].duration_secs.partial_cmp(&cat.sounds[b].duration_secs).unwrap_or(std::cmp::Ordering::Equal);
                        if self.sort_ascending { cmp } else { cmp.reverse() }
                    });
                }
            }
        }
        self.sorted_indices = indices;
    }

    fn download_and_add_sound(&mut self, url: String) {
        self.download_queue.push(QueuedDownload {
            url: url.clone(),
            is_voicemod: false,
            category_idx: self.selected_category_idx,
            retry_count: 0,
        });
        self.log_info(&format!("Queued download task: {}", url));
    }

    fn download_and_add_voicemod_sound(&mut self, uuid: String) {
        self.download_queue.push(QueuedDownload {
            url: uuid.clone(),
            is_voicemod: true,
            category_idx: self.selected_category_idx,
            retry_count: 0,
        });
        self.log_info(&format!("Queued Voicemod import task for UUID: {}", uuid));
    }

    fn start_queued_download(&mut self, dl: QueuedDownload) {
        let shared = Arc::new(Mutex::new(0.0f32));
        self.download_progress_shared = Some(shared.clone());
        if dl.is_voicemod {
            self.trigger_voicemod_download(dl.url, dl.category_idx, shared);
        } else {
            self.trigger_soundpad_download(dl.url, dl.category_idx, shared);
        }
    }

    fn trigger_soundpad_download(&mut self, url: String, category_idx: usize, progress_shared: Arc<Mutex<f32>>) {
        self.status_message = "Downloading audio...".to_string();
        self.log_info(&format!("Starting background download: {}", url));

        let raw_filename = url.split('/').last().unwrap_or("sound.mp3");
        let decoded_filename = url_decode(raw_filename);
        let safe_filename: String = decoded_filename.chars()
            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-' || *c == '_' || *c == ' ')
            .collect();

        let safe_filename = if safe_filename.is_empty() {
            "downloaded_sound.mp3".to_string()
        } else {
            safe_filename
        };

        let dir = get_exe_dir().join("sounds");
        let _ = std::fs::create_dir_all(&dir);
        let destination_path = dir.join(&safe_filename);
        let dest_str = destination_path.display().to_string();

        let tx = self.new_sounds_tx.clone();
        let url_clone = url.clone();

        std::thread::spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .unwrap();

            let result = client.get(&url_clone).send();
            match result {
                Ok(response) if response.status().is_success() => {
                    let total_size = response.content_length().unwrap_or(0);
                    let mut downloaded: u64 = 0;
                    let mut all_bytes = Vec::new();

                    if total_size > 0 {
                        let mut buf = vec![0u8; 65536];
                        use std::io::Read;
                        let mut reader = response;
                        loop {
                            let n = match reader.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };
                            all_bytes.extend_from_slice(&buf[..n]);
                            downloaded += n as u64;
                            if let Ok(mut p) = progress_shared.lock() {
                                *p = downloaded as f32 / total_size as f32;
                            }
                        }
                    } else {
                        match response.bytes() {
                            Ok(b) => all_bytes = b.to_vec(),
                            Err(e) => {
                                let _ = tx.send(DownloadResult::Error(format!("Failed to read response body: {}", e)));
                                return;
                            }
                        }
                    }

                    if let Err(e) = std::fs::write(&dest_str, &all_bytes) {
                        let _ = tx.send(DownloadResult::Error(format!("Failed to save file: {}", e)));
                        return;
                    }
                    let path_obj = std::path::Path::new(&dest_str);
                    let duration = get_duration_str(path_obj);
                    let title = path_obj.file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "Downloaded Sound".to_string());

                    let new_sound = SoundConfig {
                        title,
                        path: dest_str,
                        duration,
                        hotkey: None,
                        play_count: 0,
                        duration_secs: 0.0,
                    };

                    let _ = tx.send(DownloadResult::Success { sound: new_sound, category_idx });
                }
                Ok(response) => {
                    let _ = tx.send(DownloadResult::Error(format!("Download request failed with status: {}", response.status())));
                }
                Err(e) => {
                    let _ = tx.send(DownloadResult::Error(format!("Network request failed: {}", e)));
                }
            }
        });
    }

    fn trigger_voicemod_download(&mut self, uuid: String, category_idx: usize, progress_shared: Arc<Mutex<f32>>) {
        self.status_message = "Locating download link on Voicemod Tuna...".to_string();
        self.log_info(&format!("Scraping Voicemod Tuna for UUID: {}", uuid));

        let tx = self.new_sounds_tx.clone();

        std::thread::spawn(move || {
            let sound_page_url = format!("https://tuna.voicemod.net/sound/{}", uuid);
            let client = reqwest::blocking::Client::builder()
                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .unwrap();

            let page_result = client.get(&sound_page_url).send();
            let html = match page_result {
                Ok(resp) if resp.status().is_success() => resp.text().unwrap_or_default(),
                _ => {
                    let _ = tx.send(DownloadResult::Error("Connection failed while querying Voicemod Tuna database".to_string()));
                    return;
                }
            };

            let title = if let Some(start_pos) = html.find("<title>") {
                let sub = &html[start_pos + 7..];
                if let Some(end_pos) = sub.find("</title>") {
                    let raw_title = &sub[..end_pos];
                    let clean = if let Some(pos) = raw_title.find("Meme") {
                        raw_title[..pos].trim().to_string()
                    } else if let Some(pos) = raw_title.to_lowercase().find("meme") {
                        raw_title[..pos].trim().to_string()
                    } else {
                        raw_title.trim().to_string()
                    };
                    if !clean.is_empty() { clean } else { "Downloaded Tuna Meme".to_string() }
                } else {
                    "Downloaded Tuna Meme".to_string()
                }
            } else {
                "Downloaded Tuna Meme".to_string()
            };

            let download_url = if let Some(pos) = html.find("\"contentUrl\":\"") {
                let sub = &html[pos + 14..];
                sub.find("\"").map(|end_pos| &sub[..end_pos])
            } else {
                None
            };

            let download_url = match download_url {
                Some(url) => url.to_string(),
                None => {
                    let _ = tx.send(DownloadResult::Error("Failed to scrape direct download URL from Tuna webpage".to_string()));
                    return;
                }
            };

            let filename = download_url.split('/').last().unwrap_or("sound.mp3");
            let safe_filename: String = filename.chars()
                .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-' || *c == '_' || *c == ' ')
                .collect();
            let safe_filename = if safe_filename.is_empty() { "tuna_sound.mp3".to_string() } else { safe_filename };

            let dir = get_exe_dir().join("sounds");
            let _ = std::fs::create_dir_all(&dir);
            let destination_path = dir.join(&safe_filename);
            let dest_str = destination_path.display().to_string();

            let download_result = client.get(&download_url).send();
            match download_result {
                Ok(resp) if resp.status().is_success() => {
                    let total_size = resp.content_length().unwrap_or(0);
                    let mut downloaded: u64 = 0;
                    let mut all_bytes = Vec::new();

                    if total_size > 0 {
                        let mut buf = vec![0u8; 65536];
                        use std::io::Read;
                        let mut reader = resp;
                        loop {
                            let n = match reader.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };
                            all_bytes.extend_from_slice(&buf[..n]);
                            downloaded += n as u64;
                            if let Ok(mut p) = progress_shared.lock() {
                                *p = downloaded as f32 / total_size as f32;
                            }
                        }
                    } else {
                        match resp.bytes() {
                            Ok(b) => all_bytes = b.to_vec(),
                            Err(e) => {
                                let _ = tx.send(DownloadResult::Error(format!("Failed to read response body: {}", e)));
                                return;
                            }
                        }
                    }

                    if let Err(e) = std::fs::write(&dest_str, &all_bytes) {
                        let _ = tx.send(DownloadResult::Error(format!("Failed to save file: {}", e)));
                        return;
                    }
                    let path_obj = std::path::Path::new(&dest_str);
                    let duration = get_duration_str(path_obj);

                    let new_sound = SoundConfig {
                        title,
                        path: dest_str,
                        duration,
                        hotkey: None,
                        play_count: 0,
                        duration_secs: 0.0,
                    };

                    let _ = tx.send(DownloadResult::Success { sound: new_sound, category_idx });
                }
                Ok(resp) => {
                    let _ = tx.send(DownloadResult::Error(format!("Download connection aborted with status: {}", resp.status())));
                }
                Err(e) => {
                    let _ = tx.send(DownloadResult::Error(format!("Failed to execute download request: {}", e)));
                }
            }
        });
    }
}

impl Drop for SoundpadApp {
    fn drop(&mut self) {
        self.save_app_config();
        if !self.config.selected_input.is_empty() {
            set_default_windows_microphone(&self.config.selected_input);
        }
    }
}

impl eframe::App for SoundpadApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let sound_finished = {
            if let Ok(mut state) = self.audio_state.lock() {
                state.volume_mic = self.config.volume_mic;
                state.volume_headphones = self.config.volume_headphones;
                state.volume_physical_mic = self.config.volume_physical_mic;
                state.mute_mic_during_playback = self.config.mute_mic_during_playback;

                if let Some(ref sound) = state.active_sound {
                    sound.finished_decoding.load(Ordering::Relaxed) && sound.consumer_mic.is_empty()
                } else {
                    false
                }
            } else {
                false
            }
        };

        if sound_finished {
            self.stop_sound();
        }

        let mut rename_cmd = None;
        let mut icon_cmd = None;

        let mut visuals = egui::Visuals::dark();
        let accent = match self.config.accent_color.as_str() {
            "Red" => egui::Color32::from_rgb(220, 53, 69),
            "Green" => egui::Color32::from_rgb(40, 167, 69),
            "Purple" => egui::Color32::from_rgb(111, 66, 193),
            "Orange" => egui::Color32::from_rgb(253, 126, 20),
            _ => egui::Color32::from_rgb(13, 110, 253),
        };
        visuals.selection.bg_fill = accent;
        visuals.hyperlink_color = accent;
        ctx.set_visuals(visuals);

        let mut style = (*ctx.style()).clone();
        for font_id in style.text_styles.values_mut() {
            font_id.size = self.config.font_size;
        }
        ctx.set_style(style);

        while let Ok(event) = GlobalHotKeyEvent::receiver().try_recv() {
            if event.state() == global_hotkey::HotKeyState::Pressed {
                let pressed_id = event.id();
                let mut sound_to_play = None;

                for (cat_idx, category) in self.config.categories.iter().enumerate() {
                    for (sound_idx, sound) in category.sounds.iter().enumerate() {
                        if let Some(ref hk_str) = sound.hotkey {
                            if let Ok(hotkey) = hk_str.parse::<HotKey>() {
                                if hotkey.id() == pressed_id {
                                    sound_to_play = Some((cat_idx, sound_idx));
                                    break;
                                }
                            }
                        }
                    }
                }

                if let Some((cat_idx, sound_idx)) = sound_to_play {
                    self.selected_category_idx = cat_idx;
                    self.play_sound_at_index(sound_idx);
                }
            }
        }

        let mut newly_pressed_combination = None;
        ctx.input(|i| {
            for event in &i.events {
                if let egui::Event::Key { key, pressed: true, modifiers, .. } = event {
                    if !is_modifier_key(*key) {
                        let combo_str = map_key_to_hotkey_string(*key, modifiers);
                        newly_pressed_combination = Some(combo_str);
                    }
                }
            }
        });

        let next_url = {
            let mut queue = self.url_queue.lock().unwrap();
            if !queue.is_empty() {
                Some(queue.remove(0))
            } else {
                None
            }
        };

        if let Some(raw_url) = next_url {
            if raw_url.starts_with("soundpad://") {
                if let Some(http_url) = parse_soundpad_protocol(&raw_url) {
                    if self.config.focus_on_sound_link {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                    }
                    self.download_and_add_sound(http_url);
                }
            } else if raw_url.starts_with("voicemod:") {
                if let Some(uuid) = parse_voicemod_protocol(&raw_url) {
                    if self.config.focus_on_sound_link {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                    }
                    self.download_and_add_voicemod_sound(uuid);
                }
            }
        }

        if self.current_download.is_none() && !self.download_queue.is_empty() {
            let next_dl = self.download_queue.remove(0);
            self.current_download = Some(next_dl.clone());
            self.download_progress = 0.0;
            self.start_queued_download(next_dl);
        }

        if self.current_download.is_some() {
            if let Some(ref shared) = self.download_progress_shared {
                if let Ok(p) = shared.lock() {
                    self.download_progress = *p;
                }
            }
        }

        while let Ok(result) = self.new_sounds_rx.try_recv() {
            match result {
                DownloadResult::Success { sound, category_idx } => {
                    if category_idx < self.config.categories.len() {
                        let name = sound.title.clone();
                        self.config.categories[category_idx].sounds.push(sound);
                        self.save_app_config();
                        self.update_global_hotkeys();
                        if category_idx == self.selected_category_idx {
                            self.update_sorted_indices();
                        }
                        self.status_message = format!("Added '{}' successfully!", name);
                        self.log_info(&format!("Imported sound file '{}' added to category #{}", name, category_idx + 1));
                    }
                    self.current_download = None;
                    self.download_progress = 0.0;
                    self.download_progress_shared = None;
                }
                DownloadResult::Error(err_msg) => {
                    if let Some(mut active) = self.current_download.clone() {
                        if active.retry_count < 1 {
                            active.retry_count += 1;
                            self.current_download = Some(active.clone());
                            self.download_progress = 0.0;
                            self.log_warn(&format!("Download of '{}' failed on first attempt. Error: {}. Retrying once...", active.url, err_msg));
                            self.status_message = "Download failed. Retrying...".to_string();
                            self.start_queued_download(active);
                        } else {
                            self.status_message = format!("Download error: {}", err_msg);
                            self.log_error(&format!("Download of '{}' failed permanently. Exhausted retries. Error: {}", active.url, err_msg));
                            self.current_download = None;
                            self.download_progress = 0.0;
                            self.download_progress_shared = None;
                        }
                    } else {
                        self.status_message = format!("Download error: {}", err_msg);
                        self.log_error(&format!("Critical network issue during download session. Detailed error: {}", err_msg));
                    }
                }
            }
        }

        while let Ok(tag) = self.update_rx.try_recv() {
            self.update_available = Some(tag.clone());
            self.log_info(&format!("New version found on GitHub! Version: {}", tag));
        }

        egui::TopBottomPanel::top("top_menu_bar").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    let s_btn = ui.button("⚙ Settings");
                    if s_btn.clicked() {
                        self.show_settings = true;
                        ui.close_menu();
                    }
                    tooltip_above(&s_btn, ctx, "Open settings panel");
                    ui.separator();
                    let e_btn = ui.button("❌ Exit");
                    if e_btn.clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                    tooltip_above(&e_btn, ctx, "Close the application");
                });
                ui.menu_button("Help", |ui| {
                    let a_btn = ui.button("About");
                    if a_btn.clicked() {
                        self.show_settings = true;
                        self.settings_tab = SettingsTab::About;
                        ui.close_menu();
                    }
                    tooltip_above(&a_btn, ctx, "Version info and credits");
                });
            });
        });

        if self.config.show_quick_controls {
        egui::TopBottomPanel::top("quick_control_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let pl_btn = ui.button(regular::PLAY);
                if pl_btn.clicked() {
                    if let Some(idx) = self.selected_sound_idx {
                        self.play_sound_at_index(idx);
                    }
                }
                tooltip_above(&pl_btn, ctx, "Play selected sound");
                let pa_btn = ui.button(regular::PAUSE);
                if pa_btn.clicked() {
                    self.toggle_pause();
                }
                tooltip_above(&pa_btn, ctx, "Pause/Resume sound");
                let st_btn = ui.button(regular::STOP);
                if st_btn.clicked() {
                    self.stop_sound();
                }
                tooltip_above(&st_btn, ctx, "Stop playback");

                ui.separator();

                let (current_time, total_time, has_active_sound) = {
                    if let Ok(state) = self.audio_state.lock() {
                        let cur = if state.total_samples > 0 && state.sample_rate > 0 {
                            state.current_sample_index as f32 / state.sample_rate as f32
                        } else { 0.0 };
                        let tot = if state.total_samples > 0 {
                            state.total_samples as f32 / state.sample_rate as f32
                        } else { 0.0 };
                        (cur, tot, state.active_sound.is_some())
                    } else {
                        (0.0, 0.0, false)
                    }
                };

                let slider_enabled = has_active_sound && total_time > 0.0;
                let seek_idx = self.selected_sound_idx;

                let slider = egui::Slider::new(&mut self.seek_slider_value, 0.0..=total_time.max(0.01))
                    .show_value(false)
                    .text("");
                let slider_response = if slider_enabled {
                    let s = ui.add(slider);
                    tooltip_above(&s, ctx, "Drag to seek through the sound");
                    s
                } else {
                    ui.add_enabled(false, slider)
                };

                if slider_response.drag_released() && slider_enabled {
                    if let Some(idx) = seek_idx {
                        self.play_sound_at_index_with_offset(idx, Some(self.seek_slider_value));
                    }
                }
                if !slider_response.dragged() {
                    self.seek_slider_value = current_time;
                }

                let format_time = |secs: f32| {
                    let total_secs = secs as u32;
                    let mins = total_secs / 60;
                    let s = total_secs % 60;
                    format!("{}:{:02}", mins, s)
                };
                ui.label(format_time(self.seek_slider_value));
                ui.label("/");
                ui.label(format_time(total_time));

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.horizontal(|ui| {
                        let hp_lbl = ui.label(regular::HEADPHONES);
                        tooltip_above(&hp_lbl, ctx, "Monitoring volume (headphones)");
                        let hp_sl = ui.add_sized(
                            [80.0, 20.0],
                            egui::Slider::new(&mut self.config.volume_headphones, 0.0..=1.5)
                                .show_value(false),
                        );
                        tooltip_above(&hp_sl, ctx, "Headphone volume");
                    });
                    ui.separator();

                    ui.horizontal(|ui| {
                        let sp_lbl = ui.label(regular::SPEAKER_HIGH);
                        tooltip_above(&sp_lbl, ctx, "Sound output volume (virtual cable)");
                        let sp_sl = ui.add_sized(
                            [80.0, 20.0],
                            egui::Slider::new(&mut self.config.volume_mic, 0.0..=1.5)
                                .show_value(false),
                        );
                        tooltip_above(&sp_sl, ctx, "Soundboard volume");
                    });
                    ui.separator();

                    ui.horizontal(|ui| {
                        let mi_lbl = ui.label(regular::MICROPHONE);
                        tooltip_above(&mi_lbl, ctx, "Physical microphone sensitivity");
                        let mi_sl = ui.add_sized(
                            [80.0, 20.0],
                            egui::Slider::new(&mut self.config.volume_physical_mic, 0.0..=1.5)
                                .show_value(false),
                        );
                        tooltip_above(&mi_sl, ctx, "Real mic volume");
                    });
                });
            });
            ui.add_space(4.0);
        });
        }

        egui::TopBottomPanel::bottom("footer_panel")
            .frame(egui::Frame {
                inner_margin: egui::Margin::symmetric(8.0, 8.0),
                outer_margin: egui::Margin::ZERO,
                rounding: egui::Rounding::ZERO,
                shadow: egui::epaint::Shadow::NONE,
                fill: ctx.style().visuals.panel_fill,
                stroke: ctx.style().visuals.window_stroke,
            })
            .show(ctx, |ui| {
                if self.current_download.is_some() {
                    let height = 4.0;
                    let size = egui::vec2(ui.available_width(), height);
                    let (rect, _response) = ui.allocate_exact_size(size, egui::Sense::hover());
                    ui.painter().rect_filled(rect, egui::Rounding::ZERO, egui::Color32::from_gray(30));
                    let progress_width = rect.width() * self.download_progress;
                    let progress_rect = egui::Rect::from_min_size(rect.min, egui::vec2(progress_width, height));
                    ui.painter().rect_filled(progress_rect, egui::Rounding::ZERO, accent);
                    ui.add_space(6.0);
                }

                ui.horizontal(|ui| {
                    if self.selected_category_idx < self.config.categories.len() {
                        let current_icon = self.config.categories[self.selected_category_idx].icon.clone();
                        ui.label(egui::RichText::new(current_icon).size(24.0).color(accent));

                        let category_name = self.config.categories[self.selected_category_idx].name.clone();
                        ui.label(egui::RichText::new(&category_name).strong());

                        let sound_count = self.config.categories[self.selected_category_idx].sounds.len();
                        let selected_str = if let Some(idx) = self.selected_sound_idx {
                            format!("{}", idx + 1)
                        } else {
                            "0".to_string()
                        };

                        let total_plays: u32 = self.config.categories[self.selected_category_idx].sounds.iter().map(|s| s.play_count).sum();

                        ui.separator();
                        let s_lbl = ui.label(format!("Sounds: {}", sound_count));
                        tooltip_above(&s_lbl, ctx, "Total sounds in this category");
                        ui.separator();
                        let sel_lbl = ui.label(format!("Selected: {}", selected_str));
                        tooltip_above(&sel_lbl, ctx, "Currently selected sound index");
                        ui.separator();
                        let p_lbl = ui.label(format!("Play Count: {}", total_plays));
                        tooltip_above(&p_lbl, ctx, "Total times sounds in this category have been played");
                    }

                    if !self.download_queue.is_empty() {
                        ui.separator();
                        let q_lbl = ui.label(format!("Queue: {} item(s)", self.download_queue.len()));
                        tooltip_above(&q_lbl, ctx, "Sounds waiting to be downloaded");
                    }

                    if let Some(ref tag) = self.update_available {
                        ui.separator();
                        let btn_text = format!("⚡ Update Available: {} - View Release", tag);
                        let btn = ui.button(egui::RichText::new(btn_text).color(egui::Color32::LIGHT_GREEN));
                        if btn.clicked() {
                            ctx.open_url(egui::OpenUrl::new_tab(format!(
                                "https://github.com/makcnmflow/klwp-spad/releases/tag/{}",
                                tag
                            )));
                        }
                    }

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let gh_btn = ui.button(format!("{} GitHub", regular::GITHUB_LOGO));
                        if gh_btn.clicked() {
                            ctx.open_url(egui::OpenUrl::new_tab("https://github.com/makcnmflow/klwp-spad"));
                        }
                        tooltip_above(&gh_btn, ctx, "Open GitHub Repository");
                    });
                });
            });

        if self.show_logs {
            egui::TopBottomPanel::bottom("logs_panel")
                .resizable(true)
                .default_height(100.0)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.small("Logs Console");
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let cp_btn = ui.button("📋 Copy");
                            if cp_btn.clicked() {
                                let full_logs = self.logs.join("\n");
                                ctx.copy_text(full_logs);
                            }
                            tooltip_above(&cp_btn, ctx, "Copy all log records to clipboard");
                            let hd_btn = ui.button("Hide");
                            if hd_btn.clicked() {
                                self.show_logs = false;
                            }
                            tooltip_above(&hd_btn, ctx, "Close the logs panel");
                        });
                    });
                    ui.separator();
                    egui::ScrollArea::vertical().id_source("logs_scroll").show(ui, |ui| {
                        for log in &self.logs {
                            ui.small(log);
                        }
                    });
                });
        }

        egui::SidePanel::left("categories_panel")
            .resizable(true)
            .default_width(220.0)
            .width_range(160.0..=450.0)
            .show(ctx, |ui| {
                let cat_h = ui.heading("Categories");
                tooltip_above(&cat_h, ctx, "Right-click a category to rename or change its icon");
                ui.separator();

                egui::ScrollArea::vertical().id_source("categories_scroll").show(ui, |ui| {
                    ui.set_min_width(ui.available_width());

                    for i in 0..self.config.categories.len() {
                        let count = self.config.categories[i].sounds.len();
                        let is_selected = self.selected_category_idx == i;
                        let cat_icon = self.config.categories[i].icon.clone();
                        let cat_name = self.config.categories[i].name.clone();

                        ui.horizontal(|ui| {
                            ui.label(&cat_icon);
                            let resp = ui.selectable_label(is_selected, &cat_name);
                            if resp.clicked() {
                                self.selected_category_idx = i;
                                self.selected_sound_idx = None;
                                self.update_sorted_indices();
                                self.log_info(&format!("Navigated to category: '{}'", cat_name));
                            }

                            resp.context_menu(|ui| {
                                ui.set_min_width(180.0);
                                ui.label(egui::RichText::new("Rename Category").strong());

                                let edit_id = ui.make_persistent_id(format!("cat_edit_{}", i));
                                let mut temp_name = ui.data(|d| d.get_temp::<String>(edit_id))
                                    .unwrap_or_else(|| cat_name.clone());

                                let name_edit = ui.text_edit_singleline(&mut temp_name);
                                ui.data_mut(|d| d.insert_temp(edit_id, temp_name.clone()));

                                if name_edit.lost_focus() || (name_edit.gained_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) {
                                    let trimmed = temp_name.trim().to_string();
                                    if !trimmed.is_empty() && trimmed != cat_name {
                                        rename_cmd = Some((i, trimmed));
                                    }
                                    ui.data_mut(|d| d.remove::<String>(edit_id));
                                }

                                ui.separator();
                                ui.label(egui::RichText::new("Select Icon").strong());

                                ui.horizontal_wrapped(|ui| {
                                    for icon in AVAILABLE_ICONS {
                                        if ui.button(*icon).clicked() {
                                            icon_cmd = Some((i, icon.to_string()));
                                            ui.close_menu();
                                        }
                                    }
                                });
                            });

                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.label(format!("{}", count));
                            });
                        });
                    }
                });

                ui.separator();

                ui.horizontal(|ui| {
                    let name_inp = ui.add(egui::TextEdit::singleline(&mut self.new_category_name)
                        .hint_text("Name...")
                        .desired_width(70.0));
                    tooltip_above(&name_inp, ctx, "Type a name and click + to create a new category");

                    let add_cat = ui.button(format!("{} Add", regular::PLUS));
                    if add_cat.clicked() {
                        let name = self.new_category_name.trim().to_string();
                        if !name.is_empty() && !self.config.categories.iter().any(|c| c.name == name) {
                            self.config.categories.push(CategoryConfig {
                                name: name.clone(),
                                sounds: vec![],
                                icon: "📁".to_string(),
                            });
                            self.new_category_name.clear();
                            self.save_app_config();
                            self.log_info(&format!("Created new playlist category: '{}'", name));
                        }
                    }
                    tooltip_above(&add_cat, ctx, "Create a new category folder");
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    let add_snd = ui.button(format!("{} Add sound to list...", regular::PLUS));
                    if add_snd.clicked() {
                        if let Some(path) = rfd::FileDialog::new().pick_file()
                        {
                            let title = path.file_stem()
                                .map(|s| s.to_string_lossy().into_owned())
                                .unwrap_or_else(|| "Unnamed".to_string());

                            let mut final_path = path.clone();
                            let mut duration = get_duration_str(&final_path);

                            if duration == "0:00" {
                                if let Some(converted) = try_convert_with_ffmpeg(&final_path.display().to_string()) {
                                    final_path = std::path::PathBuf::from(&converted);
                                    duration = get_duration_str(&final_path);
                                    self.log_info(&format!("Converted '{}' to WAV via ffmpeg", title));
                                } else {
                                    self.log_warn(&format!("'{}' format not supported by rodio or ffmpeg", title));
                                }
                            }

                            if self.selected_category_idx < self.config.categories.len() {
                                let duration_secs = get_duration_seconds(&final_path.display().to_string());
                                self.config.categories[self.selected_category_idx].sounds.push(SoundConfig {
                                    title: title.clone(),
                                    path: final_path.display().to_string(),
                                    duration,
                                    hotkey: None,
                                    play_count: 0,
                                    duration_secs,
                                });
                                self.save_app_config();
                                self.update_sorted_indices();
                                self.log_info(&format!("Imported: '{}'", title));
                            }
                        }
                    }
                    tooltip_above(&add_snd, ctx, "Import an audio file (MP3, WAV, FLAC, OGG)");
                });

                ui.add_space(5.0);

                if self.selected_category_idx < self.config.categories.len() {
                    egui::ScrollArea::both().id_source("sound_table_scroll").show(ui, |ui| {
                        egui::Grid::new("sound_table_grid")
                            .striped(true)
                            .num_columns(5)
                            .spacing([15.0, 8.0])
                            .show(ui, |ui| {
                                fn sort_label(col: &Option<SortColumn>, asc: bool, target: &SortColumn) -> String {
                                    match col {
                                        Some(c) if c == target => {
                                            if asc { format!(" {}", regular::CARET_UP) } else { format!(" {}", regular::CARET_DOWN) }
                                        }
                                        _ => "".to_string()
                                    }
                                }

                                let n_btn = ui.button(format!("No.{}", sort_label(&self.sort_column, self.sort_ascending, &SortColumn::Number)));
                                if n_btn.clicked() {
                                    self.selected_sound_idx = None;
                                    if self.sort_column == Some(SortColumn::Number) {
                                        self.sort_ascending = !self.sort_ascending;
                                    } else {
                                        self.sort_column = Some(SortColumn::Number);
                                        self.sort_ascending = true;
                                    }
                                    self.update_sorted_indices();
                                }
                                tooltip_above(&n_btn, ctx, "Sort by index");

                                let t_btn = ui.button(format!("Title{}", sort_label(&self.sort_column, self.sort_ascending, &SortColumn::Title)));
                                if t_btn.clicked() {
                                    self.selected_sound_idx = None;
                                    if self.sort_column == Some(SortColumn::Title) {
                                        self.sort_ascending = !self.sort_ascending;
                                    } else {
                                        self.sort_column = Some(SortColumn::Title);
                                        self.sort_ascending = true;
                                    }
                                    self.update_sorted_indices();
                                }
                                tooltip_above(&t_btn, ctx, "Sort alphabetically");

                                let d_btn = ui.button(format!("Duration{}", sort_label(&self.sort_column, self.sort_ascending, &SortColumn::Duration)));
                                if d_btn.clicked() {
                                    self.selected_sound_idx = None;
                                    if self.sort_column == Some(SortColumn::Duration) {
                                        self.sort_ascending = !self.sort_ascending;
                                    } else {
                                        self.sort_column = Some(SortColumn::Duration);
                                        self.sort_ascending = true;
                                    }
                                    self.update_sorted_indices();
                                }
                                tooltip_above(&d_btn, ctx, "Sort by length");

                                let h_lbl = ui.label("Hotkey");
                                tooltip_above(&h_lbl, ctx, "Keyboard shortcut - click to assign or change");
                                let del_lbl = ui.label("Delete");
                                tooltip_above(&del_lbl, ctx, "Remove this sound from the list");
                                ui.end_row();

                                let mut to_remove = None;

                                let cat_idx = self.selected_category_idx;
                                let sorted = self.sorted_indices.clone();
                                for &original_idx in &sorted {
                                    let is_selected = Some(original_idx) == self.selected_sound_idx;
                                    let sound_title = self.config.categories[cat_idx].sounds[original_idx].title.clone();
                                    let sound_duration = self.config.categories[cat_idx].sounds[original_idx].duration.clone();
                                    let sound_hotkey_opt = self.config.categories[cat_idx].sounds[original_idx].hotkey.clone();

                                    ui.label((original_idx + 1).to_string());

                                    let resp = ui.selectable_label(is_selected, &sound_title);
                                    if resp.clicked() {
                                        self.selected_sound_idx = Some(original_idx);
                                    }
                                    if resp.double_clicked() {
                                        self.selected_sound_idx = Some(original_idx);
                                        self.play_sound_at_index(original_idx);
                                    }

                                    resp.context_menu(|ui| {
                                        ui.set_min_width(180.0);
                                        ui.label(egui::RichText::new("Rename Sound").strong());

                                        let edit_id = ui.make_persistent_id(format!("snd_edit_{}", original_idx));
                                        let mut temp_name = ui.data(|d| d.get_temp::<String>(edit_id))
                                            .unwrap_or_else(|| sound_title.clone());

                                        let name_edit = ui.text_edit_singleline(&mut temp_name);
                                        ui.data_mut(|d| d.insert_temp(edit_id, temp_name.clone()));

                                        if name_edit.lost_focus() || (name_edit.gained_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) {
                                            let trimmed = temp_name.trim().to_string();
                                            if !trimmed.is_empty() && trimmed != sound_title {
                                                self.sound_rename_cmd = Some((original_idx, trimmed));
                                            }
                                            ui.data_mut(|d| d.remove::<String>(edit_id));
                                        }
                                    });

                                    ui.label(&sound_duration);

                                    let hk_text = match &sound_hotkey_opt {
                                        Some(k) => format!("  {}  ", k),
                                        None => "Assign".to_string(),
                                    };

                                    let hk_btn = ui.button(hk_text);
                                    if hk_btn.clicked() {
                                        if sound_hotkey_opt.is_some() {
                                            self.hotkey_options_idx = Some(original_idx);
                                        } else {
                                            self.recording_state = Some(RecordingState {
                                                sound_idx: original_idx,
                                                recorded_combination: None,
                                            });
                                        }
                                    }
                                    let hk_tip = if sound_hotkey_opt.is_some() { "Click to change or remove this hotkey" } else { "Click to assign a keyboard shortcut" };
                                    tooltip_above(&hk_btn, ctx, hk_tip);

                                    let tr_btn = ui.button(regular::TRASH);
                                    if tr_btn.clicked() {
                                        to_remove = Some(original_idx);
                                    }
                                    tooltip_above(&tr_btn, ctx, "Delete this sound");

                                    ui.end_row();
                                }

                                if let Some(idx) = to_remove {
                                    let sound = &self.config.categories[self.selected_category_idx].sounds[idx];
                                    let title = sound.title.clone();
                                    let sound_path = sound.path.clone();

                                    let dl_dir = get_exe_dir().join("sounds");
                                    let dl_dir_str = dl_dir.display().to_string().replace('\\', "/");
                                    let path_str = sound_path.replace('\\', "/");
                                    if path_str.starts_with(&dl_dir_str) {
                                        let _ = std::fs::remove_file(&sound_path);
                                        self.log_info(&format!("Deleted downloaded file: '{}'", sound_path));
                                    }

                                    self.log_info(&format!("Removed audio file from playlist: '{}'", title));
                                    self.config.categories[self.selected_category_idx].sounds.remove(idx);
                                    self.selected_sound_idx = None;
                                    self.save_app_config();
                                    self.update_sorted_indices();
                                    self.update_global_hotkeys();
                                }
                            });
                    });
                }
            });
        });

        let mut save_combination = None;
        let mut sound_idx_to_save = 0;
        let mut should_close_rec = false;

        if let Some(ref mut rec) = self.recording_state {
            let current_sound_idx = rec.sound_idx;

            if let Some(ref combo) = newly_pressed_combination {
                rec.recorded_combination = Some(combo.clone());
            }

            egui::Window::new("Hotkey Recorder")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
                .show(ctx, |ui| {
                    ui.set_min_width(320.0);
                    ui.heading("Press Hotkey Combination...");
                    ui.small("Supported modifiers: Ctrl, Shift, Alt");
                    ui.add_space(15.0);

                    let combo_cloned = rec.recorded_combination.clone();

                    if let Some(combo) = combo_cloned {
                        ui.colored_label(egui::Color32::LIGHT_GREEN, format!("Detected: {}", combo));
                        ui.add_space(15.0);

                        ui.horizontal(|ui| {
                            let sv_btn = ui.button("Save");
                            if sv_btn.clicked() {
                                save_combination = Some(combo.clone());
                                sound_idx_to_save = current_sound_idx;
                                should_close_rec = true;
                            }
                            tooltip_above(&sv_btn, ctx, "Save this hotkey combination");
                            let rs_btn = ui.button("Reset");
                            if rs_btn.clicked() {
                                rec.recorded_combination = None;
                            }
                            tooltip_above(&rs_btn, ctx, "Clear and try again");
                        });
                    } else {
                        ui.colored_label(egui::Color32::LIGHT_YELLOW, "Awaiting keys...");
                    }

                    ui.add_space(10.0);
                    let cc_btn = ui.button("Cancel");
                    if cc_btn.clicked() {
                        should_close_rec = true;
                    }
                    tooltip_above(&cc_btn, ctx, "Close without saving");
                });
        }

        if should_close_rec {
            self.recording_state = None;
        }

        if let Some(combo) = save_combination {
            self.config.categories[self.selected_category_idx].sounds[sound_idx_to_save].hotkey = Some(combo.clone());
            self.save_app_config();
            self.update_global_hotkeys();
            self.log_info(&format!("Assigned shortcut combination '{}' to index #{}", combo, sound_idx_to_save + 1));
        }

        let mut should_close_hk_options = false;
        let mut should_change_hk = false;
        let mut should_remove_hk = false;
        let mut hk_option_idx = 0;

        if let Some(idx) = self.hotkey_options_idx {
            hk_option_idx = idx;
            let current_hk = self.config.categories[self.selected_category_idx].sounds[idx].hotkey.clone();

            egui::Window::new("Hotkey Options")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
                .show(ctx, |ui| {
                    ui.set_min_width(280.0);
                    ui.heading("Hotkey");
                    ui.add_space(10.0);

                    if let Some(hk) = &current_hk {
                        ui.label(format!("Current hotkey: {}", hk));
                    }
                    ui.add_space(15.0);

                    ui.horizontal(|ui| {
                        let ch_btn = ui.button("Change Hotkey");
                        if ch_btn.clicked() {
                            should_change_hk = true;
                            should_close_hk_options = true;
                        }
                        tooltip_above(&ch_btn, ctx, "Assign a new keyboard shortcut");
                        let rm_btn = ui.button("Remove Hotkey");
                        if rm_btn.clicked() {
                            should_remove_hk = true;
                            should_close_hk_options = true;
                        }
                        tooltip_above(&rm_btn, ctx, "Clear the current hotkey");
                    });
                    ui.add_space(10.0);
                    let cl_btn = ui.button("Close");
                    if cl_btn.clicked() {
                        should_close_hk_options = true;
                    }
                    tooltip_above(&cl_btn, ctx, "Return to sound list");
                });
        }

        if should_close_hk_options {
            self.hotkey_options_idx = None;
        }

        if should_change_hk {
            self.recording_state = Some(RecordingState {
                sound_idx: hk_option_idx,
                recorded_combination: None,
            });
        }

        if should_remove_hk {
            self.config.categories[self.selected_category_idx].sounds[hk_option_idx].hotkey = None;
            self.save_app_config();
            self.update_global_hotkeys();
            self.log_info(&format!("Removed hotkey from index #{}", hk_option_idx + 1));
        }

        if self.show_settings {
            let window_title = if self.config.is_first_run {
                "Welcome! First-time setup"
            } else {
                "Settings Panel"
            };

            egui::Window::new(window_title)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
                .show(ctx, |ui| {
                    ui.set_min_width(500.0);
                    ui.set_max_height(450.0);

                    if !self.config.is_first_run {
                        ui.horizontal(|ui| {
                            let d_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::Devices, "Devices");
                            tooltip_above(&d_tab, ctx, "Audio input/output device selection");
                            let h_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::Hotkeys, "Hotkeys");
                            tooltip_above(&h_tab, ctx, "Global keyboard shortcut settings");
                            let au_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::Audio, "Audio");
                            tooltip_above(&au_tab, ctx, "Volume levels and audio behavior");
                            let a_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::Appearance, "Appearance");
                            tooltip_above(&a_tab, ctx, "Theme colors, font size, and Discord RPC");
                            let c_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::Categories, "Categories");
                            tooltip_above(&c_tab, ctx, "Manage category folders and icons");
                            let ab_tab = ui.selectable_value(&mut self.settings_tab, SettingsTab::About, "About");
                            tooltip_above(&ab_tab, ctx, "Version info and credits");
                        });
                        ui.separator();
                    } else {
                        ui.colored_label(egui::Color32::LIGHT_BLUE, "Please configure your primary audio devices to proceed.");
                        ui.add_space(10.0);
                    }

                    match self.settings_tab {
                        SettingsTab::Devices => {
                            egui::Grid::new("settings_devices_grid").spacing([10.0, 10.0]).show(ui, |ui| {
                                let mic_lbl = ui.label("Microphone:");
                                tooltip_above(&mic_lbl, ctx, "Your physical microphone input device");
                                egui::ComboBox::from_id_source("set_mic")
                                    .selected_text(&self.config.selected_input)
                                    .show_ui(ui, |ui| {
                                        for dev in &self.input_devices {
                                            ui.selectable_value(&mut self.config.selected_input, dev.clone(), dev);
                                        }
                                    });
                                ui.end_row();

                                let vc_lbl = ui.label("Virtual Cable:");
                                tooltip_above(&vc_lbl, ctx, "The VB-Cable output device that sends audio to Discord/voice chat");
                                egui::ComboBox::from_id_source("set_cable")
                                    .selected_text(&self.config.selected_output)
                                    .show_ui(ui, |ui| {
                                        for dev in &self.output_devices {
                                            ui.selectable_value(&mut self.config.selected_output, dev.clone(), dev);
                                        }
                                    });
                                ui.end_row();

                                let hp_lbl = ui.label("Headphones:");
                                tooltip_above(&hp_lbl, ctx, "Your headphones or monitoring device to hear soundboard playback");
                                egui::ComboBox::from_id_source("set_mon")
                                    .selected_text(&self.config.selected_monitoring)
                                    .show_ui(ui, |ui| {
                                        for dev in &self.monitoring_devices {
                                            ui.selectable_value(&mut self.config.selected_monitoring, dev.clone(), dev);
                                        }
                                    });
                                ui.end_row();
                            });

                            ui.separator();
                            let mute_cb = ui.checkbox(&mut self.config.mute_mic_during_playback, "Mute microphone while sound plays");
                            tooltip_above(&mute_cb, ctx, "Automatically lower your real mic when a soundboard sound is playing, to prevent echo");
                        }
                        SettingsTab::Hotkeys => {
                            let gh_cb = ui.checkbox(&mut self.config.enable_global_hotkeys, "Global hotkeys");
                            tooltip_above(&gh_cb, ctx, "Let sound hotkeys work even when the app window is not focused");
                            ui.small("Disable this if hotkeys interfere with other apps.");
                            ui.add_space(10.0);

                            let rst_hk = ui.button("Reset all hotkeys");
                            if rst_hk.clicked() {
                                for category in &mut self.config.categories {
                                    for sound in &mut category.sounds {
                                        sound.hotkey = None;
                                    }
                                }
                                self.save_app_config();
                                self.update_global_hotkeys();
                                self.log_warn("All registered shortcut configurations have been cleared.");
                            }
                            tooltip_above(&rst_hk, ctx, "Remove all assigned hotkeys from every sound");
                        }
                        SettingsTab::Audio => {
                            ui.label("Volume levels (0–150%):");
                            ui.add_space(4.0);
                            egui::Grid::new("vol_grid").spacing([10.0, 6.0]).show(ui, |ui| {
                                let mut vol_mic_pct = (self.config.volume_mic * 100.0) as u32;
                                ui.label("Soundboard:");
                                ui.add(egui::Slider::new(&mut self.config.volume_mic, 0.0..=1.5).text(""));
                                let vl_mic = ui.add(egui::DragValue::new(&mut vol_mic_pct).speed(1).clamp_range(0..=150));
                                if vl_mic.changed() || vl_mic.lost_focus() {
                                    self.config.volume_mic = (vol_mic_pct as f32 / 100.0).clamp(0.0, 1.5);
                                }
                                ui.end_row();

                                let mut vol_hp_pct = (self.config.volume_headphones * 100.0) as u32;
                                ui.label("Headphones:");
                                ui.add(egui::Slider::new(&mut self.config.volume_headphones, 0.0..=1.5).text(""));
                                let vl_hp = ui.add(egui::DragValue::new(&mut vol_hp_pct).speed(1).clamp_range(0..=150));
                                if vl_hp.changed() || vl_hp.lost_focus() {
                                    self.config.volume_headphones = (vol_hp_pct as f32 / 100.0).clamp(0.0, 1.5);
                                }
                                ui.end_row();

                                let mut vol_mic_phys_pct = (self.config.volume_physical_mic * 100.0) as u32;
                                ui.label("Microphone:");
                                ui.add(egui::Slider::new(&mut self.config.volume_physical_mic, 0.0..=1.5).text(""));
                                let vl_mp = ui.add(egui::DragValue::new(&mut vol_mic_phys_pct).speed(1).clamp_range(0..=150));
                                if vl_mp.changed() || vl_mp.lost_focus() {
                                    self.config.volume_physical_mic = (vol_mic_phys_pct as f32 / 100.0).clamp(0.0, 1.5);
                                }
                                ui.end_row();
                            });
                        }
                        SettingsTab::Appearance => {
                            egui::Grid::new("set_app_grid").spacing([10.0, 10.0]).show(ui, |ui| {
                                let ac_lbl = ui.label("Accent Color:");
                                tooltip_above(&ac_lbl, ctx, "Change the theme accent color for buttons and highlights");
                                egui::ComboBox::from_id_source("set_accent")
                                    .selected_text(&self.config.accent_color)
                                    .show_ui(ui, |ui| {
                                        for col in &["Blue", "Red", "Green", "Purple", "Orange"] {
                                            ui.selectable_value(&mut self.config.accent_color, col.to_string(), *col);
                                        }
                                    });
                                ui.end_row();

                                let fs_lbl = ui.label("Font Size:");
                                tooltip_above(&fs_lbl, ctx, "Adjust the UI text size");
                                ui.add(egui::Slider::new(&mut self.config.font_size, 11.0..=22.0).text("px"));
                                ui.end_row();
                            });
                            ui.separator();
                            let drpc_cb = ui.checkbox(&mut self.config.enable_discord_rpc, "Discord Rich Presence");
                            tooltip_above(&drpc_cb, ctx, "Show 'Playing KLWP SPAD' in your Discord status");
                            let qc_cb = ui.checkbox(&mut self.config.show_quick_controls, "Quick controls bar");
                            tooltip_above(&qc_cb, ctx, "Show the top toolbar with play/pause and volume sliders");
                            let focus_cb = ui.checkbox(&mut self.config.focus_on_sound_link, "Focus window on sound link");
                            tooltip_above(&focus_cb, ctx, "Automatically bring the app to front when a soundpad:// or voicemod: link is received");
                            let log_cb = ui.checkbox(&mut self.show_logs, "Logs Console");
                            tooltip_above(&log_cb, ctx, "Show a debug log panel at the bottom of the window");
                        }
                        SettingsTab::Categories => {
                            ui.label("Manage Categories:");
                            ui.small("Change custom icons and delete categories here.");
                            ui.add_space(10.0);

                            let mut to_remove = None;

                            egui::ScrollArea::vertical().id_source("settings_categories_scroll").show(ui, |ui| {
                                egui::Grid::new("settings_categories_grid")
                                    .striped(true)
                                    .num_columns(3)
                                    .spacing([15.0, 10.0])
                                    .show(ui, |ui| {
                                        for i in 0..self.config.categories.len() {
                                            let mut cat = self.config.categories[i].clone();

                                            ui.label(&cat.name);

                                            egui::ComboBox::from_id_source(format!("icon_select_{}", i))
                                                .selected_text(&cat.icon)
                                                .show_ui(ui, |ui| {
                                                    for icon in AVAILABLE_ICONS {
                                                        let icon_str = icon.to_string();
                                                        if ui.selectable_value(&mut cat.icon, icon_str.clone(), icon_str).clicked() {
                                                            self.config.categories[i].icon = cat.icon.clone();
                                                            self.save_app_config();
                                                            self.log_info(&format!("Updated category '{}' icon to {}", cat.name, cat.icon));
                                                        }
                                                    }
                                                });

                                            let can_delete = self.config.categories.len() > 1;
                                            let btn = ui.add_enabled(can_delete, egui::Button::new(format!("{} Delete", regular::TRASH)));
                                            if btn.clicked() {
                                                to_remove = Some(i);
                                            }

                                            ui.end_row();
                                        }
                                    });
                            });

                            if let Some(idx) = to_remove {
                                let removed_name = self.config.categories[idx].name.clone();
                                self.config.categories.remove(idx);
                                self.selected_category_idx = 0;
                                self.selected_sound_idx = None;
                                self.update_sorted_indices();
                                self.save_app_config();
                                self.update_global_hotkeys();
                                self.log_info(&format!("Deleted category: '{}'", removed_name));
                            }
                        }
                        SettingsTab::About => {
                            ui.vertical_centered(|ui| {
                                ui.add_space(10.0);
                                ui.label(
                                    egui::RichText::new("KLWP SPAD")
                                        .size(36.0)
                                        .strong()
                                        .monospace()
                                        .color(accent),
                                );
                                ui.label(
                                    egui::RichText::new("it means killwinparty soundpad")
                                        .size(14.0)
                                        .monospace()
                                        .color(egui::Color32::GRAY),
                                );
                                ui.add_space(15.0);
                                ui.label(egui::RichText::new("Created by killwinparty (klwp)").strong());
                                ui.add_space(5.0);
                                ui.small("A simple Soundpad clone made with Rust.");
                                ui.small("I made this like in one day XD");

                                ui.add_space(15.0);
                                let version = env!("APP_VERSION");
                                let ver_str = if version.starts_with(|c: char| c.is_ascii_digit()) {
                                    format!("v{}", version)
                                } else {
                                    version.to_string()
                                };
                                ui.label(egui::RichText::new(ver_str).monospace().color(egui::Color32::GRAY));
                            });
                        }
                    }

                    ui.separator();
                    ui.horizontal(|ui| {
                        let button_text = if self.config.is_first_run { "Finish Setup" } else { "Apply and Close" };

                        let apply_btn = ui.button(button_text);
                        if apply_btn.clicked() {
                            self.config.is_first_run = false;
                            self.show_settings = false;

                            if self.config.selected_output.is_empty() {
                                let host = cpal::default_host();
                                let devices: Vec<String> = host
                                    .output_devices()
                                    .map(|d| d.filter_map(|d| d.name().ok()).collect())
                                    .unwrap_or_default();
                                let auto = find_virtual_cable_output_name(&devices);
                                if !auto.is_empty() {
                                    self.config.selected_output = auto;
                                }
                            }

                            self.save_app_config();
                            self.pending_apply = true;
                        }
                        tooltip_above(&apply_btn, ctx, "Save settings and restart audio streaming");
                    });
                });
        }

        if let Some((idx, new_name)) = rename_cmd {
            let old_name = self.config.categories[idx].name.clone();
            self.config.categories[idx].name = new_name.clone();
            self.save_app_config();
            self.log_info(&format!("Renamed category '{}' to '{}'", old_name, new_name));
        }

        if let Some((idx, new_icon)) = icon_cmd {
            let cat_name = self.config.categories[idx].name.clone();
            self.config.categories[idx].icon = new_icon.clone();
            self.save_app_config();
            self.log_info(&format!("Changed category '{}' icon to {}", cat_name, new_icon));
        }

        if let Some((idx, new_name)) = self.sound_rename_cmd.take() {
            if self.selected_category_idx < self.config.categories.len() {
                let cat = &mut self.config.categories[self.selected_category_idx];
                if idx < cat.sounds.len() {
                    let old_name = cat.sounds[idx].title.clone();
                    cat.sounds[idx].title = new_name.clone();
                    self.save_app_config();
                    self.log_info(&format!("Renamed sound '{}' to '{}'", old_name, new_name));
                }
            }
        }

        if self.pending_apply {
            self.pending_apply = false;
            self.input_stream = None;
            self.output_stream = None;
            self.monitoring_stream = None;
            self.update_global_hotkeys();
            let _ = self.discord_tx.send(DiscordMsg::UpdateStatus {
                enabled: self.config.enable_discord_rpc,
            });
            let auto_cable_mic = find_virtual_cable_microphone(&self.config.selected_output, &self.input_devices);
            set_default_windows_microphone(&auto_cable_mic);
            self.status_message = "Settings applied. Audio will restart on next playback.".to_string();
            self.log_info("Settings applied. Audio streams will restart on next playback.");
        }

        ctx.request_repaint_after(std::time::Duration::from_millis(30));
    }
}

fn tooltip_above(response: &egui::Response, ctx: &egui::Context, text: &str) {
    if response.hovered() {
        let pos = egui::pos2(
            (response.rect.center().x - 60.0).max(0.0),
            (response.rect.top() - 28.0).max(0.0),
        );
        egui::Area::new(egui::Id::new(("tooltip", text)))
            .fixed_pos(pos)
            .order(egui::Order::Tooltip)
            .show(ctx, |ui| {
                egui::Frame::popup(&ctx.style()).show(ui, |ui| {
                    ui.label(text);
                });
            });
    }
}

fn is_modifier_key(_key: egui::Key) -> bool {
    false
}

fn parse_version(v: &str) -> Vec<u32> {
    v.trim_start_matches(|c: char| c.is_alphabetic())
        .split('.')
        .filter_map(|s| s.parse::<u32>().ok())
        .collect()
}

fn is_newer_version(current: &str, latest: &str) -> bool {
    let current_parts = parse_version(current);
    let latest_parts = parse_version(latest);
    for (l, c) in latest_parts.iter().zip(current_parts.iter()) {
        if l > c { return true; }
        if l < c { return false; }
    }
    latest_parts.len() > current_parts.len()
}

fn map_key_to_hotkey_string(key: egui::Key, modifiers: &egui::Modifiers) -> String {
    let mut parts = Vec::new();
    if modifiers.ctrl { parts.push("ctrl"); }
    if modifiers.shift { parts.push("shift"); }
    if modifiers.alt { parts.push("alt"); }

    let key_str = format!("{:?}", key);
    let mapped_key = match key_str.as_str() {
        "Num0" => "0", "Num1" => "1", "Num2" => "2", "Num3" => "3",
        "Num4" => "digit4", "Num5" => "5", "Num6" => "6", "Num7" => "7",
        "Num8" => "8", "Num9" => "9",
        "Space" => "space",
        "Enter" => "enter",
        "Tab" => "tab",
        "Escape" => "escape",
        _ => &key_str.to_lowercase(),
    };
    parts.push(mapped_key);
    parts.join("+")
}