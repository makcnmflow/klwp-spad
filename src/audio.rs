use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::HeapRb;
use rodio::Source;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

pub struct ActiveSound {
    pub consumer_mic: ringbuf::Consumer<f32, Arc<HeapRb<f32>>>,
    pub consumer_headphones: ringbuf::Consumer<f32, Arc<HeapRb<f32>>>,
    pub stop_signal: Arc<AtomicBool>,
    pub finished_decoding: Arc<AtomicBool>,
}

pub struct AudioState {
    pub active_sound: Option<ActiveSound>,
    pub volume_mic: f32,
    pub volume_headphones: f32,
    pub volume_physical_mic: f32,
    pub is_paused: bool,
    pub mute_mic_during_playback: bool,
    pub current_sample_index: usize,
    pub total_samples: usize,
    pub sample_rate: u32,
}

pub fn get_duration_seconds(path: &str) -> f32 {
    let path_obj = std::path::Path::new(path);
    if let Ok(file) = File::open(path_obj) {
        if let Ok(decoder) = rodio::Decoder::new(BufReader::new(file)) {
            let sample_rate = decoder.sample_rate();
            let channels = decoder.channels();
            let total_samples = decoder.count();
            if sample_rate > 0 && channels > 0 {
                return total_samples as f32 / sample_rate as f32 / channels as f32;
            }
        }
    }
    0.0
}

pub fn get_duration_str(path: &std::path::Path) -> String {
    if let Ok(file) = File::open(path) {
        if let Ok(decoder) = rodio::Decoder::new(BufReader::new(file)) {
            let sample_rate = decoder.sample_rate();
            let channels = decoder.channels();
            let total_samples = decoder.count();

            if sample_rate > 0 && channels > 0 {
                let total_secs = (total_samples as f32 / sample_rate as f32 / channels as f32) as u64;
                let mins = total_secs / 60;
                let secs = total_secs % 60;
                return format!("{}:{:02}", mins, secs);
            }
        }
    }
    "0:00".to_string()
}

pub fn load_decoder_stream(
    path: &str,
    target_sample_rate: u32,
) -> Result<Box<dyn Iterator<Item = f32> + Send>, Box<dyn std::error::Error>> {
    use rodio::source::UniformSourceIterator;
    let file = File::open(path)?;
    let source = rodio::Decoder::new(BufReader::new(file))?.convert_samples::<f32>();
    let resampled = UniformSourceIterator::new(source, 1, target_sample_rate);
    Ok(Box::new(resampled))
}

pub fn find_virtual_cable_output_name(output_devices: &[String]) -> String {
    for dev in output_devices {
        let name = dev.to_lowercase();
        if name.contains("cable") || name.contains("vb-audio") {
            return dev.clone();
        }
    }
    String::new()
}

pub fn find_any_virtual_cable_output(host: &cpal::Host) -> Option<cpal::Device> {
    host.output_devices().ok()?.find(|d| {
        d.name().ok().map(|n| {
            let n = n.to_lowercase();
            n.contains("cable") || n.contains("vb-audio")
        }).unwrap_or(false)
    })
}

pub fn start_audio_streams(
    host: &cpal::Host,
    input_device_name: &str,
    cable_device_name: &str,
    monitoring_device_name: &str,
    audio_state: Arc<Mutex<AudioState>>,
) -> Result<(cpal::Stream, cpal::Stream, Option<cpal::Stream>, u32, u32), Box<dyn std::error::Error>> {
    let input_device = host
        .input_devices()?
        .find(|d| d.name().map(|n| n == input_device_name).unwrap_or(false))
        .ok_or("Microphone not found. Please select an active device in settings.")?;

    let cable_lower = cable_device_name.to_lowercase();
    let output_device = host
        .output_devices()?
        .find(|d| {
            d.name().ok().map(|name| {
                let n_lower = name.to_lowercase();
                !cable_lower.is_empty() && (n_lower == cable_lower || n_lower.contains(&cable_lower))
            }).unwrap_or(false)
        })
        .or_else(|| find_any_virtual_cable_output(host))
        .ok_or("Virtual cable not found. Please specify a valid cable in settings.")?;

    let input_config = input_device.default_input_config()?;
    let output_config = output_device.default_output_config()?;

    let output_sample_rate = output_config.sample_rate().0;

    let rb = HeapRb::<f32>::new(1024);
    let (mut producer, mut consumer) = rb.split();

    let input_channels = input_config.channels() as usize;

    let input_stream = input_device.build_input_stream(
        &input_config.config(),
        move |data: &[f32], _: &cpal::InputCallbackInfo| {
            let mut i = 0;
            while i < data.len() {
                let sample = data[i];
                let _ = producer.push(sample);
                i += input_channels;
            }
        },
        |err| eprintln!("Microphone error: {:?}", err),
        None,
    )?;

    let mut monitoring_stream = None;
    let mut monitoring_rate = 44100;

    if monitoring_device_name != "[Disabled]" && !monitoring_device_name.is_empty() {
        if let Ok(mut mon_devices) = host.output_devices() {
            if let Some(mon_device) = mon_devices
                .find(|d| d.name().map(|n| n == monitoring_device_name).unwrap_or(false))
            {
                if let Ok(mon_config) = mon_device.default_output_config() {
                    monitoring_rate = mon_config.sample_rate().0;
                    let mon_channels = mon_config.channels() as usize;
                    let audio_state_clone = Arc::clone(&audio_state);

                    let mon_stream = mon_device.build_output_stream(
                        &mon_config.config(),
                        move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                            let mut state = audio_state_clone.lock().unwrap();
                            let mut i = 0;
                            let vol = state.volume_headphones;
                            let is_paused = state.is_paused;

                            while i < data.len() {
                                let mut sound_sample: f32 = 0.0;

                                if !is_paused {
                                    if let Some(ref mut sound) = state.active_sound {
                                        sound_sample = sound.consumer_headphones.pop().unwrap_or(0.0) * vol;
                                    }
                                }

                                for _ in 0..mon_channels {
                                    if i < data.len() {
                                        data[i] = sound_sample.clamp(-1.0, 1.0);
                                        i += 1;
                                    }
                                }
                            }
                        },
                        |err| eprintln!("Monitoring error: {:?}", err),
                        None,
                    )?;
                    mon_stream.play()?;
                    monitoring_stream = Some(mon_stream);
                }
            }
        }
    }

    let output_channels = output_config.channels() as usize;
    let audio_state_clone = Arc::clone(&audio_state);

    let output_stream = output_device.build_output_stream(
        &output_config.config(),
        move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
            let mut state = audio_state_clone.lock().unwrap();
            let mut i = 0;
            let vol_mic = state.volume_mic;
            let vol_physical = state.volume_physical_mic;
            let is_paused = state.is_paused;
            let mute_mic = state.mute_mic_during_playback;

            while i < data.len() {
                let mic_playing = !is_paused && state.active_sound.is_some();
                let effective_mic_vol = if mute_mic && mic_playing {
                    0.0
                } else {
                    vol_physical
                };
                let mic_sample = consumer.pop().unwrap_or(0.0) * effective_mic_vol;
                let mut sound_sample: f32 = 0.0;

                if !is_paused {
                    if let Some(ref mut sound) = state.active_sound {
                        if let Some(sample) = sound.consumer_mic.pop() {
                            sound_sample = sample * vol_mic;
                            state.current_sample_index += 1;
                        }
                    }
                }

                let mixed = (mic_sample + sound_sample).clamp(-1.0, 1.0);

                for _ in 0..output_channels {
                    if i < data.len() {
                        data[i] = mixed;
                        i += 1;
                    }
                }
            }
        },
        |err| eprintln!("Cable error: {:?}", err),
        None,
    )?;

    input_stream.play()?;
    output_stream.play()?;

    Ok((
        input_stream,
        output_stream,
        monitoring_stream,
        output_sample_rate,
        monitoring_rate,
    ))
}

pub fn find_virtual_cable_microphone(cable_playback_name: &str, input_devices: &[String]) -> String {
    if cable_playback_name.is_empty() {
        return String::new();
    }

    let clean_playback = cable_playback_name
        .replace("Speakers (", "")
        .replace("Headphones (", "")
        .replace("Input", "Output")
        .replace("input", "output")
        .trim_end_matches(')')
        .to_lowercase();

    for dev in input_devices {
        let dev_lower = dev.to_lowercase();
        if dev_lower.contains(&clean_playback) {
            return dev.clone();
        }
    }

    for dev in input_devices {
        let dev_lower = dev.to_lowercase();
        if dev_lower.contains("cable") || dev_lower.contains("vb-audio") {
            return dev.clone();
        }
    }

    String::new()
}