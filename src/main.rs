#![windows_subsystem = "windows"]

mod config;
mod audio;
mod utils;
mod discord;
mod gui;

use eframe::egui;
use egui_phosphor::{add_to_fonts, Variant};
use gui::{DownloadResult, SoundpadApp};
use std::sync::{Arc, Mutex};
use utils::register_custom_protocol;

fn load_icon() -> eframe::egui::IconData {
    let icon_bytes = include_bytes!("../icon.ico");
    let image = image::load_from_memory(icon_bytes)
        .expect("Failed to decode embedded ico icon")
        .into_rgba8();
    let (width, height) = image.dimensions();
    let rgba = image.into_raw();
    eframe::egui::IconData {
        rgba,
        width,
        height,
    }
}

fn main() -> eframe::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut passed_url = None;
    if args.len() > 1 {
        let arg = &args[1];
        if arg.starts_with("soundpad://") || arg.starts_with("voicemod:") {
            passed_url = Some(arg.clone());
        }
    }

    let is_main_instance = std::net::TcpListener::bind("127.0.0.1:48291").is_ok();

    if !is_main_instance {
        #[cfg(windows)]
        unsafe {
            extern "system" {
                fn FreeConsole() -> i32;
            }
            FreeConsole();
        }
    }

    if is_main_instance {
        let listener = std::net::TcpListener::bind("127.0.0.1:48291").unwrap();
        let _ = register_custom_protocol();

        let url_queue = Arc::new(Mutex::new(Vec::<String>::new()));
        let url_queue_clone = Arc::clone(&url_queue);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(mut stream) = stream {
                    use std::io::Read;
                    let mut buffer = [0; 2048];
                    if let Ok(bytes_read) = stream.read(&mut buffer) {
                        let received = String::from_utf8_lossy(&buffer[..bytes_read])
                            .trim()
                            .to_string();
                        if received.starts_with("soundpad://") || received.starts_with("voicemod:") {
                            let mut queue = url_queue_clone.lock().unwrap();
                            queue.push(received);
                        }
                    }
                }
            }
        });

        if let Some(url) = passed_url {
            url_queue.lock().unwrap().push(url);
        }

        let (tx, rx) = std::sync::mpsc::channel::<DownloadResult>();

        let options = eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default()
                .with_icon(load_icon()),
            ..Default::default()
        };

        eframe::run_native(
            "klwp spad",
            options,
            Box::new(move |cc| {
                let mut fonts = egui::FontDefinitions::default();
                add_to_fonts(&mut fonts, Variant::Regular);
                cc.egui_ctx.set_fonts(fonts);

                Box::new(SoundpadApp::new_with_ipc(url_queue, rx, tx))
            }),
        )
    } else {
        if let Some(url) = passed_url {
            use std::io::Write;
            if let Ok(mut stream) = std::net::TcpStream::connect("127.0.0.1:48291") {
                let _ = stream.write_all(url.as_bytes());
                let _ = stream.flush();
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }
        Ok(())
    }
}
