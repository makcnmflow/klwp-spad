use discord_rich_presence::{activity, DiscordIpc, DiscordIpcClient};

#[allow(dead_code)]
pub enum DiscordMsg {
    SetActivity,
    Clear,
    UpdateStatus { enabled: bool },
}

pub fn spawn_discord_rpc_thread() -> std::sync::mpsc::Sender<DiscordMsg> {
    let (tx, rx) = std::sync::mpsc::channel::<DiscordMsg>();

    std::thread::spawn(move || {
        let client_id = "1523617127267831888";

        let mut client = Some(DiscordIpcClient::new(client_id));
        let mut connected = false;
        let mut rpc_enabled = true;

        while let Ok(msg) = rx.recv() {
            match msg {
                DiscordMsg::UpdateStatus { enabled } => {
                    rpc_enabled = enabled;
                    if !rpc_enabled {
                        if connected {
                            if let Some(ref mut c) = client {
                                let _ = c.clear_activity();
                                let _ = c.close();
                            }
                            connected = false;
                        }
                    }
                }
                _ => {}
            }

            if !rpc_enabled {
                continue;
            }

            if !connected {
                if let Some(ref mut c) = client {
                    if c.connect().is_ok() {
                        connected = true;
                    }
                }
            }

            if connected {
                if let Some(ref mut c) = client {
                    match msg {
                        DiscordMsg::UpdateStatus { enabled: true } | DiscordMsg::SetActivity => {
                            let mut act = activity::Activity::new()
                                .details("Browsing sounds")
                                .state("In main menu");

                            let assets = activity::Assets::new()
                                .large_image("icon")
                                .large_text("KLWP SPAD");
                            act = act.assets(assets);
// YES ITS A FREE AD FOR THIS PROJECT AND WHAT ARE YOU GONNA DO WITH THAT HAHAHA
                            let button = activity::Button::new("Download Ts On Github!", "https://github.com/makcnmflow/klwp-spad");
                            act = act.buttons(vec![button]);

                            if c.set_activity(act).is_err() {
                                connected = false;
                            }
                        }
                        DiscordMsg::Clear | DiscordMsg::UpdateStatus { enabled: false } => {
                            let _ = c.clear_activity();
                        }
                    }
                }
            }
        }
    });

    tx
}