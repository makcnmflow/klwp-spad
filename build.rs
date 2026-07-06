fn main() {
    let version = std::env::var("GITHUB_REF_NAME")
        .unwrap_or_else(|_| std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "1.4.90".to_string()));
    
    println!("cargo:rustc-env=APP_VERSION={}", version);

    #[cfg(target_os = "windows")]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon("icon.ico");
        res.compile().unwrap();
    }
}