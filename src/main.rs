use clap::Parser;
use gpt_os::{app::App, config::Config};
use log::error;
use std::process;

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let app = App::new(config);

    if let Err(e) = app.run().await {
        error!("❌ Application error: {}", e);
        process::exit(1);
    }
}
