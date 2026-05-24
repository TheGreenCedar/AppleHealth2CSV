use clap::Parser;
use gpt_os::config::Config;
use gpt_os::core::ErrorPolicy;
use gpt_os::pipeline::PipelineRegistry;
use log::{LevelFilter, error, info};
use std::path::Path;
use std::process;

#[tokio::main]
async fn main() {
    let start_time = std::time::Instant::now();
    let config = Config::parse();

    env_logger::Builder::from_default_env()
        .filter_level(if config.verbose {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        })
        .init();

    info!("Starting gpt-os ETL pipeline");
    info!("Input: {}", config.input_file);
    info!("Output: {}", config.output_zip);
    info!("Source: {}", config.source);
    info!("Sink: {}", config.sink);

    let error_policy = if config.tolerant {
        ErrorPolicy::Tolerant
    } else {
        ErrorPolicy::Strict
    };

    let pipeline = match PipelineRegistry::resolve_names(&config.source, &config.sink, error_policy)
    {
        Ok(pipeline) => pipeline,
        Err(e) => {
            error!("Application error: {}", e);
            process::exit(1);
        }
    };

    let input_path = Path::new(&config.input_file);
    let output_path = Path::new(&config.output_zip);

    let report = match pipeline.run(input_path, output_path).await {
        Ok(report) => report,
        Err(e) => {
            error!("Application error: {}", e);
            process::exit(1);
        }
    };

    let total_time = start_time.elapsed();
    info!(
        "Transformation completed successfully in {:.2}s",
        total_time.as_secs_f64()
    );

    if !config.no_metrics {
        println!("\nApple Health transformation completed!");
        println!(
            "Total execution time: {:.2} seconds",
            total_time.as_secs_f64()
        );
        println!("Records written: {}", report.total_records);
        println!("Record groups: {}", report.record_types);
        println!("Records skipped: {}", report.skipped_records);
        println!("Output saved to: {}", config.output_zip);
    }
}
