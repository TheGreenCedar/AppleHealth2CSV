use crate::apple_health::extractor::AppleHealthExtractor;
use crate::apple_health::types::GenericRecord;
use crate::config::Config;
use crate::core::Engine;
use crate::error::Result;
use crate::sinks::csv_zip::CsvZipSink;
use log::{LevelFilter, info};
use std::path::Path;
use std::time::{Duration, Instant};

/// Top-level application orchestrator.
///
/// This struct wires together the Apple Health extractor, CSV ZIP sink, and the
/// core ETL engine. It is responsible for bootstrapping logging, running the
/// pipeline, and emitting user-facing metrics.
pub struct App {
    config: Config,
    engine: Engine<GenericRecord, AppleHealthExtractor, CsvZipSink>,
    metrics: MetricsReporter,
}

impl App {
    /// Build a new application instance using the provided configuration.
    pub fn new(config: Config) -> Self {
        let metrics = MetricsReporter::new(!config.no_metrics);
        Self {
            config,
            engine: Engine::new(AppleHealthExtractor, CsvZipSink),
            metrics,
        }
    }

    /// Execute the ETL pipeline end-to-end.
    pub async fn run(self) -> Result<()> {
        init_logging(self.config.verbose);

        let summary = self.execute_pipeline().await?;
        self.metrics.print(&self.config, &summary);

        Ok(())
    }

    async fn execute_pipeline(&self) -> Result<RunSummary> {
        let start_time = Instant::now();
        info!("🚀 Starting Apple Health Transformer");
        info!("📁 Input: {}", self.config.input_file);
        info!("📦 Output: {}", self.config.output_zip);

        let input_path = Path::new(&self.config.input_file);
        let output_path = Path::new(&self.config.output_zip);

        self.engine.run(input_path, output_path).await?;

        let total_duration = start_time.elapsed();
        info!(
            "✅ Transformation completed successfully in {:.2}s!",
            total_duration.as_secs_f64()
        );

        Ok(RunSummary { total_duration })
    }
}

/// Simple execution summary used for user-facing metrics.
struct RunSummary {
    total_duration: Duration,
}

struct MetricsReporter {
    enabled: bool,
}

impl MetricsReporter {
    fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    fn print(&self, config: &Config, summary: &RunSummary) {
        if !self.enabled {
            return;
        }

        println!("\n🎉 Apple Health transformation completed!");
        println!(
            "📊 Total execution time: {:.2} seconds",
            summary.total_duration.as_secs_f64()
        );
        println!("📁 Output saved to: {}", config.output_zip);
    }
}

fn init_logging(verbose: bool) {
    env_logger::Builder::from_default_env()
        .filter_level(if verbose {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        })
        .init();
}
