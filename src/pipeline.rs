use crate::apple_health::extractor::AppleHealthExtractor;
use crate::apple_health::policy::AppleHealthTransformPolicy;
use crate::core::{Engine, ErrorPolicy, RunReport, RuntimeOptions};
use crate::error::{AppError, Result};
use crate::sinks::csv_zip::CsvZipSink;
use std::path::Path;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SourceKind {
    AppleHealth,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SinkKind {
    CsvZip,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PipelineSelection {
    pub source: SourceKind,
    pub sink: SinkKind,
}

impl PipelineSelection {
    pub fn from_names(source: &str, sink: &str) -> Result<Self> {
        Ok(Self {
            source: match source {
                "apple-health" => SourceKind::AppleHealth,
                other => {
                    return Err(AppError::ConfigError(format!(
                        "unknown source '{other}'; expected 'apple-health'"
                    )));
                }
            },
            sink: match sink {
                "csv-zip" => SinkKind::CsvZip,
                other => {
                    return Err(AppError::ConfigError(format!(
                        "unknown sink '{other}'; expected 'csv-zip'"
                    )));
                }
            },
        })
    }
}

impl Default for PipelineSelection {
    fn default() -> Self {
        Self {
            source: SourceKind::AppleHealth,
            sink: SinkKind::CsvZip,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineSpec {
    pub selection: PipelineSelection,
    pub runtime_options: RuntimeOptions,
}

impl PipelineSpec {
    pub async fn run(&self, input_path: &Path, output_path: &Path) -> Result<RunReport> {
        match (&self.selection.source, &self.selection.sink) {
            (SourceKind::AppleHealth, SinkKind::CsvZip) => {
                let extractor = AppleHealthExtractor::new(self.runtime_options.error_policy);
                let policy = AppleHealthTransformPolicy;
                let sink = CsvZipSink::default();
                let engine = Engine::new(extractor, policy, sink);
                engine.run(input_path, output_path).await
            }
        }
    }
}

pub struct PipelineRegistry;

impl PipelineRegistry {
    pub fn default_pipeline() -> Result<PipelineSpec> {
        Self::resolve(PipelineSelection::default(), RuntimeOptions::default())
    }

    pub fn resolve(
        selection: PipelineSelection,
        runtime_options: RuntimeOptions,
    ) -> Result<PipelineSpec> {
        match (&selection.source, &selection.sink) {
            (SourceKind::AppleHealth, SinkKind::CsvZip) => Ok(PipelineSpec {
                selection,
                runtime_options,
            }),
        }
    }

    pub fn resolve_names(
        source: &str,
        sink: &str,
        error_policy: ErrorPolicy,
    ) -> Result<PipelineSpec> {
        Self::resolve(
            PipelineSelection::from_names(source, sink)?,
            RuntimeOptions { error_policy },
        )
    }
}
