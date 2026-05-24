use crate::apple_health::types::GenericRecord;
use crate::core::{ErrorPolicy, ExtractEvent, Extractor};
use crate::error::{AppError, Result};
use crate::xml_utils::{self, BUFFER_SIZE, XmlStreamOptions};
use async_trait::async_trait;
use crossbeam_channel as channel;
use quick_xml::events::BytesStart;
use std::fs::File;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task;

#[derive(Debug, Clone, Copy)]
pub struct AppleHealthExtractor {
    error_policy: ErrorPolicy,
}

impl Default for AppleHealthExtractor {
    fn default() -> Self {
        Self {
            error_policy: ErrorPolicy::Strict,
        }
    }
}

impl AppleHealthExtractor {
    pub fn new(error_policy: ErrorPolicy) -> Self {
        Self { error_policy }
    }

    fn parse_generic(event: &BytesStart) -> Result<Option<GenericRecord>> {
        GenericRecord::from_xml(event).map(Some)
    }

    fn select_export_xml(input_path: &Path) -> Result<String> {
        let file = File::open(input_path)?;
        let archive = zip::ZipArchive::new(file)?;
        archive
            .file_names()
            .find(|name| {
                name.rsplit(['/', '\\'])
                    .next()
                    .is_some_and(|file_name| file_name == "export.xml")
            })
            .map(str::to_string)
            .ok_or_else(|| {
                AppError::ParseError("Could not find export.xml in the zip archive".to_string())
            })
    }

    fn xml_options(&self) -> XmlStreamOptions {
        XmlStreamOptions {
            skipped_root: Some(b"HealthData"),
            error_policy: self.error_policy,
        }
    }
}

#[async_trait]
impl Extractor<GenericRecord> for AppleHealthExtractor {
    async fn extract(
        &self,
        input_path: &Path,
    ) -> Result<mpsc::Receiver<Result<ExtractEvent<GenericRecord>>>> {
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        let (sync_tx, sync_rx) = channel::bounded(BUFFER_SIZE);
        let path = input_path.to_path_buf();
        let options = self.xml_options();

        let parse_handle = tokio::spawn(async move {
            if path
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
            {
                process_zip(path, sync_tx, options).await
            } else {
                process_xml(path, sync_tx, options).await
            }
        });

        let bridge_tx = tx.clone();
        task::spawn_blocking(move || {
            for event in sync_rx {
                if bridge_tx.blocking_send(event).is_err() {
                    break;
                }
            }
        });

        let error_tx = tx.clone();
        tokio::spawn(async move {
            match parse_handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let _ = error_tx.send(Err(error)).await;
                }
                Err(error) => {
                    let app_error = if error.is_panic() {
                        AppError::WorkerPanic(error.to_string())
                    } else {
                        AppError::TaskJoinError(error.to_string())
                    };
                    let _ = error_tx.send(Err(app_error)).await;
                }
            }
        });

        Ok(rx)
    }
}

async fn process_xml(
    path: PathBuf,
    tx: channel::Sender<Result<ExtractEvent<GenericRecord>>>,
    options: XmlStreamOptions,
) -> Result<()> {
    let file = File::open(path)?;
    xml_utils::process_stream_parallel(file, tx, AppleHealthExtractor::parse_generic, options).await
}

async fn process_zip(
    path: PathBuf,
    tx: channel::Sender<Result<ExtractEvent<GenericRecord>>>,
    options: XmlStreamOptions,
) -> Result<()> {
    let member_name = AppleHealthExtractor::select_export_xml(&path)?;
    xml_utils::process_zip_member_parallel(
        path,
        member_name,
        tx,
        AppleHealthExtractor::parse_generic,
        options,
    )
    .await
}
