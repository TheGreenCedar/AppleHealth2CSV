use crate::core::{GroupedRecords, Sink};
use crate::error::{AppError, Result};
use crate::record::RecordProjection;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;
use std::time::Instant;
use tempfile::Builder;
use tokio::task;
use zip::ZipArchive;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

const STORE_THRESHOLD: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct CsvZipSinkConfig {
    pub compression_level: Option<i64>,
    pub queue_capacity: usize,
}

impl Default for CsvZipSinkConfig {
    fn default() -> Self {
        Self {
            compression_level: Some(1),
            queue_capacity: rayon::current_num_threads().saturating_mul(2).max(4),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CsvZipSink {
    config: CsvZipSinkConfig,
}

impl CsvZipSink {
    pub fn new(config: CsvZipSinkConfig) -> Self {
        Self { config }
    }

    pub fn safe_entry_name(group_key: &str) -> Result<String> {
        if group_key.is_empty() {
            return Err(AppError::InvalidZipEntryName {
                name: group_key.to_string(),
                reason: "group key is empty".to_string(),
            });
        }

        let mut encoded = String::with_capacity(group_key.len() + 4);
        for byte in group_key.bytes() {
            if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
                encoded.push(byte as char);
            } else {
                encoded.push_str(&format!("_{byte:02X}"));
            }
        }

        if encoded.is_empty() {
            return Err(AppError::InvalidZipEntryName {
                name: group_key.to_string(),
                reason: "group key produced an empty filename".to_string(),
            });
        }

        Ok(format!("{encoded}.csv"))
    }

    fn load_sync<T>(
        grouped_records: GroupedRecords<T>,
        output_path: &Path,
        config: CsvZipSinkConfig,
    ) -> Result<()>
    where
        T: RecordProjection + Send + Sync + 'static,
    {
        let start = Instant::now();
        let entries = filter_entries(grouped_records);
        let total_files = entries.len();
        let total_recs: usize = entries.iter().map(|(_, v)| v.len()).sum();
        info!(
            "Exporting {} CSVs, {} total records",
            total_files, total_recs
        );
        debug!("CSV ZIP queue capacity: {}", config.queue_capacity);

        let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
        let temp = Builder::new()
            .prefix(".gpt-os-")
            .suffix(".zip.tmp")
            .tempfile_in(parent)
            .map_err(|e| AppError::AtomicWriteError(e.to_string()))?;

        let queue_capacity = config.queue_capacity;
        let compression_level = config.compression_level;
        let (tx, rx) = sync_channel(queue_capacity);
        let merge_file = temp
            .as_file()
            .try_clone()
            .map_err(|e| AppError::AtomicWriteError(e.to_string()))?;
        let merge_handle = spawn_merger(merge_file, rx, start);

        let produce_result = entries.into_par_iter().try_for_each_with(
            tx.clone(),
            |tx, (name, records)| -> Result<()> {
                let entry_name = Self::safe_entry_name(&name)?;
                let cursor = create_mini_zip(&entry_name, &records, compression_level)?;
                debug!("Prepared '{}' from group '{}'", entry_name, name);
                tx.send((entry_name, cursor))
                    .map_err(|e| AppError::ChannelError(e.to_string()))?;
                Ok(())
            },
        );

        drop(tx);
        let merge_result = merge_handle
            .join()
            .map_err(|_| AppError::WorkerPanic("CSV ZIP merger thread panicked".to_string()))?;

        produce_result?;
        merge_result?;

        temp.persist(output_path)
            .map_err(|e| AppError::AtomicWriteError(e.to_string()))?;
        info!("Done in {:.2}s", start.elapsed().as_secs_f64());
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Sink<T> for CsvZipSink
where
    T: RecordProjection + Send + Sync + std::fmt::Debug + 'static,
{
    async fn load(&self, grouped_records: GroupedRecords<T>, output_path: &Path) -> Result<()> {
        let out = output_path.to_owned();
        let config = self.config.clone();
        task::spawn_blocking(move || Self::load_sync(grouped_records, &out, config))
            .await
            .map_err(|e| {
                if e.is_panic() {
                    AppError::WorkerPanic(e.to_string())
                } else {
                    AppError::TaskJoinError(e.to_string())
                }
            })?
    }
}

fn filter_entries<T>(grouped_records: GroupedRecords<T>) -> Vec<(String, Vec<T>)> {
    let mut entries: Vec<(String, Vec<T>)> = grouped_records
        .into_iter()
        .filter_map(|(key, records)| {
            if records.is_empty() {
                warn!("Skipping empty group '{}'", key);
                None
            } else {
                Some((key, records))
            }
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn spawn_merger(
    mut out: File,
    rx: Receiver<(String, Cursor<Vec<u8>>)>,
    start: Instant,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || -> Result<()> {
        let mut zip = ZipWriter::new(&mut out);
        for (entry_name, mut mini) in rx {
            let src = ZipArchive::new(&mut mini)?;
            zip.merge_archive(src)?;
            debug!("Merged '{}'", entry_name);
        }
        zip.finish()?;
        debug!(
            "ZIP merge completed in {:.2}s",
            start.elapsed().as_secs_f64()
        );
        Ok(())
    })
}

fn create_mini_zip<T>(
    entry_name: &str,
    records: &[T],
    compression_level: Option<i64>,
) -> Result<Cursor<Vec<u8>>>
where
    T: RecordProjection,
{
    let csv_buf = create_csv_buffer(records)?;
    let mut cursor = Cursor::new(Vec::with_capacity(csv_buf.len() / 3 + 256));
    {
        let mut mini = ZipWriter::new(&mut cursor);
        let options = file_options(csv_buf.len(), compression_level);
        mini.start_file(entry_name, options)?;
        mini.write_all(&csv_buf)?;
        mini.finish()?;
    }
    cursor.set_position(0);
    Ok(cursor)
}

fn create_csv_buffer<T>(records: &[T]) -> Result<Vec<u8>>
where
    T: RecordProjection,
{
    let mut headers: Vec<&str> = records
        .iter()
        .flat_map(RecordProjection::field_names)
        .collect();
    headers.sort_unstable();
    headers.dedup();

    let mut csv_buf = Vec::with_capacity(records.len().saturating_mul(headers.len().max(1) * 8));
    {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(true)
            .buffer_capacity(128 * 1024)
            .from_writer(&mut csv_buf);
        writer.write_record(&headers)?;
        for record in records {
            let row: Vec<&str> = headers
                .iter()
                .map(|header| record.field_value(header).unwrap_or(""))
                .collect();
            writer.write_record(&row)?;
        }
        writer.flush()?;
    }

    Ok(csv_buf)
}

fn file_options(csv_len: usize, compression_level: Option<i64>) -> FileOptions<'static, ()> {
    if csv_len < STORE_THRESHOLD {
        FileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o644)
    } else {
        FileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(compression_level)
            .unix_permissions(0o644)
    }
}
