use crate::error::Result;
use crate::transform::TransformPolicy;
use ahash::AHashMap;
use async_trait::async_trait;
use log::{debug, info};
use std::fmt::Debug;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub type GroupedRecords<T> = AHashMap<String, Vec<T>>;

/// Represents a single record that can move through the ETL framework.
pub trait Processable: Send + Sync + Debug + 'static {}

impl<T> Processable for T where T: Send + Sync + Debug + 'static {}

#[derive(Debug)]
pub enum ExtractEvent<T> {
    Record(T),
    Skipped { reason: crate::error::AppError },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ErrorPolicy {
    Strict,
    Tolerant,
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        Self::Strict
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeOptions {
    pub error_policy: ErrorPolicy,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            error_policy: ErrorPolicy::Strict,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunReport {
    pub total_records: usize,
    pub skipped_records: usize,
    pub record_types: usize,
    pub extract_duration: Duration,
    pub transform_duration: Duration,
    pub load_duration: Duration,
    pub total_duration: Duration,
}

/// Extracts records from a data source into a channel.
#[async_trait]
pub trait Extractor<T: Processable> {
    async fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<Result<ExtractEvent<T>>>>;
}

/// Loads grouped records into a data sink.
#[async_trait]
pub trait Sink<T: Processable> {
    async fn load(&self, grouped_records: GroupedRecords<T>, output_path: &Path) -> Result<()>;
}

pub struct MaterializedEngine<T, E, P, S>
where
    T: Processable,
    E: Extractor<T> + Sync,
    P: TransformPolicy<T> + Sync,
    S: Sink<T> + Sync,
{
    extractor: E,
    policy: P,
    sink: S,
    _marker: std::marker::PhantomData<T>,
}

pub type Engine<T, E, P, S> = MaterializedEngine<T, E, P, S>;

impl<T, E, P, S> MaterializedEngine<T, E, P, S>
where
    T: Processable,
    E: Extractor<T> + Sync,
    P: TransformPolicy<T> + Sync,
    S: Sink<T> + Sync,
{
    pub fn new(extractor: E, policy: P, sink: S) -> Self {
        Self {
            extractor,
            policy,
            sink,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn run(&self, input_path: &Path, output_path: &Path) -> Result<RunReport> {
        let start_time = Instant::now();
        info!("Starting ETL pipeline");
        info!("Input: {}", input_path.display());
        info!("Output: {}", output_path.display());

        let extract_start = Instant::now();
        info!("Starting extraction phase...");
        let receiver = self.extractor.extract(input_path).await?;
        let extract_duration = extract_start.elapsed();
        debug!(
            "Extraction phase setup completed in {:.3}s",
            extract_duration.as_secs_f64()
        );

        let transform_start = Instant::now();
        info!("Starting transformation phase...");
        let transform_result = transformer::transform(receiver, &self.policy).await?;
        let transform_duration = transform_start.elapsed();

        let total_records: usize = transform_result
            .grouped_records
            .values()
            .map(Vec::len)
            .sum();
        let record_types = transform_result.grouped_records.len();
        info!(
            "Transformation completed in {:.3}s: {} records grouped into {} types, {} skipped",
            transform_duration.as_secs_f64(),
            total_records,
            record_types,
            transform_result.skipped_records
        );

        let load_start = Instant::now();
        info!("Starting load phase...");
        self.sink
            .load(transform_result.grouped_records, output_path)
            .await?;
        let load_duration = load_start.elapsed();
        info!(
            "Load phase completed in {:.3}s",
            load_duration.as_secs_f64()
        );

        let total_duration = start_time.elapsed();
        info!(
            "ETL pipeline completed successfully in {:.3}s",
            total_duration.as_secs_f64()
        );
        info!(
            "Performance breakdown - Extract setup: {:.3}s, Transform: {:.3}s, Load: {:.3}s",
            extract_duration.as_secs_f64(),
            transform_duration.as_secs_f64(),
            load_duration.as_secs_f64()
        );

        if total_records > 0 {
            let throughput = total_records as f64 / total_duration.as_secs_f64();
            info!("Throughput: {:.0} records/second", throughput);
        }

        Ok(RunReport {
            total_records,
            skipped_records: transform_result.skipped_records,
            record_types,
            extract_duration,
            transform_duration,
            load_duration,
            total_duration,
        })
    }
}

mod transformer {
    use super::{ExtractEvent, GroupedRecords, Processable};
    use crate::error::Result;
    use crate::transform::TransformPolicy;
    use log::{debug, info, warn};
    use rayon::prelude::*;
    use std::time::Instant;
    use tokio::sync::mpsc::Receiver;

    pub struct TransformResult<T> {
        pub grouped_records: GroupedRecords<T>,
        pub skipped_records: usize,
    }

    pub async fn transform<T, P>(
        mut receiver: Receiver<Result<ExtractEvent<T>>>,
        policy: &P,
    ) -> Result<TransformResult<T>>
    where
        T: Processable,
        P: TransformPolicy<T> + Sync,
    {
        let start_time = Instant::now();
        let mut grouped_records: GroupedRecords<T> = GroupedRecords::new();
        let mut total_processed = 0usize;
        let mut skipped_records = 0usize;

        while let Some(result) = receiver.recv().await {
            match result? {
                ExtractEvent::Record(record) => {
                    if policy.include(&record) {
                        grouped_records
                            .entry(policy.group_key(&record))
                            .or_default()
                            .push(record);
                        total_processed += 1;
                    }
                }
                ExtractEvent::Skipped { reason } => {
                    skipped_records += 1;
                    warn!("Skipped record: {}", reason);
                }
            }
        }

        grouped_records
            .values_mut()
            .par_bridge()
            .for_each(|records| {
                records.sort_by_cached_key(|record| policy.sort_key(record).map(str::to_owned));
            });

        let duration = start_time.elapsed();
        info!(
            "Transformation completed: {} records processed, {} skipped in {:.3}s",
            total_processed,
            skipped_records,
            duration.as_secs_f64()
        );

        if total_processed > 0 {
            let records_per_sec = total_processed as f64 / duration.as_secs_f64();
            debug!(
                "Transformation throughput: {:.0} records/second",
                records_per_sec
            );
        }

        Ok(TransformResult {
            grouped_records,
            skipped_records,
        })
    }
}
