use crossbeam_channel as channel;
use quick_xml::events::{BytesStart, Event};
use rayon::ThreadPool;
use std::path::PathBuf;
use std::sync::{OnceLock, mpsc as sync_mpsc};
use tokio::task;

use crate::core::{ErrorPolicy, ExtractEvent};
use crate::error::{AppError, Result};

pub const BUFFER_SIZE: usize = 1024 * 128;
const BATCH_SIZE: usize = 500;

pub type ParseFn<T> = fn(&BytesStart) -> Result<Option<T>>;

#[derive(Debug, Clone, Copy)]
pub struct XmlStreamOptions {
    pub skipped_root: Option<&'static [u8]>,
    pub error_policy: ErrorPolicy,
}

impl Default for XmlStreamOptions {
    fn default() -> Self {
        Self {
            skipped_root: None,
            error_policy: ErrorPolicy::Strict,
        }
    }
}

static THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();

pub fn get_thread_pool() -> Result<&'static ThreadPool> {
    if let Some(pool) = THREAD_POOL.get() {
        return Ok(pool);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .build()
        .map_err(AppError::ThreadPoolError)?;

    let _ = THREAD_POOL.set(pool);
    THREAD_POOL
        .get()
        .ok_or_else(|| AppError::Unknown("thread pool was not initialized".to_string()))
}

fn send_event<T>(
    sender: &channel::Sender<Result<ExtractEvent<T>>>,
    event: Result<ExtractEvent<T>>,
) -> Result<()>
where
    T: Send + 'static,
{
    sender
        .send(event)
        .map_err(|e| AppError::ChannelError(e.to_string()))
}

fn process_batch<T>(
    batch: Vec<BytesStart<'static>>,
    sender: &channel::Sender<Result<ExtractEvent<T>>>,
    parse_fn: ParseFn<T>,
    options: XmlStreamOptions,
) -> Result<()>
where
    T: Send + 'static,
{
    for event in &batch {
        let result = parse_fn(event);
        match result {
            Ok(Some(record)) => send_event(sender, Ok(ExtractEvent::Record(record)))?,
            Ok(None) => {}
            Err(error) => match options.error_policy {
                ErrorPolicy::Strict => return Err(error),
                ErrorPolicy::Tolerant => {
                    send_event(sender, Ok(ExtractEvent::Skipped { reason: error }))?;
                }
            },
        }
    }

    Ok(())
}

fn spawn_batch<T>(
    batch: Vec<BytesStart<'static>>,
    sender: channel::Sender<Result<ExtractEvent<T>>>,
    parse_fn: ParseFn<T>,
    options: XmlStreamOptions,
    completion_tx: sync_mpsc::Sender<Result<()>>,
    pool: &ThreadPool,
) where
    T: Send + 'static,
{
    pool.spawn(move || {
        let result = process_batch(batch, &sender, parse_fn, options);
        let _ = completion_tx.send(result);
    });
}

fn wait_for_batches(completion_rx: sync_mpsc::Receiver<Result<()>>, batches: usize) -> Result<()> {
    let mut first_error = None;

    for _ in 0..batches {
        match completion_rx
            .recv()
            .map_err(|e| AppError::ChannelError(e.to_string()))?
        {
            Ok(()) => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

fn process_xml_reader<T, R>(
    reader: R,
    sender: channel::Sender<Result<ExtractEvent<T>>>,
    parse_fn: ParseFn<T>,
    options: XmlStreamOptions,
    pool: &ThreadPool,
) -> Result<()>
where
    T: Send + 'static,
    R: std::io::Read,
{
    let buf_reader = std::io::BufReader::with_capacity(BUFFER_SIZE, reader);
    let mut xml_reader = quick_xml::reader::Reader::from_reader(buf_reader);
    xml_reader.config_mut().trim_text(true);
    let mut buf = Vec::with_capacity(BUFFER_SIZE);
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let (completion_tx, completion_rx) = sync_mpsc::channel();
    let mut spawned_batches = 0usize;
    let mut reader_error = None;

    loop {
        match xml_reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref event)) | Ok(Event::Empty(ref event)) => {
                if options
                    .skipped_root
                    .is_some_and(|root| event.name().as_ref() == root)
                {
                    buf.clear();
                    continue;
                }

                batch.push(event.to_owned());
                if batch.len() >= BATCH_SIZE {
                    let current_batch = std::mem::take(&mut batch);
                    spawn_batch(
                        current_batch,
                        sender.clone(),
                        parse_fn,
                        options,
                        completion_tx.clone(),
                        pool,
                    );
                    spawned_batches += 1;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                reader_error = Some(AppError::ParseError(e.to_string()));
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    if !batch.is_empty() {
        spawn_batch(
            batch,
            sender.clone(),
            parse_fn,
            options,
            completion_tx.clone(),
            pool,
        );
        spawned_batches += 1;
    }

    drop(completion_tx);
    let batch_result = wait_for_batches(completion_rx, spawned_batches);

    if let Some(error) = reader_error {
        return Err(error);
    }

    batch_result
}

pub async fn process_stream_parallel<T, R>(
    reader: R,
    sender: channel::Sender<Result<ExtractEvent<T>>>,
    parse_fn: ParseFn<T>,
    options: XmlStreamOptions,
) -> Result<()>
where
    T: Send + 'static,
    R: std::io::Read + Send + 'static,
{
    let pool = get_thread_pool()?;
    task::spawn_blocking(move || process_xml_reader(reader, sender, parse_fn, options, pool))
        .await
        .map_err(|e| {
            if e.is_panic() {
                AppError::WorkerPanic(e.to_string())
            } else {
                AppError::TaskJoinError(e.to_string())
            }
        })?
}

pub async fn process_zip_member_parallel<T>(
    input_path: PathBuf,
    member_name: String,
    sender: channel::Sender<Result<ExtractEvent<T>>>,
    parse_fn: ParseFn<T>,
    options: XmlStreamOptions,
) -> Result<()>
where
    T: Send + 'static,
{
    let pool = get_thread_pool()?;
    task::spawn_blocking(move || -> Result<()> {
        let file = std::fs::File::open(input_path)?;
        let mut archive = zip::ZipArchive::new(file)?;
        let member = archive.by_name(&member_name)?;
        process_xml_reader(member, sender, parse_fn, options, pool)
    })
    .await
    .map_err(|e| {
        if e.is_panic() {
            AppError::WorkerPanic(e.to_string())
        } else {
            AppError::TaskJoinError(e.to_string())
        }
    })?
}
