use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("ZIP error: {0}")]
    ZipArchiveError(#[from] zip::result::ZipError),

    #[error("Thread pool build error: {0}")]
    ThreadPoolError(#[from] rayon::ThreadPoolBuildError),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Task join error: {0}")]
    TaskJoinError(String),

    #[error("Worker panic: {0}")]
    WorkerPanic(String),

    #[error("Invalid ZIP entry name '{name}': {reason}")]
    InvalidZipEntryName { name: String, reason: String },

    #[error("Atomic output write error: {0}")]
    AtomicWriteError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<Box<dyn std::error::Error>> for AppError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        AppError::Unknown(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
