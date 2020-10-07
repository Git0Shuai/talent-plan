/// kvs error type
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// io error
    #[error("io error")]
    Io(#[from] std::io::Error),
    /// serialize error
    #[error("serialize error")]
    Serde(#[from] serde_json::Error),
    /// key not found error
    #[error("key not found. key={}", .0)]
    KeyNotFound(String),
    /// key is empty string
    #[error("invalid key. key={}", .0)]
    InvalidKey(String),
    /// failed parse json string length from &[u8]
    #[error("failed parsing db file.")]
    ParseLengthError(#[from] std::array::TryFromSliceError),
}

/// kvs result Type
pub type Result<T> = std::result::Result<T, Error>;
