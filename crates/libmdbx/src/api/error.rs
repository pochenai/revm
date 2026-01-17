//!
use thiserror::Error;

// TODO improve errors
#[derive(Debug, Error)]
///
pub enum DatabaseError {
    #[error("{0}")]
    ///
    Custom(String),
    #[error("failed to decode a key from a table")]
    ///
    Decode,
}
