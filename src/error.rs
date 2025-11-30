use bincode::error::{DecodeError, EncodeError};
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("encoding error: {0}")]
    Encode(#[from] EncodeError),
    #[error("decoding error: {0}")]
    Decode(#[from] DecodeError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupted file: {0}")]
    CorruptedFile(String),
}

