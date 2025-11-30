use serde::{Deserialize, Serialize};
use crate::DBError;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum Action {
    Set,
    Delete,
}

#[derive(Serialize, Deserialize)]
pub struct Record {
    pub action: Action,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(action: Action, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { action, key, value }
    }

    /// Encode record to bytes with length prefix for file storage
    /// Format: <total_len: u32><bincode-encoded record>
    pub fn encode(&self) -> Vec<u8> {
        let body = bincode::serde::encode_to_vec(self, bincode::config::standard()).unwrap();
        let len = body.len() as u32;

        let mut result = Vec::with_capacity(4 + body.len());
        result.extend_from_slice(&len.to_le_bytes());
        result.extend(body);
        result
    }

    /// Decode record from bytes (without the length prefix)
    pub fn decode(buf: &[u8]) -> Result<Self, DBError> {
        let (record, _) = bincode::serde::decode_from_slice(buf, bincode::config::standard())?;
        Ok(record)
    }
}
