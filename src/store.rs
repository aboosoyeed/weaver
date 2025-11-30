use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use async_stream::try_stream;
use tokio_stream::Stream;
use crate::record::{Action, Record};
use crate::DBError;

pub struct Store {
    path: String,
}

impl Store {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let record = Record::new(Action::Set, key, value);
        self.append_to_file(&record.encode());
    }

    pub fn delete(&self, key: Vec<u8>) {
        let record = Record::new(Action::Delete, key, Vec::new());
        self.append_to_file(&record.encode());
    }

    fn append_to_file(&self, data: &[u8]) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .unwrap();
        file.write_all(data).unwrap();
    }

    fn stream_from_file(&self) -> impl Stream<Item = Result<Record, DBError>> {
        try_stream! {
            let mut file = match File::open(&self.path) {
                Ok(f) => f,
                Err(e) if e.kind() == ErrorKind::NotFound => return,
                Err(e) => Err(e)?,
            };

            loop {
                // Read total_len
                let mut total_len_bytes = [0u8; 4];
                match file.read_exact(&mut total_len_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break, // EOF
                    Err(e) => Err(e)?,
                }
                let total_len = u32::from_le_bytes(total_len_bytes);

                // Sanity check: reject unreasonably large records (e.g., > 100MB)
                const MAX_RECORD_SIZE: u32 = 100 * 1024 * 1024;
                if total_len > MAX_RECORD_SIZE {
                    Err(DBError::CorruptedFile(format!(
                        "record size {} exceeds max {}",
                        total_len, MAX_RECORD_SIZE
                    )))?;
                }

                // Read record body
                let mut buf = vec![0u8; total_len as usize];
                match file.read_exact(&mut buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        Err(DBError::CorruptedFile("truncated record".to_string()))?;
                    }
                    Err(e) => Err(e)?,
                }

                yield Record::decode(&buf)?
            }
        }
    }
}