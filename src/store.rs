use crate::DBError;
use crate::record::{Action, Record};
use async_stream::try_stream;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use tokio_stream::Stream;

const MAX_RECORD_SIZE: u32 = 100 * 1024 * 1024;

pub struct Store {
    path: String,
}

pub struct RecordIter {
    file: Option<File>,
}

impl Iterator for RecordIter {
    type Item = Result<Record, DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        let file = self.file.as_mut()?;

        // Read total_len
        let mut total_len_bytes = [0u8; 4];
        match file.read_exact(&mut total_len_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return None, // EOF
            Err(e) => return Some(Err(e.into())),
        }
        let total_len = u32::from_le_bytes(total_len_bytes);

        // Sanity check
        if total_len > MAX_RECORD_SIZE {
            return Some(Err(DBError::CorruptedFile(format!(
                "record size {} exceeds max {}",
                total_len, MAX_RECORD_SIZE
            ))));
        }

        // Read record body
        let mut buf = vec![0u8; total_len as usize];
        match file.read_exact(&mut buf) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Some(Err(DBError::CorruptedFile("truncated record".to_string())));
            }
            Err(e) => return Some(Err(e.into())),
        }

        Some(Record::decode(&buf))
    }
}

impl Store {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>{
        let record = Record::new(Action::Set, key, value);
        self.append_to_file(&record.encode())?;
        Ok(())
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), DBError> {
        let record = Record::new(Action::Delete, key, Vec::new());
        self.append_to_file(&record.encode())?;
        Ok(())
    }

    pub fn iter_from_file(&self) -> RecordIter {
        let file = File::open(&self.path).ok();
        RecordIter { file }
    }

    fn append_to_file(&self, data: &[u8]) ->  Result<(), DBError>{
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(data)?;
        Ok(())
    }

    // TODO: will be used for replication
    #[allow(dead_code)]
    fn stream_from_file(&self) -> impl Stream<Item = Result<Record, DBError>> {
        let iter = self.iter_from_file();
        try_stream! {
            for record in iter {
                yield record?
            }
        }
    }
}
