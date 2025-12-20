use std::collections::{HashMap, VecDeque};
use crate::DBError;
use crate::record::{Action, Record};
use async_stream::try_stream;
use std::fs::{rename, File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::Stream;

const MAX_RECORD_SIZE: u32 = 100 * 1024 * 1024;
const MAX_WAL_SIZE: u32 = 10 * 1024 * 1024;

pub struct Store {
    path: PathBuf,
    active_wal_size: u32
}

pub struct RecordIter {
    file: Option<File>,
}

pub struct MultiFileRecordIter {
    pending_files: std::collections::VecDeque<PathBuf>,
    current_iter: RecordIter,
}

impl Iterator for RecordIter {
    type Item = Result<(Record, u32), DBError>;

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

        Some(Record::decode(&buf).map(|record| (record, 4 + total_len)))
    }
}

impl MultiFileRecordIter {
    fn new(files: Vec<PathBuf>) -> Self {
        let mut pending_files: VecDeque<PathBuf> = files.into();
        let current_iter = Self::open_next(&mut pending_files);
        Self { pending_files, current_iter }
    }

    fn open_next(pending: &mut VecDeque<PathBuf>) -> RecordIter {
        match pending.pop_front() {
            Some(path) => RecordIter { file: File::open(path).ok() },
            None => RecordIter { file: None },
        }
    }
}

impl Iterator for MultiFileRecordIter {
    type Item = Result<(Record, u32), DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.next() {
                Some(result) => return Some(result),
                None => {
                    // Current file exhausted, try next
                    if self.pending_files.is_empty() {
                        return None;
                    }
                    self.current_iter = Self::open_next(&mut self.pending_files);
                }
            }
        }
    }
}

impl Store {
    pub fn new<P:AsRef<Path>>(path: P) -> Result<Self, DBError> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(DBError::InvalidPath)
        }
        Ok(Self { path:path.to_path_buf(), active_wal_size:0 })
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>{
        let record = Record::new(Action::Set, key, value);
        self.append_to_file(&record.encode())?;
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        let record = Record::new(Action::Delete, key, Vec::new());
        self.append_to_file(&record.encode())?;
        Ok(())
    }

    /// Iterate over all records: segments (oldest first) then active WAL
    pub fn iter_all(&self) -> MultiFileRecordIter {
        let mut files = self.list_segments();
        files.push(self.wal_path());
        MultiFileRecordIter::new(files)
    }



    pub fn write_all(&self,data:&HashMap<Vec<u8>, Vec<u8>>) -> Result<(), DBError>{
        let file = OpenOptions::new()
            .create(true)
            .truncate(true) // truncate file if it already exists
            .open(format!("{}.new", &self.path.display()))?;

        let mut writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer
        for (key, value) in data{
            let record = Record::new(Action::Set, key.clone(), value.clone());
            writer.write_all(&record.encode())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        rename(format!("{}.new", &self.path.display()), &self.path)?;
        Ok(())
    }

    

    pub fn set_wal_size(&mut self, size: u32) {
        self.active_wal_size = size;
    }

    fn increment_wal_size(&mut self, amount: u32) {
        self.active_wal_size += amount;
    }

    pub fn wal_path(&self) -> PathBuf {
        self.path.join("wal.log")
    }

    fn segment_path(&self, ts: u64) -> PathBuf {
        self.path.join(format!("segment_{}.log", ts))
    }

    fn list_segments(&self) -> Vec<PathBuf> {
        let mut segments = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                    && filename.starts_with("segment_")
                    && filename.ends_with(".log")
                {
                    segments.push(path);
                }
            }
        }
        segments.sort();
        segments
    }

    fn rotate_wal(&mut self) -> Result<(), DBError>{
        if !self.wal_path().exists(){
            return Ok(())
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e|DBError::Other(Box::new(e)))?.as_millis() as u64;
        let segment = self.segment_path(now);
        rename(self.wal_path(), segment)?;
        self.active_wal_size=0;
        Ok(())
    }

    fn append_to_file(&mut self, data: &[u8]) ->  Result<(), DBError>{
        if self.active_wal_size + data.len() as u32 >= MAX_WAL_SIZE{
            self.rotate_wal()?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.wal_path())?;
        file.write_all(data)?;
        self.increment_wal_size(data.len() as u32);
        Ok(())
    }

    // TODO: will be used for replication
    #[allow(dead_code)]
    fn stream_all(&self) -> impl Stream<Item = Result<Record, DBError>> {
        let iter = self.iter_all();
        try_stream! {
            for item in iter {
                let (record, _) = item?;
                yield record
            }
        }
    }
}
