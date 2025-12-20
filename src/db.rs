use crate::entry::Entry;
use crate::error::DBError;
use crate::record::Action;
use crate::store::Store;
use ::serde::{Deserialize, Serialize};
use bincode::{config, serde};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::compaction::compact;

pub struct DB {
    data: HashMap<Vec<u8>, Vec<u8>>,
    pub store: Store,
}

impl DB {
    fn new(path: &str) -> Result<Self, DBError> {
        let mut store = Store::new(path)?;
        let mut data = HashMap::new();
        // Replay all segments + active WAL to rebuild state
        for record in store.iter_all() {
            let (record, _) = record?;
            match record.action {
                Action::Set => data.insert(record.key, record.value),
                Action::Delete => data.remove(&record.key),
            };
        }
        // Set WAL size from filesystem (only track active WAL, not segments)
        let wal_size = std::fs::metadata(store.wal_path())
            .map(|m| m.len() as u32)
            .unwrap_or(0);
        store.set_wal_size(wal_size);
        Ok(Self { store, data })
    }

    pub async fn start(path: &str) -> Result<Arc<Mutex<Self>>, DBError>{
        let db = Arc::new(Mutex::new(Self::new(path)?));
        tokio::spawn(compact(db.clone(), Duration::from_secs(10)));
        Ok(db)
    }

    pub fn set<K: Serialize, V: Serialize>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<u64>,
    ) -> Result<(), DBError> {
        let config = config::standard();
        let encoded_key = serde::encode_to_vec(&key, config)?;
        let entry = Entry::new(value, ttl);
        let encoded_value = serde::encode_to_vec(&entry, config)?;
        self.store.set(encoded_key.clone(), encoded_value.clone())?;
        self.data.insert(encoded_key, encoded_value);
        Ok(())
    }

    pub fn delete<K: Serialize>(&mut self, key: K) -> Result<(), DBError> {
        let config = config::standard();
        let encoded_key = serde::encode_to_vec(&key, config)?;
        self.store.delete(encoded_key.clone())?;
        self.data.remove(&encoded_key);
        Ok(())
    }

    pub fn get<K: Serialize, V: for<'de> Deserialize<'de>>(
        &self,
        key: K,
    ) -> Result<Option<V>, DBError> {
        let config = config::standard();
        let encoded_key = serde::encode_to_vec(&key, config)?;
        match self.data.get(&encoded_key) {
            Some(bytes) => {
                let (entry, _): (Entry<V>, _) = serde::decode_from_slice(bytes, config)?;
                Ok(entry.get_data())
            }
            None => Ok(None),
        }
    }

    pub fn run_compaction(&self) -> Result<(), DBError>{
        self.store.write_all(&self.data)?;
        Ok(())
    }
}
