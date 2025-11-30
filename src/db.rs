use std::collections::HashMap;
use std::path::Path;
use ::serde::{Serialize, Deserialize};
use bincode::{config, serde};
use crate::entry::Entry;
use crate::error::DBError;
pub struct DB {
    data: HashMap<Vec<u8>, Vec<u8>>,
    path: String,
}



impl DB {
    pub fn new(path: &str) -> Self {
        let data = if Path::new(path).exists() {
            HashMap::new() // TODO: Fix load_from_file to return correct type
        } else {
            HashMap::new()
        };
        Self {
            data,
            path: path.to_string(),
        }
    }

    pub fn set<K: Serialize, V: Serialize>(&mut self, key: K, value: V, ttl: Option<u64>) -> Result<(), DBError> {
        let config = config::standard();
        let encoded_key = serde::encode_to_vec(&key, config)?;
        let store = Entry::new(value, ttl);
        let encoded_store = serde::encode_to_vec(&store, config)?;
        self.data.insert(encoded_key, encoded_store);
        Ok(())
    }

    pub fn get<K: Serialize, V: for<'de> Deserialize<'de>>(&self, key: K) -> Result<Option<V>, DBError> {
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

    
}

