use crate::entry::Entry;
use crate::error::DBError;
use crate::record::Action;
use crate::store::Store;
use ::serde::{Deserialize, Serialize};
use bincode::{config, serde};
use std::collections::HashMap;

pub struct DB {
    data: HashMap<Vec<u8>, Vec<u8>>,
    store: Store,
}

impl DB {
    pub fn new(path: &str) -> Result<Self, DBError> {
        let store = Store::new(path.to_string());
        let mut data = HashMap::new();
        for record in store.iter_from_file() {
            let record = record?;
            match record.action {
                Action::Set => data.insert(record.key, record.value),
                Action::Delete => data.remove(&record.key),
            };
        }
        Ok(Self { store, data })
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
}
