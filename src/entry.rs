use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Entry<V> {
    data: V,
    expires_at: Option<u64>,
}

impl<V> Entry<V> {
    pub fn new(data: V, ttl_seconds: Option<u64>) -> Self {
        let expires_at = ttl_seconds.map(|ttl| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + ttl
        });
        Self { data, expires_at }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map_or(false,
                               |expires_at| SystemTime::now().duration_since(UNIX_EPOCH)
                                   .unwrap().as_secs() > expires_at)
    }

    pub fn get_data(self) -> Option<V> {
        if self.is_expired() {
            None
        } else {
            Some(self.data)
        }
    }
}