use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::DB;

pub async fn compact(db: Arc<Mutex<DB>>, interval: Duration){
    let mut ticker = tokio::time::interval(interval);
    loop{
        ticker.tick().await;

        println!("Compacting");
        db.lock().unwrap().run_compaction().unwrap();
    }
}