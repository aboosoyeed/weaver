use std::ops::Deref;
use std::thread::sleep;
use std::time::Duration;
use weaver::DB;

#[tokio::main]
async fn main() {
    //init a hash map to store the data
    let db = DB::start("data.txt").await.unwrap();
    db.lock().unwrap().set("name", "John", Some(1)).expect("TODO: panic message");
    sleep(Duration::from_secs(2));
    let name: Option<String> = db.lock().unwrap().get("name").unwrap();
    println!("name: {:?}", name);
}
