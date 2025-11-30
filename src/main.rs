use std::thread::sleep;
use std::time::Duration;
use weaver::DB;

fn main() {
    //init a hash map to store the data
    let mut db = DB::new("data.txt");
    db.set("name", "John", Some(1));
    sleep(Duration::from_secs(2));
    let name: Option<String> = db.get("name").unwrap();
    println!("name: {:?}", name);
}
