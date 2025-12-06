use std::io::Write;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;
use weaver::{DB, DBError};

#[test]
fn roundtrip_set_get_delete_get() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut db = DB::new(path).unwrap();

    // Set a value
    db.set("key1", "value1", None).unwrap();

    // Get it back
    let result: Option<String> = db.get("key1").unwrap();
    assert_eq!(result, Some("value1".to_string()));

    // Delete it
    db.delete("key1").unwrap();

    // Verify it's gone
    let result: Option<String> = db.get("key1").unwrap();
    assert_eq!(result, None);
}

#[test]
fn recovery_after_reopen() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    // Set some values and close
    {
        let mut db = DB::new(path).unwrap();
        db.set("key1", "value1", None).unwrap();
        db.set("key2", "value2", None).unwrap();
        db.delete("key1").unwrap();
    } // db dropped here, file closed

    // Reopen and verify data recovered
    {
        let db = DB::new(path).unwrap();
        let result: Option<String> = db.get("key1").unwrap();
        assert_eq!(result, None, "key1 should be deleted");

        let result: Option<String> = db.get("key2").unwrap();
        assert_eq!(result, Some("value2".to_string()), "key2 should persist");
    }
}

#[test]
fn ttl_expiry() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut db = DB::new(path).unwrap();

    // Set with 1 second TTL
    db.set("key1", "value1", Some(1)).unwrap();

    // Should exist immediately
    let result: Option<String> = db.get("key1").unwrap();
    assert_eq!(result, Some("value1".to_string()));

    // Wait for expiry
    thread::sleep(Duration::from_secs(2));

    // Should be gone
    let result: Option<String> = db.get("key1").unwrap();
    assert_eq!(result, None, "key should have expired");
}

#[test]
fn corrupted_file_handling() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Write garbage data
    temp_file.write_all(b"not a valid record").unwrap();
    temp_file.flush().unwrap();

    // Opening should fail with corruption error
    let result = DB::new(&path);
    match result {
        Err(DBError::CorruptedFile(_)) => {} // expected
        Err(e) => panic!("expected CorruptedFile error, got {:?}", e),
        Ok(_) => panic!("expected error, got Ok"),
    }
}
