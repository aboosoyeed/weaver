Next priorities:

1. Tests (done)
- Roundtrip: set → get → delete → get
- Recovery: set, close, reopen, verify data
- TTL expiry
- Corrupted file handling

2. Error handling cleanup (next)
- Store::set/delete currently use unwrap() - should return Result

3. Compaction
- Log grows forever - need to merge and remove stale entries

4. Server mode
- TCP server with simple protocol (Redis RESP or custom)