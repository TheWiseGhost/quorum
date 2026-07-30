# Bugs caught by the test harness

Every real bug the crash tests or the simulator found during development: how it was found, the root cause, and the fix.

| # | Found by | Symptom | Root cause | Fix |
|---|----------|---------|------------|-----|
| 1 | `CrashRecoveryTest` (SimFileSystem power loss), 32 of 40 seeds | After a crash, reopening failed with `FileNotFoundException: db/000040.sst` | A flush or compaction fsynced the new SSTable's *contents* and the MANIFEST record, but never the *directory entry* for the new file. On POSIX file systems a newly created file's name is not durable until its parent directory is fsynced, so the MANIFEST could name a table that no longer existed after the crash. | `VersionSet.logAndApply` fsyncs the database directory before appending the MANIFEST record. |
| 2 | `RaftStorageTest` crash sweep, 20 of 20 seeds | After a follower overwrote a conflicting log suffix and then crashed, recovery lost dozens of *fsynced* entries | `truncateFrom` reopened the segment and built its `LogWriter` *before* truncating the file. The writer cached the old file size as its append offset, so new records landed after a hole of zeros. Recovery read the zeros as a torn frame and dropped everything after them. | Truncate and fsync first, then construct the writer at the real end of the file. |
