# Bugs caught by the test harness

Every real bug the crash tests or the simulator found during development: how it was found, the root cause, and the fix.

| # | Found by | Symptom | Root cause | Fix |
|---|----------|---------|------------|-----|
| 1 | `CrashRecoveryTest` (SimFileSystem power loss), 32 of 40 seeds | After a crash, reopening failed with `FileNotFoundException: db/000040.sst` | A flush or compaction fsynced the new SSTable's *contents* and the MANIFEST record, but never the *directory entry* for the new file. On POSIX file systems a newly created file's name is not durable until its parent directory is fsynced, so the MANIFEST could name a table that no longer existed after the crash. | `VersionSet.logAndApply` fsyncs the database directory before appending the MANIFEST record. |
