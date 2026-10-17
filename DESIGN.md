# Quorum design notes

This file records the decisions behind Quorum and the trade-offs each one makes. For what the system does and how to run it, see the [README](README.md).

## 1. The deterministic core

**Decision:** `quorum-raft`, `quorum-storage` and `quorum-kv` never touch real time, threads, sockets, files or unseeded randomness. Every effect goes through `Clock`, `Scheduler`, `FileSystem` and `Transport`, plus a seeded `Rng`.

**Why:** It makes deterministic simulation possible. The simulator plugs in a virtual-time event queue, a simulated disk and a simulated network. A whole 5-node cluster with clients and faults then runs in one thread as a pure function of a 64-bit seed. Any failure replays exactly from its seed.

**Enforcement:** `DeterministicCoreRuleTest` scans the core sources for banned APIs (`System.nanoTime`, `new Thread(`, `java.nio.channels`, `ThreadLocalRandom`, `parallelStream`, and others). `DeterminismTest` runs the same seed twice and compares a fingerprint of every event, message and apply.

**Cost:** Some code is less direct than it would otherwise be. For example, the LSM engine can't block a writer on a condition variable while waiting for a background flush, because in the single-threaded simulator nothing else would ever run. When no background work is in flight, the writer does the flush itself ("caller runs"). It only waits when another thread really is busy.

## 2. Raft as a pure state machine (etcd/raft style)

`RaftNode` has three inputs, `tick()`, `step(Message)` and the propose/read calls, and one output: `Ready`. The driver (`KvNode`) persists, sends, applies and then calls `advance()`.

- **Two message lists in `Ready`.** Leader-originated `AppendEntries`, `Heartbeat`, `InstallSnapshot` and `TimeoutNow` go out *before* the local fsync. Votes and acknowledgements go out *after* it. This is the parallel-disk-write optimization from the dissertation (§10.2.1). It is safe because the leader counts itself towards a commit quorum only once its own fsync has finished (`advance()` updates the leader's own `match`).
- **Membership takes effect at commit, not append.** Changes are single-server, and only one can be pending at a time. A leader may propose one only after it has committed an entry in its own term (the fix for the single-server-change bug Ongaro published in 2015). New servers join as learners, catch up, and are then promoted, so a cold new voter never stalls commits.
- **Bootstrap configuration lives in the log.** Every founding member writes the identical entry #1 (`BOOTSTRAP` with the initial voters) at term 1. A brand-new server can therefore rebuild the full membership history from the log alone. The first version kept the bootstrap configuration outside the log, and a joining server ended up thinking it was the only voter.
- **PreVote + CheckQuorum + leader stickiness.** A partitioned node can't inflate the term while it is cut off and then disrupt a healthy leader when it rejoins. A leader that can't reach a quorum steps down after one election timeout.
- **ReadIndex** batches confirmation rounds: each read records `(ctx, commitIndex, round)`, heartbeats carry the latest round, and one quorum of acks confirms every read up to that round. Reads arriving before the new leader's no-op commits wait for it.
- **Lease reads are deliberately not implemented.** They rely on bounded clock drift, and the simulator deliberately skews clocks by up to 2×. ReadIndex costs one round trip and is always correct. Leader-local reads exist only as a planted bug.

## 3. What gets fsynced, and why

| Write | fsync? | Reason |
|---|---|---|
| Raft log append | yes, once per `Ready` batch | an acknowledgement is a promise of durability |
| Hard state, term or vote changed | yes, atomic replace | a forgotten vote allows two leaders in one term. The planted `VOTE_NOT_PERSISTED` bug proves the check catches this. |
| Hard state, commit index only | **no**, *except* when the batch commits a membership change | a restarted node relearns the commit index from the leader, and apply is idempotent. Persisting it on every batch cost two fsyncs (file + directory) and halved write throughput. But membership is rebuilt by replaying conf entries up to the persisted commit index, so a node must never fall more than one configuration behind (bug #6). |
| State machine (LSM) writes | **no** | the Raft log is the source of truth. Each entry's mutations *and* the applied index go into one atomic `WriteBatch`, so a crash leaves the state machine at an exact log position and replay is idempotent. |
| LSM WAL on memtable rotation | **yes** | see bug #4 in [BUGS.md](BUGS.md). Unsynced data in two files is lost independently, so without this sync, recovery may not produce a prefix of history. |
| New file names (SSTables, segments, CURRENT) | directory fsync | a new file's name isn't durable until its parent directory is (bug #1) |

## 4. LSM tree choices

- **Leveled compaction over tiered.** Leveled gives lower read and space amplification (measured space amplification is ≈1.0 after overwrite-heavy workloads) at the cost of higher write amplification (≈5 measured in `DbBench`). A KV store serving linearizable reads favors reads.
- **All of L0 compacts together.** L0 tables overlap. Compacting a subset could leave an older version in L0 that shadows a newer one already moved to L1.
- **Output files are split only at user-key boundaries.** Each user key then lives in at most one table per level (L1 and below). That means compaction can pick a single file without LevelDB's boundary-expansion logic.
- **Snapshot-aware garbage collection.** The newest version visible to the oldest live snapshot is kept. A tombstone is dropped only when no deeper level could contain the key.
- **CLOCK block cache.** A hit just sets a reference bit (no list reordering under a lock), and eviction is second-chance.
- **Bloom filters use double hashing,** at 10 bits/key and k=7 (≈0.8% false positives, measured <2% in tests). Misses cost ~1 µs because a miss never touches a data block.

## 5. Exactly-once semantics

Clients tag each write with `(sessionId, seq)`. The state machine stores the last `seq` and its result per session, inside the same atomic batch as the write itself. A retry of the same `seq` gets the cached answer. Because only the last result is kept, each session may have **one write in flight**. `QuorumClient` gets concurrency from a *pool* of sessions, and it discards (rather than reuses) a session whose write timed out with an unknown outcome. Without dedup, a retried CAS can apply twice. The planted `NO_CLIENT_DEDUP` bug produces exactly that, and the linearizability checker flags it.

Session expiry is not implemented, so the session table grows by one small row per client session.

## 6. Snapshots

A Raft snapshot is an LSM checkpoint: immutable SSTables are hard-linked, then bundled with a fresh MANIFEST into one byte array. The leader streams it in chunks with offset-based acknowledgements, and a stalled transfer is resent on a heartbeat timer. The follower then:

1. persists the snapshot (atomic file replace),
2. discards its whole Raft log,
3. writes the tables into a new generation directory `kv/data-N+1`, fsyncs it, and atomically repoints `CURRENT_DATA`.

A crash between steps 1 and 3 is repaired at startup, because the state machine's applied index is behind the persisted snapshot, so the snapshot is installed again. Holding the bundle in memory caps snapshot size at the heap size; streaming it from files is a known next step.

## 7. The simulator

- **Disk model** (`SimFileSystem`): every write and truncate since the last fsync is pending. On a crash a random prefix of each file's pending operations survives, and the last surviving write may be torn at a random byte. Namespace operations (create, rename, link, unlink) are pending until their directory is fsynced, and a random prefix of those survives too.
- **Network model:** latency per seed (0.1–22 ms), loss of up to 15%, duplication, symmetric and one-way partitions, and "bridge" partitions where a single node can see both halves. Every message is serialized and parsed, so the wire codec is exercised on every hop.
- **Process model:** crash-restart (with power loss), GC-style pauses (all of a node's events are stashed and replayed on resume), clock skew (per-node tick rate between 0.7× and 1.4×), and membership churn that adds wiped nodes as learners, promotes them, and removes voters.
- **Swarm testing:** each seed enables a random subset of fault types with random intensities. Uniform fault mixes tend to hide bugs that need one particular mix.
- **Checks:** Election Safety and Leader Completeness when any node becomes leader, State Machine Safety on every apply, Log Matching every simulated second, linearizability of the whole client history at the end, and liveness after all faults heal (a write plus a read of every key within 20 s). Replicas must also converge, byte for byte, at equal applied index.
- **Mutation testing:** six planted bugs, each of which the suite must catch.

## 8. Known limitations

- Snapshots are materialized in memory, and so are installed snapshot bundles.
- There's no sharding (one Raft group), no transactions across keys, and no session expiry.
- The leader serves all reads through ReadIndex. Follower reads are not implemented.
- On macOS, `FileChannel.force` maps to `F_FULLFSYNC`, which this laptop can do ~260 times per second, shared by all processes. Fsync-on cluster throughput measured here is therefore disk-bound. See the README benchmarks for the fsync-off ablation that isolates the software path.
