# Quorum

[![ci](https://github.com/TheWiseGhost/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/TheWiseGhost/quorum/actions/workflows/ci.yml)

A linearizable, fault-tolerant distributed key-value store written from scratch in Java 25. Every replica runs a **Raft** implementation over a custom **LSM-tree storage engine**. Correctness is checked by a **deterministic simulator** that runs a whole cluster in a single thread under crashes, power loss, torn writes, partitions, message loss, GC pauses, clock skew and membership churn. A **linearizability checker** then verifies every client history it produces.

```
$ ./gradlew :quorum-sim:simSweep -Pseeds=20000 -Pstart=100000 -PsimArgs="--duration 20000"
20000 seeds [100000..119999], 0 failed
  39,014,250 client ops checked, 211,176 leader elections, 656,682 faults injected,
  32,507 membership changes, 272,809 snapshots
  1,417,260,374 events, 6930.0 simulated minutes in 244.2s wall (1702x real time)
```

That is about 115 hours of cluster time under continuous faults, checked in four minutes on a laptop.

No third-party code is used outside tests and benchmarks: no Netty, no RocksDB, no Raft library. The only runtime dependency is the JDK.

## Highlights

- **Raft** (`quorum-raft`): a pure, I/O-free state machine in the style of etcd/raft. It has leader election with **PreVote** and **CheckQuorum**, **pipelined** replication with fast log backtracking, **chunked snapshot** transfer, **ReadIndex** linearizable reads, **single-server membership changes** through learners, and **leadership transfer**. Leaders send AppendEntries *before* their own fsync, which overlaps network and disk.
- **LSM storage engine** (`quorum-storage`): a write-ahead log with **group commit**, a skiplist memtable, and **SSTables** with prefix-compressed blocks, restart-point binary search, per-block CRC32C and **bloom filters**. It also has **leveled compaction** with snapshot-aware garbage collection, **MVCC snapshots**, a **CLOCK block cache**, versioned manifests with reference-counted versions, and **hard-link checkpoints**.
- **Replicated KV** (`quorum-kv`): get, put, delete, **CAS** and range scans, with **exactly-once** writes via client sessions, and Raft snapshots built from LSM checkpoints.
- **Deterministic simulator** (`quorum-sim`): a virtual-time event loop, a disk model that loses unsynced data and tears writes on power loss, a hostile network, swarm-tested fault schedules, and continuous Raft invariant checks. Any failure **replays exactly from its seed**.
- **Linearizability checker** (`quorum-check`): the Wing & Gong search with Lowe's memoization, partitioned per key (P-compositionality). Indeterminate (timed-out) operations are supported, and an **HTML timeline** is rendered for any history that fails.
- **Mutation-tested tests**: six deliberately planted bugs, each of which the simulator must catch.
- **Production plumbing**: an NIO selector-based peer transport, a **virtual-thread** client server with request pipelining, an async Java client with leader discovery and retries, a CLI, Prometheus `/metrics`, Docker Compose, and a YCSB binding.

## Architecture

```
          clients (QuorumClient / CLI / YCSB)
                       │  pipelined binary protocol, one virtual thread per connection
┌──────────────────────▼──────────────────────────────────────────────────────┐
│ QuorumServer                                                                │
│  ClientServer ──► node loop (single thread) ◄── NioPeerTransport (selector) │
│                       │                                                     │
│   KvNode: drives Raft via Ready → persist → send → apply → advance          │
│     ├─ RaftNode      pure state machine: no I/O, no clock, no threads       │
│     ├─ RaftStorage   segmented log + hard state + snapshot (atomic files)   │
│     └─ KvStateMachine ── LsmEngine                                          │
│                            WAL → memtable → L0 … L6 SSTables (+bloom, CLOCK) │
└─────────────────────────────────────────────────────────────────────────────┘
       every effect goes through Clock / Scheduler / FileSystem / Transport
                       │                                │
              real implementations               simulated implementations
          (FileChannel, NIO, executors)    (SimWorld, SimFileSystem, SimCluster)
```

The core modules (`raft`, `storage`, `kv`) are banned from using real time, threads, files or sockets, and a test enforces it. That restriction is what allows the same code to run unmodified, deterministically, inside the simulator. See [DESIGN.md](DESIGN.md) for the reasoning behind each major decision.

## Correctness

### What the simulator checks

| Property | When |
|---|---|
| **Election Safety**: at most one leader per term | every time a node becomes leader |
| **Leader Completeness**: a new leader holds every entry committed in an earlier term | every time a node becomes leader |
| **State Machine Safety**: no two nodes apply different entries at the same index | every apply, on every node |
| **Log Matching**: logs that agree at an (index, term) are identical up to there | every simulated second, and at the end |
| **Linearizability** of the full client history (get/put/delete/CAS, including timed-out ops) | end of run |
| **Liveness**: once faults heal, a write and a read of every key complete within 20 s | end of run |
| **Convergence**: voters at the same applied index hold byte-identical data | end of run |

### The tests have teeth: planted bugs

Each bug is switched on in turn, and seeds are searched until the simulator reports a violation (`./gradlew :quorum-sim:plantedBugs`):

| Planted bug | Caught by | First failing seed |
|---|---|---|
| `FIGURE8_COMMIT`: commit an earlier-term entry by counting replicas (Raft paper, Fig. 8) | Leader Completeness | 1918 |
| `VOTE_NOT_PERSISTED`: `votedFor` not durable, so a node can vote twice after a crash | Election Safety | 59 |
| `LOG_NO_FSYNC`: followers acknowledge appends before fsync | Leader Completeness | 2 |
| `NO_CLIENT_DEDUP`: a retried CAS can apply twice | Linearizability | 1 |
| `LEADER_LOCAL_READ`: a deposed leader serves reads without ReadIndex | Linearizability | 13 |
| `NO_TRUNCATE_CONFLICT`: a follower keeps conflicting entries | State Machine Safety | 3 |

The first failing seeds are identical on an Apple ARM laptop and on GitHub's Linux x86 CI runners: the simulator is deterministic across machines, so any failure CI finds can be replayed locally from its seed.

Figure 8 needs a precise sequence: five nodes, two leader crashes at the right moments, and a stale entry replicated to exactly a majority. It takes about 2,000 random seeds to find, so CI runs the planted-bug suite with a 2,500-seed budget.

### Real bugs the tests found

The crash tests, simulator and integration tests found six real bugs during development. Each is written up with its symptom, root cause and fix in [BUGS.md](BUGS.md). Three examples:

- **A performance optimization that broke safety.** Skipping the commit-index fsync doubled write throughput, but a restarted node could then rebuild a membership several changes out of date. The 10,000-seed sweep found **two leaders elected in the same term** in 8 seeds, all involving membership churn plus crashes. The fix keeps the optimization except when a batch commits a membership change.
- **WAL rotation without fsync.** It caused silent replica divergence: two followers held different values at the same applied index. The cause was that unsynced data in two files can be lost independently, so a crash could keep a later batch and drop an earlier one. It was found through a planted-bug run and then reproduced in isolation.
- **SSTable created without a directory fsync.** After power loss, the manifest named a table that no longer existed.

## Benchmarks

Hardware: Apple M5 MacBook (10 cores), internal SSD, JDK 25 (Temurin). All numbers come from the scripts in `quorum-bench` and can be rerun with the commands below.

### Storage engine (`./gradlew :quorum-bench:dbBench`), 1M keys, 100-byte values

| benchmark | ops/sec | p50 µs | p99 µs |
|---|---:|---:|---:|
| fillseq | 192,447 | 2 | 16 |
| fillrandom | 85,212 | 5 | 36 |
| overwrite | 87,861 | 6 | 40 |
| readrandom | 119,305 | 7 | 29 |
| readmissing (bloom filters) | 4,620,333 | 1 | 1 |
| readseq | 1,315,935 | – | – |
| fsynced writes, 1 thread | 286 | 3,083 | 6,439 |
| fsynced writes, 8 threads (**group commit**) | 1,454 | 5,915 | 8,879 |

After 2M random writes: **write amplification 5.0**, **space amplification 0.99**, 110 compactions, and a three-level tree (L0: 3, L1: 5, L2: 47 tables). Group commit turns 8 concurrent fsync writers into **5.1× the throughput** of one, because they share each fsync.

### 3-node cluster (`./gradlew :quorum-bench:clusterBench`), YCSB-style, Zipfian keys, 128 outstanding requests, fsync on

| workload | ops/sec | p50 ms | p99 ms | p999 ms |
|---|---:|---:|---:|---:|
| A (50% read / 50% update) | 1,988 | 63.0 | 95.7 | 231.7 |
| B (95% read / 5% update) | 4,060 | 31.0 | 62.3 | 83.4 |
| C (100% read, ReadIndex) | 57,480 | 1.9 | 3.0 | 4.2 |
| F (read-modify-write via CAS) | 1,307 | 97.9 | 147.5 | 226.1 |

**These write numbers are disk-bound.** On macOS, `FileChannel.force` issues `F_FULLFSYNC`, which this SSD can perform about **260 times per second in total**, shared by all processes (measured: 3.9 ms per fsync alone, 11 ms with 3 concurrent fsyncers). All three replicas share that one disk. That the cluster sustains ~1,000 replicated, fsynced writes per second anyway comes from batching: about 10 commands share each fsync.

### Ablations (workload A, same setup)

| configuration | ops/sec | p50 ms | what it shows |
|---|---:|---:|---|
| default | 1,963 | 64.5 | |
| **batching off** (one Ready, so one fsync, per request) | 178 | 695.3 | batching is worth **11×** |
| pipelining off (`maxInflight=1`) | 2,345 | 54.8 | no gain while fsync-bound: one message in flight lets followers batch more |
| **fsync off** (unsafe; isolates the software path) | 29,000 | 4.0 | CPU, network and serialization handle **~15×** what this disk allows |

### JMH microbenchmarks (`./gradlew :quorum-bench:jmh`)

| benchmark | result |
|---|---:|
| bloom filter probe | 4.4 ns |
| key hash (64-bit) | 9.1 ns |
| CRC32C, 4 KiB block (JDK intrinsic) | 675 ns (≈6 GB/s) |
| encode AppendEntries (64 entries) | 2.4 µs |
| LSM get, key absent (bloom short-circuit) | ~3.0 M ops/s |

## Running it

Requires JDK 25 (the Gradle toolchain finds it automatically).

```bash
./gradlew build
```

That runs about 120 unit and integration tests, plus a 50-seed simulation smoke test, determinism checks and the core-isolation rule.

**Simulation:**

```bash
./gradlew :quorum-sim:simSweep -Pseeds=1000 -PsimArgs="--duration 20000"
```

```bash
./gradlew :quorum-sim:simSweep -PsimArgs="--seed 1234 --trace"
```

```bash
./gradlew :quorum-sim:simSweep -PsimArgs="--seed 1234 --determinism"
```

```bash
./gradlew :quorum-sim:plantedBugs -PmaxSeeds=2500
```

The commands in order: a 1,000-seed sweep, replaying one seed with its full event trace, running a seed twice to compare trace hashes, and the mutation suite. A failing run prints the seed, the violated property, a replay command and, for linearizability failures, an HTML timeline in `quorum-sim/sim-failures/`.

**A real cluster:**

```bash
docker compose up -d
```

```bash
docker compose exec n1 quorum --servers n1:8000,n2:8000,n3:8000 put hello world
```

```bash
docker compose exec n1 quorum --servers n1:8000,n2:8000,n3:8000 status
```

```bash
docker compose kill n3
```

```bash
docker compose exec n1 quorum --servers n1:8000,n2:8000,n3:8000 put after failover
```

```bash
curl -s localhost:9002/metrics | grep quorum_
```

That is: start three nodes, write a key, see which node is `LEADER`, kill that node (use its name in place of `n3`), write again (the survivors elect a new leader in under a second; this was verified end to end, with the write completing in 0.7 s including CLI startup), and read the Prometheus metrics from a surviving node. `docker compose start n3` brings the node back, and it catches up as a follower.

To run without Docker, build the launchers:

```bash
./gradlew :quorum-server:installDist :quorum-client:installDist
```

Then start each node with `quorum-server/build/install/quorum-server/bin/quorum-server --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --client 127.0.0.1:8001 --metrics 9001 --data data/n1` (and the same for ids 2 and 3), and use `quorum-client/build/install/quorum/bin/quorum put|get|cas|scan|status|add-learner|promote|remove|transfer`.

**Benchmarks:**

```bash
./gradlew :quorum-bench:dbBench -PbenchArgs="--num 1000000"
```

```bash
./gradlew :quorum-bench:clusterBench -PbenchArgs="--workload all --records 20000 --ops 100000"
```

```bash
./gradlew :quorum-bench:clusterBench -PbenchArgs="--workload A --no-fsync"
```

```bash
./gradlew :quorum-bench:jmh -PbenchArgs="-f 1"
```

To use the official YCSB harness, put the `quorum-bench` jar on YCSB's classpath and pass `-db io.quorum.bench.QuorumYcsbClient -p quorum.servers=h:p,h:p,h:p`.

## Layout

| module | contents |
|---|---|
| `quorum-common` | `Clock`, `Scheduler`, `FileSystem` (real + crash-simulating), WAL framing, codecs, planted-bug flags |
| `quorum-storage` | LSM engine: WAL, memtable, SSTable, bloom filter, block cache, manifest/versions, compaction, iterators, checkpoints |
| `quorum-raft` | `RaftNode` (pure), `RaftLog`, `RaftStorage` (segmented durable log), message codec |
| `quorum-kv` | `KvNode` driver, `KvStateMachine`, commands, sessions, client protocol |
| `quorum-check` | linearizability checker, history recorder, HTML visualizer |
| `quorum-sim` | `SimWorld`, simulated cluster/network/disk, nemesis, membership driver, invariant checks, runners |
| `quorum-net` | NIO peer transport, virtual-thread client server, request metrics |
| `quorum-server` | server process, config, `/status` + `/metrics`, in-process `LocalCluster` |
| `quorum-client` | async client (session pool, leader discovery, retries) and CLI |
| `quorum-bench` | JMH, `DbBench`, `ClusterBench`, YCSB binding |

## Further reading

- [DESIGN.md](DESIGN.md): the design decisions, what gets fsynced and why, and known limitations.
- [BUGS.md](BUGS.md): every bug the harness caught, with root causes.
- Ongaro, *Consensus: Bridging Theory and Practice*; etcd/raft; LevelDB's `impl.md`; Lowe, *Testing for Linearizability*; Wilson, *Testing Distributed Systems w/ Deterministic Simulation* (FoundationDB).
