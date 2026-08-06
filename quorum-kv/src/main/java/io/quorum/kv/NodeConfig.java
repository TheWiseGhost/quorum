package io.quorum.kv;

import io.quorum.common.PlantedBug;
import io.quorum.raft.ConfState;
import io.quorum.raft.RaftConfig;
import io.quorum.storage.Options;

import java.util.Set;

/**
 * @param bootstrap         initial voters for a founding member; {@link ConfState#EMPTY} to join later
 * @param tickNanos         Raft logical tick length (election timeout = electionTicks × tick)
 * @param snapshotEvery     take a snapshot after this many applied entries
 * @param segmentBytes      Raft log segment size
 * @param fsync             fsync the Raft log before acknowledging (false only for benchmark ablations: UNSAFE)
 * @param batching          coalesce queued requests/messages into one Ready (one fsync) per loop pass
 */
public record NodeConfig(long id, ConfState bootstrap, RaftConfig raft, Options storage, long tickNanos,
                         long snapshotEvery, long segmentBytes, Set<PlantedBug> bugs, boolean fsync, boolean batching) {
    public NodeConfig(long id, ConfState bootstrap, RaftConfig raft, Options storage, long tickNanos,
                      long snapshotEvery, long segmentBytes, Set<PlantedBug> bugs) {
        this(id, bootstrap, raft, storage, tickNanos, snapshotEvery, segmentBytes, bugs, true, true);
    }

    public static NodeConfig defaults(long id, ConfState bootstrap) {
        return new NodeConfig(id, bootstrap, RaftConfig.defaults(id), Options.defaults(), 100_000_000L, 10_000,
                64 << 20, PlantedBug.NONE);
    }

    public NodeConfig withFsync(boolean f) {
        return new NodeConfig(id, bootstrap, raft, storage, tickNanos, snapshotEvery, segmentBytes, bugs, f, batching);
    }

    public NodeConfig withBatching(boolean b) {
        return new NodeConfig(id, bootstrap, raft, storage, tickNanos, snapshotEvery, segmentBytes, bugs, fsync, b);
    }

    public NodeConfig withRaft(RaftConfig r) {
        return new NodeConfig(id, bootstrap, r, storage, tickNanos, snapshotEvery, segmentBytes, bugs, fsync, batching);
    }

    public boolean bug(PlantedBug b) {
        return bugs.contains(b);
    }
}
