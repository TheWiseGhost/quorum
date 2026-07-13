package io.quorum.raft;

import io.quorum.common.PlantedBug;

import java.util.Set;

public record RaftConfig(
        long id,
        int electionTicks,
        int heartbeatTicks,
        int maxInflight,
        int maxEntriesPerMessage,
        long maxBytesPerMessage,
        int snapshotChunkBytes,
        boolean preVote,
        boolean checkQuorum,
        Set<PlantedBug> bugs) {

    public static RaftConfig defaults(long id) {
        return new RaftConfig(id, 10, 1, 64, 256, 1 << 20, 1 << 20, true, true, PlantedBug.NONE);
    }

    public RaftConfig withBugs(Set<PlantedBug> b) {
        return new RaftConfig(id, electionTicks, heartbeatTicks, maxInflight, maxEntriesPerMessage,
                maxBytesPerMessage, snapshotChunkBytes, preVote, checkQuorum, b);
    }

    public RaftConfig withSnapshotChunkBytes(int n) {
        return new RaftConfig(id, electionTicks, heartbeatTicks, maxInflight, maxEntriesPerMessage,
                maxBytesPerMessage, n, preVote, checkQuorum, bugs);
    }

    public RaftConfig withMaxInflight(int n) {
        return new RaftConfig(id, electionTicks, heartbeatTicks, n, maxEntriesPerMessage,
                maxBytesPerMessage, snapshotChunkBytes, preVote, checkQuorum, bugs);
    }

    public RaftConfig withPreVote(boolean p) {
        return new RaftConfig(id, electionTicks, heartbeatTicks, maxInflight, maxEntriesPerMessage,
                maxBytesPerMessage, snapshotChunkBytes, p, checkQuorum, bugs);
    }

    public boolean bug(PlantedBug b) {
        return bugs.contains(b);
    }
}
