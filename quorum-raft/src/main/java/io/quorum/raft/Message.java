package io.quorum.raft;

import java.util.List;

/** Raft RPCs. All messages are immutable; {@code term} is the sender's term (or the term requested by a pre-vote). */
public sealed interface Message {
    long from();

    long to();

    long term();

    record RequestVote(long from, long to, long term, long lastIndex, long lastTerm, boolean preVote,
                       boolean transfer) implements Message {}

    record RequestVoteResponse(long from, long to, long term, boolean granted, boolean preVote) implements Message {}

    record AppendEntries(long from, long to, long term, long prevIndex, long prevTerm, List<Entry> entries,
                         long commit) implements Message {}

    /**
     * @param index    on success, the last index now matching the leader; on rejection, the
     *                 rejected {@code prevIndex}
     * @param hintIndex/hintTerm fast-backtracking hint on rejection
     */
    record AppendEntriesResponse(long from, long to, long term, boolean success, long index, long hintIndex,
                                 long hintTerm) implements Message {}

    /** @param readCtx latest ReadIndex round this heartbeat confirms (0 if none) */
    record Heartbeat(long from, long to, long term, long commit, long readCtx) implements Message {}

    record HeartbeatResponse(long from, long to, long term, long readCtx) implements Message {}

    record InstallSnapshot(long from, long to, long term, SnapshotMeta meta, long offset, byte[] chunk,
                           long totalSize) implements Message {}

    /** @param nextOffset bytes received so far; equals total size once installed */
    record InstallSnapshotResponse(long from, long to, long term, long snapshotIndex, long nextOffset,
                                   boolean done) implements Message {}

    record TimeoutNow(long from, long to, long term) implements Message {}
}
