package io.quorum.raft;

/** State that must be durable before a node sends any message that depends on it. */
public record HardState(long term, long votedFor, long commit) {
    public static final HardState EMPTY = new HardState(0, 0, 0);
}
