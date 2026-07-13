package io.quorum.raft;

/**
 * A confirmed linearizable read: once the state machine has applied {@code index}, the read
 * identified by {@code ctx} may be served from local state.
 */
public record ReadState(long ctx, long index) {}
