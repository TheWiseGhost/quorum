package io.quorum.kv;

/** Client-facing operations accepted by a {@link KvNode}. */
public sealed interface Request {
    record Get(byte[] key) implements Request {}

    record Scan(byte[] from, byte[] to, int limit) implements Request {}

    record Write(Command command) implements Request {}

    record AddLearner(long nodeId) implements Request {}

    record Promote(long nodeId) implements Request {}

    record Remove(long nodeId) implements Request {}

    record TransferLeader(long nodeId) implements Request {}

    record Status() implements Request {}
}
