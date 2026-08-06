package io.quorum.kv;

import io.quorum.raft.Message;

/** Delivers Raft messages to peers. Best effort: messages may be lost, delayed or reordered. */
@FunctionalInterface
public interface Transport {
    void send(Message m);
}
