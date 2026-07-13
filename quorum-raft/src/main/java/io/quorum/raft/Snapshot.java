package io.quorum.raft;

/** A state machine image at {@code meta.index()}; {@code data} is opaque to Raft. */
public record Snapshot(SnapshotMeta meta, byte[] data) {
    public long index() {
        return meta.index();
    }
}
