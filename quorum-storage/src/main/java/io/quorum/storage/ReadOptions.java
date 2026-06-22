package io.quorum.storage;

/** @param snapshot read as of this snapshot, or the latest state if null. */
public record ReadOptions(Snapshot snapshot) {
    public static final ReadOptions DEFAULT = new ReadOptions(null);
}
