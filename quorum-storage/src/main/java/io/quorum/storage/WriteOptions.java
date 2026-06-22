package io.quorum.storage;

/** @param sync fsync the write-ahead log before acknowledging. */
public record WriteOptions(boolean sync) {
    public static final WriteOptions SYNC = new WriteOptions(true);
    public static final WriteOptions NO_SYNC = new WriteOptions(false);
}
