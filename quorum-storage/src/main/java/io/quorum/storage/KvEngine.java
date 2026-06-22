package io.quorum.storage;

import java.util.Optional;

/** An ordered, durable key-value store with atomic batches and MVCC snapshots. */
public interface KvEngine extends AutoCloseable {
    void write(WriteBatch batch, WriteOptions options);

    Optional<byte[]> get(byte[] key, ReadOptions options);

    /** Iterates user keys in {@code [from, to)}; {@code to == null} means unbounded. */
    KvIterator scan(byte[] from, byte[] to, ReadOptions options);

    Snapshot snapshot();

    /**
     * Writes a self-contained, consistent copy of the database into {@code dir} (hard-linking
     * immutable tables) and returns the file names written.
     */
    java.util.List<String> checkpoint(String dir);

    EngineStats stats();

    @Override
    void close();

    default Optional<byte[]> get(byte[] key) {
        return get(key, ReadOptions.DEFAULT);
    }

    default void put(byte[] key, byte[] value) {
        write(new WriteBatch().put(key, value), WriteOptions.NO_SYNC);
    }

    default void delete(byte[] key) {
        write(new WriteBatch().delete(key), WriteOptions.NO_SYNC);
    }
}
