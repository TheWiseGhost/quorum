package io.quorum.storage;

/** Cursor over internal keys in {@link InternalKey#COMPARATOR} order. */
interface InternalIterator extends AutoCloseable {
    boolean valid();

    void seekToFirst();

    /** Positions at the first entry {@code >= target}. */
    void seek(byte[] target);

    void next();

    byte[] key();

    byte[] value();

    @Override
    default void close() {}
}
