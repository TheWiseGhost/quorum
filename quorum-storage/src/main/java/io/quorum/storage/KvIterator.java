package io.quorum.storage;

import java.util.Iterator;

/** Forward iterator over live user keys. Must be closed to release the underlying version. */
public interface KvIterator extends Iterator<KvIterator.Entry>, AutoCloseable {
    record Entry(byte[] key, byte[] value) {}

    @Override
    void close();
}
