package io.quorum.common;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/** Positional file I/O. Writes are not durable until {@link #fsync()}. */
public interface FileHandle extends Closeable {
    /** Writes all remaining bytes of {@code src} at {@code position}. */
    void write(long position, ByteBuffer src);

    /** Reads up to {@code dst.remaining()} bytes; returns bytes read, or -1 at EOF. */
    int read(long position, ByteBuffer dst);

    long size();

    void truncate(long size);

    void fsync();

    @Override
    void close();

    default void append(ByteBuffer src) {
        write(size(), src);
    }

    default void append(byte[] bytes) {
        append(ByteBuffer.wrap(bytes));
    }

    /** Reads exactly {@code length} bytes or throws. */
    default byte[] readFully(long position, int length) {
        byte[] out = new byte[length];
        ByteBuffer dst = ByteBuffer.wrap(out);
        while (dst.hasRemaining()) {
            int n = read(position + dst.position(), dst);
            if (n < 0) throw new UncheckedIOException(new IOException(
                    "short read at " + position + " wanted " + length + " size " + size()));
        }
        return out;
    }

    default byte[] readAll() {
        long size = size();
        if (size > Integer.MAX_VALUE) throw new IllegalStateException("file too large");
        return readFully(0, (int) size);
    }
}
