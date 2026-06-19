package io.quorum.common;


import java.nio.ByteBuffer;

/**
 * Append-only record log used for the WAL and the MANIFEST.
 * Frame: {@code maskedCrc32c:u32 | length:u32 | type:u8 | payload}; the CRC covers type+payload.
 */
public final class LogWriter implements AutoCloseable {
    public static final int HEADER = 9;
    public static final byte FULL = 1;

    private final FileHandle file;
    private long offset;

    public LogWriter(FileHandle file) {
        this.file = file;
        this.offset = file.size();
    }

    /** Appends one record and returns the number of bytes written. */
    public int add(byte[] payload) {
        byte[] frame = new byte[HEADER + payload.length];
        frame[8] = FULL;
        System.arraycopy(payload, 0, frame, HEADER, payload.length);
        Bytes.putIntLE(frame, 0, Crc.mask(Crc.crc32c(frame, 8, payload.length + 1)));
        Bytes.putIntLE(frame, 4, payload.length);
        file.write(offset, ByteBuffer.wrap(frame));
        offset += frame.length;
        return frame.length;
    }

    public void sync() {
        file.fsync();
    }

    public long size() {
        return offset;
    }

    @Override
    public void close() {
        file.close();
    }
}
