package io.quorum.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Cursor over a byte array, the inverse of {@link ByteWriter}. */
public final class ByteReader {
    private final byte[] buf;
    private int pos;
    private final int limit;

    public ByteReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public ByteReader(byte[] buf, int off, int len) {
        this.buf = buf;
        this.pos = off;
        this.limit = off + len;
    }

    private void need(int n) {
        if (n < 0 || pos + n > limit) throw new CorruptionException("truncated input at " + pos + " need " + n);
    }

    public int getByte() {
        need(1);
        return buf[pos++] & 0xff;
    }

    public int getInt() {
        need(4);
        int v = Bytes.getIntLE(buf, pos);
        pos += 4;
        return v;
    }

    public long getLong() {
        need(8);
        long v = Bytes.getLongLE(buf, pos);
        pos += 8;
        return v;
    }

    public long getVarLong() {
        long v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int b = getByte();
            v |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return v;
        }
        throw new CorruptionException("varint too long");
    }

    public int getVarInt() {
        long v = getVarLong();
        if (v > 0xffffffffL) throw new CorruptionException("varint32 overflow");
        return (int) v;
    }

    public byte[] getRaw(int len) {
        need(len);
        byte[] out = Arrays.copyOfRange(buf, pos, pos + len);
        pos += len;
        return out;
    }

    public byte[] getBytes() {
        return getRaw(getVarInt());
    }

    public byte[] getNullableBytes() {
        int n = getVarInt();
        return n == 0 ? null : getRaw(n - 1);
    }

    public String getString() {
        return new String(getBytes(), StandardCharsets.UTF_8);
    }

    public boolean getBool() {
        return getByte() != 0;
    }

    public int position() {
        return pos;
    }

    public void position(int p) {
        pos = p;
    }

    public int remaining() {
        return limit - pos;
    }

    public boolean hasRemaining() {
        return pos < limit;
    }

    public byte[] array() {
        return buf;
    }
}
