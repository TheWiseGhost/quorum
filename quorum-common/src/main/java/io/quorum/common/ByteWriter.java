package io.quorum.common;

import java.util.Arrays;

/** Growable little-endian byte buffer with LEB128 varints. */
public final class ByteWriter {
    private byte[] buf;
    private int pos;

    public ByteWriter() {
        this(64);
    }

    public ByteWriter(int capacity) {
        buf = new byte[Math.max(capacity, 16)];
    }

    private void ensure(int extra) {
        if (pos + extra > buf.length) buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + extra));
    }

    public ByteWriter putByte(int b) {
        ensure(1);
        buf[pos++] = (byte) b;
        return this;
    }

    public ByteWriter putInt(int v) {
        ensure(4);
        Bytes.putIntLE(buf, pos, v);
        pos += 4;
        return this;
    }

    public ByteWriter putLong(long v) {
        ensure(8);
        Bytes.putLongLE(buf, pos, v);
        pos += 8;
        return this;
    }

    public ByteWriter putVarLong(long v) {
        ensure(10);
        while ((v & ~0x7FL) != 0) {
            buf[pos++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf[pos++] = (byte) v;
        return this;
    }

    public ByteWriter putVarInt(int v) {
        return putVarLong(v & 0xffffffffL);
    }

    public ByteWriter putRaw(byte[] b) {
        return putRaw(b, 0, b.length);
    }

    public ByteWriter putRaw(byte[] b, int off, int len) {
        ensure(len);
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
        return this;
    }

    /** Varint length prefix then bytes. */
    public ByteWriter putBytes(byte[] b) {
        putVarInt(b.length);
        return putRaw(b);
    }

    /** Like {@link #putBytes} but encodes null distinctly from empty. */
    public ByteWriter putNullableBytes(byte[] b) {
        if (b == null) return putVarInt(0);
        putVarInt(b.length + 1);
        return putRaw(b);
    }

    public ByteWriter putString(String s) {
        return putBytes(Bytes.utf8(s));
    }

    public ByteWriter putBool(boolean b) {
        return putByte(b ? 1 : 0);
    }

    public int position() {
        return pos;
    }

    public void reset() {
        pos = 0;
    }

    /** Overwrites 4 bytes at an earlier position. */
    public void setInt(int at, int v) {
        Bytes.putIntLE(buf, at, v);
    }

    public byte[] array() {
        return buf;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, pos);
    }
}
