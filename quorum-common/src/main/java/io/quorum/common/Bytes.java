package io.quorum.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

public final class Bytes {
    private Bytes() {}

    public static final byte[] EMPTY = new byte[0];

    public static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    public static int compare(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String str(byte[] b) {
        return b == null ? "null" : new String(b, StandardCharsets.UTF_8);
    }

    public static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    public static boolean startsWith(byte[] b, byte[] prefix) {
        return b.length >= prefix.length && Arrays.equals(b, 0, prefix.length, prefix, 0, prefix.length);
    }

    public static byte[] longBE(long v) {
        byte[] out = new byte[8];
        putLongBE(out, 0, v);
        return out;
    }

    public static void putLongBE(byte[] b, int off, long v) {
        for (int i = 7; i >= 0; i--) {
            b[off + i] = (byte) v;
            v >>>= 8;
        }
    }

    public static long getLongBE(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (b[off + i] & 0xff);
        return v;
    }

    public static void putIntLE(byte[] b, int off, int v) {
        b[off] = (byte) v;
        b[off + 1] = (byte) (v >>> 8);
        b[off + 2] = (byte) (v >>> 16);
        b[off + 3] = (byte) (v >>> 24);
    }

    public static int getIntLE(byte[] b, int off) {
        return (b[off] & 0xff) | (b[off + 1] & 0xff) << 8 | (b[off + 2] & 0xff) << 16 | (b[off + 3] & 0xff) << 24;
    }

    public static void putLongLE(byte[] b, int off, long v) {
        putIntLE(b, off, (int) v);
        putIntLE(b, off + 4, (int) (v >>> 32));
    }

    public static long getLongLE(byte[] b, int off) {
        return (getIntLE(b, off) & 0xffffffffL) | ((long) getIntLE(b, off + 4) << 32);
    }

    /** Smallest key strictly greater than every key with this prefix, or null if none. */
    public static byte[] prefixEnd(byte[] prefix) {
        byte[] end = prefix.clone();
        for (int i = end.length - 1; i >= 0; i--) {
            if ((end[i] & 0xff) != 0xff) {
                end[i]++;
                return Arrays.copyOf(end, i + 1);
            }
        }
        return null;
    }

    /** 64-bit hash (MurmurHash3 fmix over 8-byte chunks) for bloom filters. */
    public static long hash64(byte[] b) {
        long h = 0x9E3779B97F4A7C15L ^ b.length;
        int i = 0;
        for (; i + 8 <= b.length; i += 8) {
            h ^= fmix(getLongLE(b, i));
            h = Long.rotateLeft(h, 27) * 0x9E3779B97F4A7C15L + 0x52dce729;
        }
        long tail = 0;
        for (int s = 0; i < b.length; i++, s += 8) tail |= (b[i] & 0xffL) << s;
        h ^= fmix(tail);
        return fmix(h);
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
