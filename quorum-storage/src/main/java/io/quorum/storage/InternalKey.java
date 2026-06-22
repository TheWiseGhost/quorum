package io.quorum.storage;

import io.quorum.common.Bytes;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Internal keys are {@code userKey ‖ trailer} where trailer = {@code seq << 8 | type}, stored
 * little-endian in the last 8 bytes. Ordering: user key ascending, then trailer descending, so
 * the newest version of a key sorts first.
 */
public final class InternalKey {
    private InternalKey() {}

    public static final int TYPE_DELETE = 0;
    public static final int TYPE_PUT = 1;
    /** Highest type value; seeking with it finds the newest entry at or below a sequence. */
    static final int TYPE_FOR_SEEK = TYPE_PUT;
    public static final long MAX_SEQUENCE = (1L << 55) - 1;

    public static byte[] of(byte[] userKey, long seq, int type) {
        byte[] out = Arrays.copyOf(userKey, userKey.length + 8);
        Bytes.putLongLE(out, userKey.length, (seq << 8) | type);
        return out;
    }

    /** Key that sorts before every entry of {@code userKey} visible at {@code seq}. */
    public static byte[] seekKey(byte[] userKey, long seq) {
        return of(userKey, seq, TYPE_FOR_SEEK);
    }

    static long trailer(byte[] ik) {
        return Bytes.getLongLE(ik, ik.length - 8);
    }

    public static long sequence(byte[] ik) {
        return trailer(ik) >>> 8;
    }

    public static int type(byte[] ik) {
        return (int) (trailer(ik) & 0xff);
    }

    public static byte[] userKey(byte[] ik) {
        return Arrays.copyOf(ik, ik.length - 8);
    }

    static int compareUserKeys(byte[] ikA, byte[] ikB) {
        return Arrays.compareUnsigned(ikA, 0, ikA.length - 8, ikB, 0, ikB.length - 8);
    }

    /** Compares the user part of an internal key against a plain user key. */
    static int compareUserKey(byte[] ik, byte[] userKey) {
        return Arrays.compareUnsigned(ik, 0, ik.length - 8, userKey, 0, userKey.length);
    }

    static boolean sameUserKey(byte[] ik, byte[] userKey) {
        return compareUserKey(ik, userKey) == 0;
    }

    public static final Comparator<byte[]> COMPARATOR = (a, b) -> {
        int c = compareUserKeys(a, b);
        if (c != 0) return c;
        return Long.compare(trailer(b), trailer(a));
    };

    static String debug(byte[] ik) {
        return Bytes.str(userKey(ik)) + "@" + sequence(ik) + (type(ik) == TYPE_DELETE ? "D" : "P");
    }
}
