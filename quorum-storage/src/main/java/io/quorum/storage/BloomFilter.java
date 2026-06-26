package io.quorum.storage;

import io.quorum.common.Bytes;

/**
 * Bloom filter with double hashing (Kirsch–Mitzenmacher): probe i is {@code h1 + i*h2}.
 * With 10 bits/key and k=7 the false-positive rate is ~0.8%.
 * Encoding: {@code bits[] | k:u8}.
 */
public final class BloomFilter {
    private BloomFilter() {}

    public static byte[] build(long[] hashes, int count, int bitsPerKey) {
        int k = Math.max(1, Math.min(30, (int) Math.round(bitsPerKey * 0.69)));
        long bits = Math.max(64, (long) count * bitsPerKey);
        int bytes = (int) ((bits + 7) / 8);
        bits = bytes * 8L;
        byte[] out = new byte[bytes + 1];
        out[bytes] = (byte) k;
        for (int i = 0; i < count; i++) {
            long h = hashes[i];
            long h1 = h & 0xffffffffL, h2 = h >>> 32;
            for (int j = 0; j < k; j++) {
                long bit = Math.floorMod(h1 + j * h2, bits);
                out[(int) (bit >>> 3)] |= (byte) (1 << (bit & 7));
            }
        }
        return out;
    }

    public static boolean mayContain(byte[] filter, long hash) {
        if (filter.length < 2) return true;
        int bytes = filter.length - 1;
        int k = filter[bytes];
        if (k > 30) return true; // reserved for future encodings
        long bits = bytes * 8L;
        long h1 = hash & 0xffffffffL, h2 = hash >>> 32;
        for (int j = 0; j < k; j++) {
            long bit = Math.floorMod(h1 + j * h2, bits);
            if ((filter[(int) (bit >>> 3)] & (1 << (bit & 7))) == 0) return false;
        }
        return true;
    }

    public static long hash(byte[] userKey) {
        return Bytes.hash64(userKey);
    }
}
