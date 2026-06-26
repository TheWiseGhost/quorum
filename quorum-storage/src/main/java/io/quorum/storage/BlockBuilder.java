package io.quorum.storage;

import io.quorum.common.ByteWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds a prefix-compressed block. Entry: {@code shared:varint | unshared:varint |
 * valueLen:varint | keyDelta | value}. Every {@code restartInterval} entries the full key is
 * stored and its offset recorded, enabling binary search. Trailer: restart offsets (u32 each)
 * then the restart count (u32).
 */
final class BlockBuilder {
    private final int restartInterval;
    private final ByteWriter buf = new ByteWriter(4096);
    private final List<Integer> restarts = new ArrayList<>();
    private byte[] lastKey = new byte[0];
    private int counter;
    private int entries;

    BlockBuilder(int restartInterval) {
        this.restartInterval = restartInterval;
        restarts.add(0);
    }

    void add(byte[] key, byte[] value) {
        int shared = 0;
        if (counter < restartInterval) {
            int max = Math.min(lastKey.length, key.length);
            while (shared < max && lastKey[shared] == key[shared]) shared++;
        } else {
            restarts.add(buf.position());
            counter = 0;
        }
        buf.putVarInt(shared).putVarInt(key.length - shared).putVarInt(value.length);
        buf.putRaw(key, shared, key.length - shared).putRaw(value);
        lastKey = key;
        counter++;
        entries++;
    }

    int sizeEstimate() {
        return buf.position() + restarts.size() * 4 + 4;
    }

    boolean isEmpty() {
        return entries == 0;
    }

    byte[] lastKey() {
        return lastKey;
    }

    byte[] finish() {
        for (int r : restarts) buf.putInt(r);
        buf.putInt(restarts.size());
        return buf.toByteArray();
    }

    void reset() {
        buf.reset();
        restarts.clear();
        restarts.add(0);
        lastKey = new byte[0];
        counter = 0;
        entries = 0;
    }

    static byte[] copyKey(byte[] k) {
        return Arrays.copyOf(k, k.length);
    }
}
