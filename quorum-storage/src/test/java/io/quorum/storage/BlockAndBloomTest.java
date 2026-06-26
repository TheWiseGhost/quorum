package io.quorum.storage;

import io.quorum.common.Bytes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class BlockAndBloomTest {
    @Test
    void blockSeekFindsEveryKeyAndGaps() {
        BlockBuilder b = new BlockBuilder(4);
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 500; i += 2) {
            byte[] k = Bytes.utf8(String.format("key-%05d", i));
            keys.add(k);
            b.add(k, Bytes.utf8("v" + i));
        }
        Block block = new Block(b.finish());
        Block.Iter it = block.iterator(Bytes.COMPARATOR);
        it.seekToFirst();
        int n = 0;
        for (; it.valid(); it.next()) assertArrayEquals(keys.get(n++), it.key());
        assertEquals(keys.size(), n);
        for (int i = 0; i < 500; i++) {
            it.seek(Bytes.utf8(String.format("key-%05d", i)));
            int expect = (i + 1) / 2 * 2;
            if (expect >= 500) {
                assertFalse(it.valid());
            } else {
                assertTrue(it.valid());
                assertEquals(String.format("key-%05d", expect), Bytes.str(it.key()));
                assertEquals("v" + expect, Bytes.str(it.value()));
            }
        }
    }

    @Test
    void bloomHasNoFalseNegativesAndLowFalsePositiveRate() {
        int n = 10_000;
        long[] hashes = new long[n];
        for (int i = 0; i < n; i++) hashes[i] = BloomFilter.hash(Bytes.utf8("present-" + i));
        byte[] filter = BloomFilter.build(hashes, n, 10);
        for (int i = 0; i < n; i++) assertTrue(BloomFilter.mayContain(filter, hashes[i]));
        int fp = 0;
        for (int i = 0; i < n; i++) if (BloomFilter.mayContain(filter, BloomFilter.hash(Bytes.utf8("absent-" + i)))) fp++;
        double rate = fp / (double) n;
        assertTrue(rate < 0.02, "false positive rate " + rate);
    }

    @Test
    void blockCacheEvictsWithinCapacity() {
        BlockCache cache = new BlockCache(10_000);
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            BlockBuilder b = new BlockBuilder(16);
            b.add(Bytes.utf8("k"), new byte[r.nextInt(500)]);
            cache.put(i % 7, i, new Block(b.finish()));
            assertTrue(cache.usedBytes() <= 10_000);
        }
        cache.evictFile(3);
        for (int i = 0; i < 1000; i++) if (i % 7 == 3) assertNull(cache.get(3, i));
    }
}
