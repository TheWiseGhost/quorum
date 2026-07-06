package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.Scheduler;
import io.quorum.common.SimFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** Random operations against the engine and a TreeMap model, with flushes, compactions and reopens. */
class LsmEngineModelTest {
    private static byte[] key(int i) {
        return Bytes.utf8(String.format("k%05d", i));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void matchesModel(long seed) {
        Random r = new Random(seed);
        SimFileSystem fs = new SimFileSystem();
        LsmEngine db = LsmEngine.open(fs, "db", Options.small(), Scheduler.INLINE);
        TreeMap<byte[], byte[]> model = new TreeMap<>(Bytes.COMPARATOR);
        long compactions = 0;
        for (int step = 0; step < 8000; step++) {
            int op = r.nextInt(100);
            byte[] k = key(r.nextInt(600));
            if (op < 55) {
                byte[] v = Bytes.utf8("v" + step + "-" + "x".repeat(r.nextInt(40)));
                db.put(k, v);
                model.put(k, v);
            } else if (op < 70) {
                db.delete(k);
                model.remove(k);
            } else if (op < 75) {
                WriteBatch b = new WriteBatch();
                for (int i = 0; i < 5; i++) {
                    byte[] bk = key(r.nextInt(600));
                    if (r.nextBoolean()) {
                        b.put(bk, Bytes.utf8("b" + step));
                        model.put(bk, Bytes.utf8("b" + step));
                    } else {
                        b.delete(bk);
                        model.remove(bk);
                    }
                }
                db.write(b, WriteOptions.NO_SYNC);
            } else if (op < 93) {
                assertArrayEquals(model.get(k), db.get(k).orElse(null), "get " + Bytes.str(k) + " at step " + step);
            } else if (op < 97) {
                byte[] from = key(r.nextInt(600));
                byte[] to = key(r.nextInt(600));
                if (Bytes.compare(from, to) > 0) { byte[] t = from; from = to; to = t; }
                assertScanMatches(db, model.subMap(from, true, to, false), from, to);
            } else if (op < 98) {
                db.compactUntilIdle();
                db.checkInvariants();
            } else if (op < 99) {
                compactions += db.stats().compactions();
                db.close();
                db = LsmEngine.open(fs, "db", Options.small(), Scheduler.INLINE);
            } else {
                db.flush();
            }
        }
        assertScanMatches(db, model, null, null);
        compactions += db.stats().compactions();
        assertTrue(compactions > 0, "expected compactions");
        db.checkInvariants();
        db.close();
    }

    private static void assertScanMatches(LsmEngine db, Map<byte[], byte[]> expected, byte[] from, byte[] to) {
        List<String> got = new ArrayList<>();
        try (KvIterator it = db.scan(from, to, ReadOptions.DEFAULT)) {
            while (it.hasNext()) {
                KvIterator.Entry e = it.next();
                got.add(Bytes.str(e.key()) + "=" + Bytes.str(e.value()));
            }
        }
        List<String> want = new ArrayList<>();
        expected.forEach((k, v) -> want.add(Bytes.str(k) + "=" + Bytes.str(v)));
        assertEquals(want, got);
    }

    @Test
    void snapshotsSurviveCompaction() {
        SimFileSystem fs = new SimFileSystem();
        LsmEngine db = LsmEngine.open(fs, "db", Options.small(), Scheduler.INLINE);
        for (int i = 0; i < 2000; i++) db.put(key(i), Bytes.utf8("old" + i));
        Snapshot snap = db.snapshot();
        for (int i = 0; i < 2000; i++) {
            if (i % 2 == 0) db.delete(key(i));
            else db.put(key(i), Bytes.utf8("new" + i));
        }
        db.compactUntilIdle();
        for (int i = 0; i < 2000; i++) {
            assertEquals("old" + i, Bytes.str(db.get(key(i), new ReadOptions(snap)).orElseThrow()));
            assertEquals(i % 2 == 0 ? null : "new" + i, db.get(key(i)).map(Bytes::str).orElse(null));
        }
        int n = 0;
        try (KvIterator it = db.scan(null, null, new ReadOptions(snap))) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        assertEquals(2000, n);
        snap.close();
        db.compactUntilIdle();
        db.close();
    }

    @Test
    void checkpointOpensAsIndependentDatabase() {
        SimFileSystem fs = new SimFileSystem();
        LsmEngine db = LsmEngine.open(fs, "db", Options.small(), Scheduler.INLINE);
        for (int i = 0; i < 3000; i++) db.put(key(i % 1000), Bytes.utf8("v" + i));
        List<String> files = db.checkpoint("cp");
        assertTrue(files.contains("CURRENT"));
        db.put(key(1), Bytes.utf8("after"));
        LsmEngine copy = LsmEngine.open(fs, "cp", Options.small(), Scheduler.INLINE);
        for (int i = 0; i < 1000; i++) assertEquals("v" + (2000 + i), Bytes.str(copy.get(key(i)).orElseThrow()));
        copy.put(key(2), Bytes.utf8("independent"));
        assertEquals("v2002", Bytes.str(db.get(key(2)).orElseThrow()));
        copy.close();
        db.close();
    }
}
