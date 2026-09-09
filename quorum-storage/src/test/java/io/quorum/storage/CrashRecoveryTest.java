package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.Cancellable;
import io.quorum.common.Rng;
import io.quorum.common.Scheduler;
import io.quorum.common.SimFileSystem;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Crashes the simulated disk at random points. After recovery the database must equal the model
 * after some prefix of the writes that includes every write acknowledged with sync=true.
 */
class CrashRecoveryTest {
    static LongStream seeds() {
        return LongStream.rangeClosed(1, 60);
    }

    /** Background work runs only when the test says so, so crashes land mid-flush too. */
    static final class DeferredScheduler implements Scheduler {
        final ArrayDeque<Runnable> queue = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            queue.add(task);
        }

        @Override
        public Cancellable schedule(long delayNanos, Runnable task) {
            queue.add(task);
            return Cancellable.NONE;
        }

        void runSome(Random r) {
            int n = r.nextInt(queue.size() + 1);
            for (int i = 0; i < n && !queue.isEmpty(); i++) queue.poll().run();
        }
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void recoversExactlyAPrefixContainingAllSyncedWrites(long seed) {
        Random r = new Random(seed);
        Rng crashRng = Rng.seeded(seed);
        SimFileSystem fs = new SimFileSystem();
        List<int[]> history = new ArrayList<>(); // {key, value or -1 for delete}
        int durablePrefix = 0;
        DeferredScheduler bg = new DeferredScheduler();
        LsmEngine db = LsmEngine.open(fs, "db", Options.small(), bg);
        for (int round = 0; round < 6; round++) {
            int ops = 200 + r.nextInt(1500);
            for (int i = 0; i < ops; i++) {
                int k = r.nextInt(300);
                boolean del = r.nextInt(5) == 0;
                boolean sync = r.nextInt(10) == 0;
                int value = history.size();
                if (del) db.write(new WriteBatch().delete(key(k)), new WriteOptions(sync));
                else db.write(new WriteBatch().put(key(k), Bytes.utf8("v" + value)), new WriteOptions(sync));
                history.add(new int[]{k, del ? -1 : value});
                if (sync) durablePrefix = history.size();
                if (r.nextInt(400) == 0) db.compactUntilIdle();
                if (r.nextInt(30) == 0) bg.runSome(r);
            }
            fs.crash(crashRng);
            bg.queue.clear(); // the old process is gone
            db = LsmEngine.open(fs, "db", Options.small(), bg);
            db.checkInvariants();
            int matched = matchingPrefix(db, history, durablePrefix);
            assertTrue(matched >= 0, "seed " + seed + " round " + round + ": state matches no prefix >= " + durablePrefix);
            // Writes beyond the surviving prefix are gone; continue from the recovered state.
            history = new ArrayList<>(history.subList(0, matched));
            durablePrefix = matched;
        }
        db.close();
    }

    private static byte[] key(int k) {
        return Bytes.utf8("key" + k);
    }

    private static Map<Integer, String> read(LsmEngine db) {
        Map<Integer, String> out = new HashMap<>();
        try (KvIterator it = db.scan(null, null, ReadOptions.DEFAULT)) {
            while (it.hasNext()) {
                KvIterator.Entry e = it.next();
                out.put(Integer.parseInt(Bytes.str(e.key()).substring(3)), Bytes.str(e.value()));
            }
        }
        return out;
    }

    /** Longest prefix length >= min whose model equals the database, or -1. */
    private static int matchingPrefix(LsmEngine db, List<int[]> history, int min) {
        Map<Integer, String> actual = read(db);
        Map<Integer, String> model = new HashMap<>();
        int best = -1;
        for (int i = 0; i <= history.size(); i++) {
            if (i > 0) {
                int[] h = history.get(i - 1);
                if (h[1] < 0) model.remove(h[0]);
                else model.put(h[0], "v" + h[1]);
            }
            if (i >= min && model.equals(actual)) best = i;
        }
        return best;
    }
}
