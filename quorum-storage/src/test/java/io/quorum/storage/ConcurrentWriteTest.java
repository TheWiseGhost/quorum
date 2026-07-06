package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.ExecutorScheduler;
import io.quorum.common.RealFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Group commit and background flush/compaction on real threads and a real file system. */
class ConcurrentWriteTest {
    @Test
    void concurrentSyncWritersAreAllDurableAndVisible(@TempDir Path tmp) throws Exception {
        RealFileSystem fs = new RealFileSystem(tmp);
        try (ExecutorScheduler bg = new ExecutorScheduler("lsm-bg", 1)) {
            LsmEngine db = LsmEngine.open(fs, "db", Options.small(), bg);
            int threads = 8, perThread = 400;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> ts = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                ts.add(Thread.ofVirtual().start(() -> {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            db.write(new WriteBatch().put(Bytes.utf8("t" + id + "-" + i), Bytes.utf8("v" + i)),
                                    new WriteOptions(i % 4 == 0));
                            if (i % 50 == 0) assertTrue(db.get(Bytes.utf8("t" + id + "-" + i)).isPresent());
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }));
            }
            for (Thread t : ts) t.join();
            if (failure.get() != null) throw new AssertionError(failure.get());
            db.compactUntilIdle();
            db.checkInvariants();
            db.close();

            LsmEngine reopened = LsmEngine.open(fs, "db", Options.small(), bg);
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < perThread; i++) {
                    assertEquals("v" + i, Bytes.str(reopened.get(Bytes.utf8("t" + t + "-" + i)).orElseThrow()));
                }
            }
            reopened.close();
        }
    }
}
