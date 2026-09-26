package io.quorum.bench;

import io.quorum.common.Bytes;
import io.quorum.common.ExecutorScheduler;
import io.quorum.common.RealFileSystem;
import io.quorum.storage.EngineStats;
import io.quorum.storage.KvIterator;
import io.quorum.storage.LsmEngine;
import io.quorum.storage.Options;
import io.quorum.storage.ReadOptions;
import io.quorum.storage.WriteBatch;
import io.quorum.storage.WriteOptions;
import org.HdrHistogram.Histogram;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LevelDB {@code db_bench}-style benchmark of the storage engine on the real file system.
 * Usage: {@code DbBench [--num 1000000] [--value 100] [--threads 8] [--dir /tmp/qdb]}.
 */
public final class DbBench {
    public static void main(String[] args) throws Exception {
        int num = 1_000_000, valueSize = 100, threads = 8;
        Path dir = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--num" -> num = Integer.parseInt(args[++i]);
                case "--value" -> valueSize = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--dir" -> dir = Path.of(args[++i]);
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        if (dir == null) dir = Files.createTempDirectory("quorum-dbbench");
        System.out.printf("DbBench: %,d keys, %d-byte values, %d threads for sync writes, dir %s%n%n", num, valueSize, threads, dir);
        System.out.printf("%-24s %14s %12s %12s %12s%n", "benchmark", "ops/sec", "p50 µs", "p99 µs", "MB/s");
        byte[] value = new byte[valueSize];
        new SplittableRandom(7).nextBytes(value);
        try (ExecutorScheduler bg = new ExecutorScheduler("bench-bg", 2)) {
            RealFileSystem fs = new RealFileSystem(dir);
            LsmEngine db = LsmEngine.open(fs, "fillseq", Options.defaults(), bg);
            int n = num;
            run("fillseq", n, 1, i -> db.put(key(i), value), valueSize);
            db.close();

            LsmEngine rdb = LsmEngine.open(fs, "fillrandom", Options.defaults(), bg);
            SplittableRandom r = new SplittableRandom(42);
            run("fillrandom", n, 1, i -> rdb.put(key(r.nextInt(n)), value), valueSize);
            run("overwrite", n, 1, i -> rdb.put(key(r.nextInt(n)), value), valueSize);
            rdb.compactUntilIdle();
            EngineStats st = rdb.stats();
            run("readrandom", n, 1, i -> rdb.get(key(r.nextInt(n))), valueSize);
            run("readmissing", n, 1, i -> rdb.get(Bytes.utf8("missing" + i)), 0);
            long[] seen = {0};
            long t0 = System.nanoTime();
            try (KvIterator it = rdb.scan(null, null, ReadOptions.DEFAULT)) {
                while (it.hasNext()) {
                    it.next();
                    seen[0]++;
                }
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("%-24s %,14.0f %12s %12s %12.1f%n", "readseq", seen[0] / secs, "-", "-", seen[0] * (16.0 + valueSize) / secs / 1e6);
            System.out.println();
            System.out.println("after fillrandom + overwrite (" + (2 * n) + " writes):");
            System.out.println("  " + st.toString().replace("\n", "\n  "));
            System.out.printf("  space amplification: %.2f (live table bytes / unique user bytes)%n",
                    st.liveTableBytes() / (double) ((long) n * (16 + valueSize)));
            rdb.close();

            LsmEngine sdb = LsmEngine.open(fs, "fillsync", Options.defaults(), bg);
            int syncOps = Math.min(n, 20_000);
            AtomicLong ctr = new AtomicLong();
            run("fillsync (1 thread)", syncOps / 4, 1,
                    i -> sdb.write(new WriteBatch().put(key(ctr.incrementAndGet()), value), WriteOptions.SYNC), valueSize);
            run("fillsync (" + threads + " threads)", syncOps, threads,
                    i -> sdb.write(new WriteBatch().put(key(ctr.incrementAndGet()), value), WriteOptions.SYNC), valueSize);
            sdb.close();
        }
    }

    interface Op {
        void run(int i) throws Exception;
    }

    static byte[] key(long i) {
        return Bytes.utf8(String.format("%016d", i));
    }

    static void run(String name, int ops, int threads, Op op, int valueSize) throws Exception {
        Histogram h = new Histogram(3_600_000_000L, 3);
        long t0 = System.nanoTime();
        if (threads == 1) {
            for (int i = 0; i < ops; i++) {
                long s = System.nanoTime();
                op.run(i);
                h.recordValue(Math.max(1, (System.nanoTime() - s) / 1000));
            }
        } else {
            List<Thread> ts = new ArrayList<>();
            int per = ops / threads;
            for (int t = 0; t < threads; t++) {
                Histogram local = h;
                ts.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (int i = 0; i < per; i++) {
                            long s = System.nanoTime();
                            op.run(i);
                            synchronized (local) {
                                local.recordValue(Math.max(1, (System.nanoTime() - s) / 1000));
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Thread t : ts) t.join();
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        long count = h.getTotalCount();
        System.out.printf("%-24s %,14.0f %12d %12d %12.1f%n", name, count / secs, h.getValueAtPercentile(50),
                h.getValueAtPercentile(99), count * (16.0 + valueSize) / secs / 1e6);
    }
}
