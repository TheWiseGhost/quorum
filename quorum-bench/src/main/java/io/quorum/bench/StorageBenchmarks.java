package io.quorum.bench;

import io.quorum.common.Bytes;
import io.quorum.common.Scheduler;
import io.quorum.common.SimFileSystem;
import io.quorum.storage.LsmEngine;
import io.quorum.storage.Options;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/** Engine-level benchmarks on an in-memory file system (isolates CPU cost from the disk). */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class StorageBenchmarks {
    private static final int KEYS = 200_000;
    private LsmEngine db;
    private final SplittableRandom rnd = new SplittableRandom(1);
    private final byte[] value = new byte[100];
    private long counter;

    private static byte[] key(long i) {
        return Bytes.utf8(String.format("user%012d", i));
    }

    @Setup(Level.Trial)
    public void setup() {
        db = LsmEngine.open(new SimFileSystem(), "db", Options.defaults(), Scheduler.INLINE);
        for (int i = 0; i < KEYS; i++) db.put(key(i), value);
        db.compactUntilIdle();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        db.close();
    }

    @Benchmark
    public void putRandom() {
        db.put(key(rnd.nextInt(KEYS)), value);
    }

    @Benchmark
    public byte[] getRandomHit() {
        return db.get(key(rnd.nextInt(KEYS))).orElse(null);
    }

    @Benchmark
    public byte[] getMiss() {
        return db.get(key(KEYS + (counter++ % KEYS))).orElse(null); // bloom filters short-circuit these
    }
}
