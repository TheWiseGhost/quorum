package io.quorum.bench;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.Bytes;
import io.quorum.common.Crc;
import io.quorum.raft.Entry;
import io.quorum.raft.Message;
import io.quorum.raft.MessageCodec;
import io.quorum.storage.BloomFilter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Hot-path microbenchmarks. Run: {@code ./gradlew :quorum-bench:jmh -PbenchArgs="-f 1"}. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class MicroBenchmarks {
    private byte[] filter;
    private long[] probes;
    private int probeIdx;
    private byte[] block4k;
    private byte[] key16;
    private Message.AppendEntries append;
    private byte[] appendWire;

    @Setup
    public void setup() {
        int n = 100_000;
        long[] hashes = new long[n];
        for (int i = 0; i < n; i++) hashes[i] = BloomFilter.hash(Bytes.utf8("key-" + i));
        filter = BloomFilter.build(hashes, n, 10);
        probes = new long[1024];
        for (int i = 0; i < probes.length; i++) probes[i] = BloomFilter.hash(Bytes.utf8("probe-" + i));
        block4k = new byte[4096];
        for (int i = 0; i < block4k.length; i++) block4k[i] = (byte) (i * 31);
        key16 = Bytes.utf8("user:0000012345");
        List<Entry> ents = new ArrayList<>();
        for (int i = 0; i < 64; i++) ents.add(Entry.normal(7, 1000 + i, new byte[100]));
        append = new Message.AppendEntries(1, 2, 7, 999, 7, ents, 990);
        appendWire = MessageCodec.encode(append);
    }

    @Benchmark
    public boolean bloomProbe() {
        return BloomFilter.mayContain(filter, probes[probeIdx++ & 1023]);
    }

    @Benchmark
    public long bloomHash() {
        return BloomFilter.hash(key16);
    }

    @Benchmark
    public int crc32c4k() {
        return Crc.crc32c(block4k);
    }

    @Benchmark
    public long varintRoundTrip() {
        ByteWriter w = new ByteWriter(64);
        for (long v = 1; v < 1L << 56; v <<= 7) w.putVarLong(v);
        ByteReader r = new ByteReader(w.array(), 0, w.position());
        long sum = 0;
        while (r.hasRemaining()) sum += r.getVarLong();
        return sum;
    }

    @Benchmark
    public byte[] encodeAppendEntries64() {
        return MessageCodec.encode(append);
    }

    @Benchmark
    public Message decodeAppendEntries64() {
        return MessageCodec.decode(appendWire);
    }
}
