package io.quorum.bench;

import io.quorum.client.QuorumClient;
import io.quorum.common.Bytes;
import io.quorum.server.LocalCluster;
import org.HdrHistogram.Recorder;
import org.HdrHistogram.Histogram;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * YCSB-style workloads against a 3-node cluster (in-process on localhost, or an external
 * cluster via --servers). Uses a fixed number of outstanding requests (closed loop) and
 * reports throughput and latency percentiles. fsync is always on.
 *
 * <pre>
 *   ClusterBench [--workload A|B|C|F|all] [--records 100000] [--ops 200000] [--concurrency 64]
 *                [--servers h:p,h:p,h:p]
 * </pre>
 * A: 50% read / 50% update. B: 95/5. C: read only. F: read-modify-write via CAS.
 */
public final class ClusterBench {
    public static void main(String[] args) throws Exception {
        String workload = "all";
        int records = 100_000, ops = 200_000, concurrency = 64;
        String servers = null;
        boolean fsync = true, pipeline = true, batching = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--no-fsync" -> fsync = false;
                case "--no-pipeline" -> pipeline = false;
                case "--no-batching" -> batching = false;
                case "--workload" -> workload = args[++i];
                case "--records" -> records = Integer.parseInt(args[++i]);
                case "--ops" -> ops = Integer.parseInt(args[++i]);
                case "--concurrency" -> concurrency = Integer.parseInt(args[++i]);
                case "--servers" -> servers = args[++i];
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        LocalCluster cluster = null;
        List<InetSocketAddress> addrs = new ArrayList<>();
        if (servers == null) {
            Path dir = Files.createTempDirectory("quorum-clusterbench");
            boolean f = fsync, p = pipeline, b = batching;
            cluster = new LocalCluster(3, dir, 10, nc -> nc.withFsync(f).withBatching(b)
                    .withRaft(p ? nc.raft() : nc.raft().withMaxInflight(1)));
            cluster.awaitLeader(10_000);
            addrs = cluster.clientAddresses();
            System.out.printf("in-process 3-node cluster on localhost (fsync=%s pipeline=%s batching=%s), data in %s%n",
                    fsync, pipeline, batching, dir);
        } else {
            for (String s : servers.split(",")) {
                int c = s.lastIndexOf(':');
                addrs.add(new InetSocketAddress(s.substring(0, c), Integer.parseInt(s.substring(c + 1))));
            }
        }
        try (QuorumClient client = new QuorumClient(addrs, 30_000)) {
            byte[] value = new byte[100];
            System.out.printf("loading %,d records…%n", records);
            int n = records;
            runClosedLoop("load", n, concurrency, client, (i, r) -> client.put(key(i), value));
            System.out.printf("%n%-10s %12s %10s %10s %10s %10s%n", "workload", "ops/sec", "p50 ms", "p99 ms", "p999 ms", "max ms");
            List<String> wls = workload.equals("all") ? List.of("A", "B", "C", "F") : List.of(workload.toUpperCase());
            for (String wl : wls) {
                double readFraction = switch (wl) {
                    case "A" -> 0.5;
                    case "B" -> 0.95;
                    case "C" -> 1.0;
                    case "F" -> 0.0;
                    default -> throw new IllegalArgumentException(wl);
                };
                Result res = runClosedLoop(wl, ops, concurrency, client, (i, r) -> {
                    byte[] k = key(zipf(r, n));
                    if (wl.equals("F")) {
                        return client.get(k).thenCompose(cur -> client.cas(k, cur, value)).thenApply(x -> null);
                    }
                    if (r.nextDouble() < readFraction) return client.get(k).thenApply(x -> null);
                    return client.put(k, value);
                });
                System.out.printf("%-10s %,12.0f %10.2f %10.2f %10.2f %10.2f%n", wl, res.throughput,
                        res.h.getValueAtPercentile(50) / 1000.0, res.h.getValueAtPercentile(99) / 1000.0,
                        res.h.getValueAtPercentile(99.9) / 1000.0, res.h.getMaxValue() / 1000.0);
            }
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    record Result(double throughput, Histogram h) {}

    interface Op {
        CompletableFuture<?> run(int i, SplittableRandom r);
    }

    static byte[] key(long i) {
        return Bytes.utf8(String.format("user%012d", i));
    }

    /** Approximate Zipfian (θ≈0.99) over [0, n) via inverse-CDF on a power law. */
    static long zipf(SplittableRandom r, int n) {
        double u = r.nextDouble();
        return Math.min(n - 1, (long) (Math.pow(n + 1.0, u) - 1));
    }

    /** Keeps {@code concurrency} requests outstanding; latency is measured per request. */
    static Result runClosedLoop(String name, int ops, int concurrency, QuorumClient client, Op op) throws Exception {
        Recorder rec = new Recorder(3);
        Semaphore inflight = new Semaphore(concurrency);
        SplittableRandom r = new SplittableRandom(name.hashCode());
        long t0 = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            inflight.acquire();
            long start = System.nanoTime();
            op.run(i, r.split()).whenComplete((v, e) -> {
                if (e == null) rec.recordValue(Math.max(1, (System.nanoTime() - start) / 1000));
                inflight.release();
            });
        }
        inflight.acquire(concurrency);
        inflight.release(concurrency);
        double secs = (System.nanoTime() - t0) / 1e9;
        Histogram h = rec.getIntervalHistogram();
        return new Result(h.getTotalCount() / secs, h);
    }
}
