package io.quorum.net;

import io.quorum.kv.Response;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/** Request counters and a log-bucketed latency histogram (Prometheus exposition format). */
public final class RequestMetrics {
    /** Bucket upper bounds in microseconds: 50µs … ~1.6s, doubling. */
    private static final long[] BOUNDS_US = new long[16];

    static {
        long b = 50;
        for (int i = 0; i < BOUNDS_US.length; i++, b *= 2) BOUNDS_US[i] = b;
    }

    private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
    private final Map<String, AtomicLongArray> latency = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> latencySumUs = new ConcurrentHashMap<>();

    public void record(String kind, Response.Code code, long nanos) {
        counts.computeIfAbsent(kind + "\u0000" + code, k -> new LongAdder()).increment();
        if (code != Response.Code.OK) return;
        long us = nanos / 1_000;
        AtomicLongArray h = latency.computeIfAbsent(kind, k -> new AtomicLongArray(BOUNDS_US.length + 1));
        int i = 0;
        while (i < BOUNDS_US.length && us > BOUNDS_US[i]) i++;
        h.incrementAndGet(i);
        latencySumUs.computeIfAbsent(kind, k -> new LongAdder()).add(us);
    }

    public void appendPrometheus(StringBuilder sb) {
        sb.append("# TYPE quorum_requests_total counter\n");
        counts.forEach((k, v) -> {
            String[] parts = k.split("\u0000");
            sb.append("quorum_requests_total{kind=\"").append(parts[0]).append("\",code=\"").append(parts[1])
                    .append("\"} ").append(v.sum()).append('\n');
        });
        sb.append("# TYPE quorum_request_latency_seconds histogram\n");
        latency.forEach((kind, h) -> {
            long cumulative = 0;
            for (int i = 0; i < BOUNDS_US.length; i++) {
                cumulative += h.get(i);
                sb.append("quorum_request_latency_seconds_bucket{kind=\"").append(kind).append("\",le=\"")
                        .append(BOUNDS_US[i] / 1e6).append("\"} ").append(cumulative).append('\n');
            }
            cumulative += h.get(BOUNDS_US.length);
            sb.append("quorum_request_latency_seconds_bucket{kind=\"").append(kind).append("\",le=\"+Inf\"} ")
                    .append(cumulative).append('\n');
            sb.append("quorum_request_latency_seconds_count{kind=\"").append(kind).append("\"} ").append(cumulative).append('\n');
            sb.append("quorum_request_latency_seconds_sum{kind=\"").append(kind).append("\"} ")
                    .append(latencySumUs.get(kind).sum() / 1e6).append('\n');
        });
    }
}
