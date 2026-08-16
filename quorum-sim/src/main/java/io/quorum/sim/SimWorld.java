package io.quorum.sim;

import io.quorum.common.Cancellable;
import io.quorum.common.Clock;
import io.quorum.common.Rng;
import io.quorum.common.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.BooleanSupplier;

/**
 * Single-threaded discrete-event scheduler in virtual time. Every node, disk, network link,
 * client and fault injector schedules work here, so a whole cluster run is a deterministic
 * function of the seed. Events at the same instant run in insertion order.
 */
public final class SimWorld implements Clock {
    private static final class Event implements Comparable<Event> {
        final long time;
        final long seq;
        final Runnable task;
        boolean cancelled;

        Event(long time, long seq, Runnable task) {
            this.time = time;
            this.seq = seq;
            this.task = task;
        }

        @Override
        public int compareTo(Event o) {
            return time != o.time ? Long.compare(time, o.time) : Long.compare(seq, o.seq);
        }
    }

    private final PriorityQueue<Event> queue = new PriorityQueue<>();
    private final Rng rng;
    private long now;
    private long seq;
    private long eventsRun;
    private long traceHash = 0xcbf29ce484222325L;
    private final List<String> trace;

    public SimWorld(long seed, boolean keepTrace) {
        this.rng = Rng.seeded(seed);
        this.trace = keepTrace ? new ArrayList<>() : null;
    }

    public Rng rng() {
        return rng;
    }

    @Override
    public long nowNanos() {
        return now;
    }

    public long nowMillis() {
        return now / 1_000_000;
    }

    public long eventsRun() {
        return eventsRun;
    }

    public long traceHash() {
        return traceHash;
    }

    public List<String> trace() {
        return trace;
    }

    /** Mixes an observation into the run fingerprint (FNV-1a over longs). */
    public void mix(long... values) {
        for (long v : values) {
            traceHash ^= v;
            traceHash *= 0x100000001b3L;
        }
    }

    public void log(String line) {
        if (trace != null) trace.add(String.format("%8.3fms %s", now / 1e6, line));
    }

    public Cancellable schedule(long delayNanos, Runnable task) {
        Event e = new Event(now + Math.max(0, delayNanos), seq++, task);
        queue.add(e);
        return () -> e.cancelled = true;
    }

    /** A scheduler whose tasks are silently dropped once {@code alive} turns false. */
    public Scheduler scheduler(BooleanSupplier alive, long maxJitterNanos) {
        return new Scheduler() {
            @Override
            public void execute(Runnable task) {
                long jitter = maxJitterNanos > 0 ? rng.between(0, maxJitterNanos) : 0;
                schedule(jitter, () -> {
                    if (alive.getAsBoolean()) task.run();
                });
            }

            @Override
            public Cancellable schedule(long delayNanos, Runnable task) {
                return SimWorld.this.schedule(delayNanos, () -> {
                    if (alive.getAsBoolean()) task.run();
                });
            }
        };
    }

    public boolean runOne() {
        Event e = queue.poll();
        if (e == null) return false;
        now = e.time;
        if (e.cancelled) return true;
        eventsRun++;
        mix(e.time, e.seq);
        e.task.run();
        return true;
    }

    public void runUntil(long timeNanos) {
        while (!queue.isEmpty() && queue.peek().time <= timeNanos) runOne();
        now = Math.max(now, timeNanos);
    }

    /** Runs until {@code done} or the deadline; returns whether {@code done} became true. */
    public boolean runUntil(BooleanSupplier done, long deadlineNanos) {
        while (!done.getAsBoolean()) {
            if (queue.isEmpty() || queue.peek().time > deadlineNanos) {
                now = Math.max(now, deadlineNanos);
                return done.getAsBoolean();
            }
            runOne();
        }
        return true;
    }
}
