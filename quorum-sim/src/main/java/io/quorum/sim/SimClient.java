package io.quorum.sim;

import io.quorum.check.History;
import io.quorum.check.Op;
import io.quorum.common.Bytes;
import io.quorum.common.Cancellable;
import io.quorum.common.Rng;
import io.quorum.kv.Command;
import io.quorum.kv.Request;
import io.quorum.kv.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A closed-loop client: one operation at a time on a small key space (to force contention).
 * Each operation is retried with the same (clientId, seq) until it succeeds or its deadline
 * passes; a write that gives up is recorded as indeterminate.
 */
final class SimClient {
    private static final long ATTEMPT_TIMEOUT = 400_000_000L;
    private static final long OP_DEADLINE = 6_000_000_000L;

    private final int index;
    private final long sessionId;
    private final SimCluster cluster;
    private final SimWorld world;
    private final History history;
    private final Rng rng;
    private final int keys;
    private final Map<String, Long> lastSeen = new HashMap<>();
    private long seq;
    private long counter;
    private long leaderGuess;
    private int token;
    private boolean stopped;
    private boolean busy;
    int completed;

    SimClient(int index, SimCluster cluster, History history, Rng rng, int keys) {
        this.index = index;
        this.cluster = cluster;
        this.world = cluster.world();
        this.history = history;
        this.rng = rng;
        this.keys = keys;
        this.sessionId = rng.nextLong();
        this.leaderGuess = 1;
    }

    boolean idle() {
        return !busy;
    }

    void stop() {
        stopped = true;
    }

    void start() {
        world.schedule(rng.between(0, 20_000_000), this::nextOp);
    }

    private record Planned(Request req, History.Pending pending, Op.Kind kind, String key) {}

    private void nextOp() {
        if (stopped) {
            busy = false;
            return;
        }
        busy = true;
        String key = "k" + rng.nextInt(keys);
        byte[] k = Bytes.utf8(key);
        int roll = rng.nextInt(100);
        Planned p;
        if (roll < 40) {
            p = new Planned(new Request.Get(k), history.invoke(index, Op.Kind.GET, key, null, null, world.nowNanos()), Op.Kind.GET, key);
        } else if (roll < 75) {
            long v = unique();
            p = new Planned(new Request.Write(new Command.Put(sessionId, ++seq, k, Bytes.longBE(v))),
                    history.invoke(index, Op.Kind.PUT, key, v, null, world.nowNanos()), Op.Kind.PUT, key);
        } else if (roll < 90) {
            long v = unique();
            Long expect = lastSeen.get(key);
            p = new Planned(new Request.Write(new Command.Cas(sessionId, ++seq, k, expect == null ? null : Bytes.longBE(expect), Bytes.longBE(v))),
                    history.invoke(index, Op.Kind.CAS, key, v, expect, world.nowNanos()), Op.Kind.CAS, key);
        } else {
            p = new Planned(new Request.Write(new Command.Delete(sessionId, ++seq, k)),
                    history.invoke(index, Op.Kind.DELETE, key, null, null, world.nowNanos()), Op.Kind.DELETE, key);
        }
        attempt(p, world.nowNanos() + OP_DEADLINE);
    }

    private long unique() {
        return (index + 1L) * 1_000_000_000L + ++counter;
    }

    private void attempt(Planned p, long deadline) {
        if (world.nowNanos() > deadline) {
            if (p.kind() == Op.Kind.GET) p.pending().fail();
            else p.pending().info(world.nowNanos());
            world.schedule(rng.between(0, 5_000_000), this::nextOp);
            return;
        }
        int my = ++token;
        long target = pickTarget();
        Cancellable timeout = world.schedule(ATTEMPT_TIMEOUT, () -> {
            if (token != my) return;
            leaderGuess = 0;
            attempt(p, deadline);
        });
        cluster.clientCall(target, p.req(), resp -> {
            if (token != my) return; // a late answer to an abandoned attempt
            token++;
            timeout.cancel();
            handle(p, resp, deadline);
        });
    }

    private long pickTarget() {
        if (leaderGuess > 0 && cluster.node(leaderGuess).member) return leaderGuess;
        List<SimNode> members = cluster.nodes().stream().filter(n -> n.member).toList();
        return members.get(rng.nextInt(members.size())).id;
    }

    private void handle(Planned p, Response resp, long deadline) {
        switch (resp.code()) {
            case OK -> {
                long now = world.nowNanos();
                switch (p.kind()) {
                    case GET -> {
                        Long v = resp.value() == null ? null : Bytes.getLongBE(resp.value(), 0);
                        p.pending().ok(now, v, false);
                        lastSeen.put(p.key(), v);
                    }
                    case CAS -> {
                        p.pending().ok(now, resp.value() == null ? null : Bytes.getLongBE(resp.value(), 0), resp.ok());
                        Command.Cas c = (Command.Cas) ((Request.Write) p.req()).command();
                        if (resp.ok()) lastSeen.put(p.key(), Bytes.getLongBE(c.newValue(), 0));
                        else lastSeen.put(p.key(), resp.value() == null ? null : Bytes.getLongBE(resp.value(), 0));
                    }
                    case PUT -> {
                        p.pending().ok(now, null, false);
                        Command.Put put = (Command.Put) ((Request.Write) p.req()).command();
                        lastSeen.put(p.key(), Bytes.getLongBE(put.value(), 0));
                    }
                    case DELETE -> {
                        p.pending().ok(now, null, false);
                        lastSeen.put(p.key(), null);
                    }
                }
                completed++;
                world.schedule(rng.between(0, 3_000_000), this::nextOp);
            }
            case NOT_LEADER -> {
                leaderGuess = resp.leaderHint();
                world.schedule(rng.between(1_000_000, 10_000_000), () -> attempt(p, deadline));
            }
            case RETRY, ERROR -> {
                leaderGuess = 0;
                world.schedule(rng.between(2_000_000, 20_000_000), () -> attempt(p, deadline));
            }
        }
    }

    /** One final read per key with a long deadline; used to check liveness after healing. */
    void finalRead(String key, Runnable onDone, Runnable onTimeout) {
        History.Pending pending = history.invoke(index, Op.Kind.GET, key, null, null, world.nowNanos());
        finalAttempt(key, new Request.Get(Bytes.utf8(key)), pending, world.nowNanos() + 20_000_000_000L, onDone, onTimeout);
    }

    /** One final write: the cluster must also accept new commands after healing. */
    void finalWrite(String key, Runnable onDone, Runnable onTimeout) {
        long v = unique();
        History.Pending pending = history.invoke(index, Op.Kind.PUT, key, v, null, world.nowNanos());
        Request req = new Request.Write(new Command.Put(sessionId, ++seq, Bytes.utf8(key), Bytes.longBE(v)));
        finalAttempt(key, req, pending, world.nowNanos() + 20_000_000_000L, onDone, () -> {
            pending.info(world.nowNanos());
            onTimeout.run();
        });
    }

    private void finalAttempt(String key, Request req, History.Pending pending, long deadline, Runnable onDone,
                              Runnable onTimeout) {
        if (world.nowNanos() > deadline) {
            pending.fail();
            onTimeout.run();
            return;
        }
        int my = ++token;
        Cancellable timeout = world.schedule(ATTEMPT_TIMEOUT, () -> {
            if (token != my) return;
            leaderGuess = 0;
            finalAttempt(key, req, pending, deadline, onDone, onTimeout);
        });
        long target = pickTarget();
        cluster.clientCall(target, req, resp -> {
            if (token != my) return;
            token++;
            timeout.cancel();
            if (resp.code() == Response.Code.OK) {
                Long out = req instanceof Request.Get && resp.value() != null ? Bytes.getLongBE(resp.value(), 0) : null;
                pending.ok(world.nowNanos(), out, false);
                onDone.run();
            } else {
                leaderGuess = resp.code() == Response.Code.NOT_LEADER ? resp.leaderHint() : 0;
                world.schedule(5_000_000, () -> finalAttempt(key, req, pending, deadline, onDone, onTimeout));
            }
        });
    }
}
