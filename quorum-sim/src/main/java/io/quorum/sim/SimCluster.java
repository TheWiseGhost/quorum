package io.quorum.sim;

import io.quorum.common.Rng;
import io.quorum.kv.Request;
import io.quorum.kv.Response;
import io.quorum.raft.ConfState;
import io.quorum.raft.Message;
import io.quorum.raft.MessageCodec;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/** The simulated servers plus the network between them (latency, loss, duplication, partitions). */
final class SimCluster {
    private final SimWorld world;
    private final SimConfig cfg;
    private final Rng rng;
    private final Map<Long, SimNode> nodes = new TreeMap<>();
    private final Set<Long> blocked = new HashSet<>();
    private final InvariantChecker invariants;
    long messagesSent, messagesDropped;

    SimCluster(SimWorld world, SimConfig cfg) {
        this.world = world;
        this.cfg = cfg;
        this.rng = world.rng().split();
        this.invariants = new InvariantChecker(this);
        Rng skew = world.rng().split();
        TreeSet<Long> voters = new TreeSet<>();
        for (long id = 1; id <= cfg.initialVoters(); id++) voters.add(id);
        ConfState bootstrap = new ConfState(voters, new TreeSet<>());
        for (long id = 1; id <= cfg.maxNodes(); id++) {
            double factor = cfg.clockSkew() ? 0.7 + skew.nextDouble() * 0.7 : 1.0;
            SimNode n = new SimNode(id, this, world, factor);
            nodes.put(id, n);
            if (id <= cfg.initialVoters()) {
                n.bootstrap = bootstrap;
                n.member = true;
            }
        }
    }

    void startInitial() {
        for (SimNode n : nodes.values()) if (n.member) n.start();
    }

    SimWorld world() {
        return world;
    }

    SimConfig cfg() {
        return cfg;
    }

    InvariantChecker invariants() {
        return invariants;
    }

    SimNode node(long id) {
        return nodes.get(id);
    }

    Collection<SimNode> nodes() {
        return nodes.values();
    }

    /** God's-eye view: the live leader with the highest term. */
    SimNode leader() {
        SimNode best = null;
        for (SimNode n : nodes.values()) {
            if (n.up() && n.node.raft().isLeader() && (best == null || n.node.raft().term() > best.node.raft().term())) best = n;
        }
        return best;
    }

    // ------------------------------------------------------------------ partitions

    private static long pair(long a, long b) {
        return (a << 32) | b;
    }

    boolean isBlocked(long from, long to) {
        return blocked.contains(pair(from, to));
    }

    void block(long from, long to) {
        blocked.add(pair(from, to));
    }

    void healAll() {
        blocked.clear();
    }

    boolean partitioned() {
        return !blocked.isEmpty();
    }

    // ------------------------------------------------------------------ messaging

    private long latency() {
        return rng.between(cfg.minLatencyUs(), cfg.maxLatencyUs() + 1) * 1_000L;
    }

    /** Raft traffic. Each message is serialized, so the wire codec is exercised on every hop. */
    void send(Message m) {
        messagesSent++;
        int type = m.getClass().getSimpleName().hashCode();
        world.mix(m.from(), m.to(), type, m.term());
        byte[] wire = MessageCodec.encode(m);
        deliverLater(m.from(), m.to(), wire);
        if (rng.chance(cfg.duplicateRate())) deliverLater(m.from(), m.to(), wire);
    }

    private void deliverLater(long from, long to, byte[] wire) {
        if (isBlocked(from, to) || rng.chance(cfg.dropRate())) {
            messagesDropped++;
            return;
        }
        world.schedule(latency(), () -> {
            if (isBlocked(from, to)) {
                messagesDropped++;
                return;
            }
            SimNode n = nodes.get(to);
            if (n != null) n.deliver(MessageCodec.decode(wire));
        });
    }

    /** Client → server → client round trip with latency and loss on both legs. */
    void clientCall(long target, Request req, Consumer<Response> cb) {
        if (rng.chance(cfg.dropRate())) return;
        world.schedule(latency(), () -> nodes.get(target).submit(req, resp -> {
            if (rng.chance(cfg.dropRate())) return;
            world.schedule(latency(), () -> cb.accept(resp));
        }));
    }
}
