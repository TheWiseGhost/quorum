package io.quorum.raft;

import io.quorum.common.Rng;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Minimal in-memory driver for unit-testing {@link RaftNode} with a perfect (but partitionable) network. */
final class TestCluster {
    final Map<Long, RaftNode> nodes = new TreeMap<>();
    final Map<Long, List<Entry>> applied = new HashMap<>();
    final Map<Long, List<ReadState>> reads = new HashMap<>();
    final Map<Long, Snapshot> installed = new HashMap<>();
    final ArrayDeque<Message> network = new ArrayDeque<>();
    final Set<Long> isolated = new HashSet<>();
    final Rng rng;
    RaftConfig base = RaftConfig.defaults(0);

    TestCluster(long seed, long... ids) {
        rng = Rng.seeded(seed);
        for (long id : ids) add(id, ConfState.of(ids));
    }

    RaftNode add(long id, ConfState bootstrap) {
        RaftConfig c = new RaftConfig(id, base.electionTicks(), base.heartbeatTicks(), base.maxInflight(),
                base.maxEntriesPerMessage(), base.maxBytesPerMessage(), 64, base.preVote(), base.checkQuorum(), base.bugs());
        RaftNode n = new RaftNode(c, rng.split(), bootstrap, RaftNode.Persisted.fresh());
        nodes.put(id, n);
        applied.put(id, new ArrayList<>());
        reads.put(id, new ArrayList<>());
        return n;
    }

    void process() {
        boolean progress = true;
        while (progress) {
            progress = false;
            for (RaftNode n : nodes.values()) {
                while (n.hasReady()) {
                    progress = true;
                    Ready rd = n.ready();
                    network.addAll(rd.sendNow());
                    network.addAll(rd.sendAfterPersist());
                    if (rd.snapshot() != null) {
                        installed.put(n.id(), rd.snapshot());
                        applied.get(n.id()).clear();
                    }
                    applied.get(n.id()).addAll(rd.committed());
                    reads.get(n.id()).addAll(rd.readStates());
                    n.advance(rd);
                }
            }
            while (!network.isEmpty()) {
                progress = true;
                Message m = network.poll();
                if (isolated.contains(m.from()) || isolated.contains(m.to())) continue;
                RaftNode to = nodes.get(m.to());
                if (to != null) to.step(m);
            }
        }
    }

    void tick(int n) {
        for (int i = 0; i < n; i++) {
            for (RaftNode node : nodes.values()) node.tick();
            process();
        }
    }

    RaftNode leader() {
        RaftNode found = null;
        for (RaftNode n : nodes.values()) {
            if (n.isLeader() && !isolated.contains(n.id()) && (found == null || n.term() > found.term())) found = n;
        }
        return found;
    }

    RaftNode electLeader() {
        for (int i = 0; i < 200; i++) {
            tick(1);
            RaftNode l = leader();
            if (l != null) return l;
        }
        throw new AssertionError("no leader elected");
    }
}
