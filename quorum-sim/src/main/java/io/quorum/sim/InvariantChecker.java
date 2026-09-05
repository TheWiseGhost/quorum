package io.quorum.sim;

import io.quorum.common.Bytes;
import io.quorum.kv.KvNode;
import io.quorum.raft.Entry;
import io.quorum.raft.RaftNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Checks Raft's safety properties continuously while the simulation runs:
 * Election Safety, Leader Completeness, State Machine Safety, and (periodically) Log Matching.
 */
final class InvariantChecker implements KvNode.Observer {
    static final class Violation extends RuntimeException {
        Violation(String msg) {
            super(msg);
        }
    }

    private final SimCluster cluster;
    /**
     * index → {entry term, content hash, observed term} for every entry any node has applied
     * (hence committed). The observed term is the applying node's term at that moment, an upper
     * bound on the term in which the entry was committed.
     */
    private final TreeMap<Long, long[]> committed = new TreeMap<>();
    private final Map<Long, Long> leaderOfTerm = new TreeMap<>();
    int leaderChanges;

    InvariantChecker(SimCluster cluster) {
        this.cluster = cluster;
    }

    private static long hash(Entry e) {
        return Arrays.hashCode(e.data()) * 31L + e.type().ordinal();
    }

    @Override
    public void onApply(long nodeId, Entry e) {
        cluster.world().mix(nodeId, e.index(), e.term());
        long[] prev = committed.get(e.index());
        long h = hash(e);
        if (prev == null) {
            committed.put(e.index(), new long[]{e.term(), h, cluster.node(nodeId).node.raft().term()});
        } else if (prev[0] != e.term() || prev[1] != h) {
            throw new Violation("State Machine Safety: node " + nodeId + " applied " + e + " at index " + e.index()
                    + " but another node applied term " + prev[0] + " there");
        }
    }

    @Override
    public void onRoleChange(long nodeId, RaftNode.Role role, long term) {
        cluster.world().log("n" + nodeId + " → " + role + " term " + term);
        if (role != RaftNode.Role.LEADER) return;
        leaderChanges++;
        Long prev = leaderOfTerm.putIfAbsent(term, nodeId);
        if (prev != null && prev != nodeId) {
            throw new Violation("Election Safety: nodes " + prev + " and " + nodeId + " both leader in term " + term);
        }
        RaftNode leader = cluster.node(nodeId).node.raft();
        for (Map.Entry<Long, long[]> c : committed.tailMap(leader.snapshotIndex(), false).entrySet()) {
            // Only entries committed in an earlier term must be present. A leader elected in
            // term 7 after a long pause need not know entries another leader committed in term 10.
            if (c.getValue()[2] >= term) continue;
            long t = leader.termAt(c.getKey());
            if (t != c.getValue()[0]) {
                throw new Violation("Leader Completeness: new leader " + nodeId + " (term " + term + ") has term " + t
                        + " at committed index " + c.getKey() + " (committed term " + c.getValue()[0] + ")");
            }
        }
    }

    /** Log Matching: if two logs share (index, term) they are identical up to that index. */
    void checkLogMatching() {
        List<SimNode> live = cluster.nodes().stream().filter(n -> n.alive && n.node != null).toList();
        for (int a = 0; a < live.size(); a++) {
            for (int b = a + 1; b < live.size(); b++) {
                RaftNode x = live.get(a).node.raft(), y = live.get(b).node.raft();
                Map<Long, Entry> ye = new TreeMap<>();
                for (Entry e : y.entries()) ye.put(e.index(), e);
                List<Entry> xs = x.entries();
                long highestCommon = -1;
                for (Entry e : xs) {
                    Entry o = ye.get(e.index());
                    if (o != null && o.term() == e.term()) highestCommon = e.index();
                }
                for (Entry e : xs) {
                    if (e.index() > highestCommon) break;
                    Entry o = ye.get(e.index());
                    if (o != null && !o.equals(e)) {
                        throw new Violation("Log Matching: nodes " + x.id() + " and " + y.id() + " agree at index "
                                + highestCommon + " but differ at " + e.index() + ": " + e + " vs " + o
                                + " (" + Bytes.str(e.data()) + ")");
                    }
                }
            }
        }
    }

    int committedEntries() {
        return committed.size();
    }
}
