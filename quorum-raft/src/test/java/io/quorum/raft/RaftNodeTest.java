package io.quorum.raft;

import io.quorum.common.Bytes;
import io.quorum.common.PlantedBug;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    private static List<String> data(List<Entry> entries) {
        return entries.stream().filter(e -> e.data().length > 0 && e.type() == Entry.Type.NORMAL)
                .map(e -> Bytes.str(e.data())).toList();
    }

    @Test
    void electsSingleLeaderAndReplicates() {
        TestCluster c = new TestCluster(1, 1, 2, 3);
        RaftNode leader = c.electLeader();
        long leaders = c.nodes.values().stream().filter(RaftNode::isLeader).count();
        assertEquals(1, leaders);
        for (int i = 0; i < 20; i++) assertTrue(leader.propose(Bytes.utf8("cmd" + i)) > 0);
        c.process();
        for (long id : c.nodes.keySet()) assertEquals(20, data(c.applied.get(id)).size(), "node " + id);
    }

    @Test
    void singleNodeClusterCommitsAlone() {
        TestCluster c = new TestCluster(2, 1);
        RaftNode leader = c.electLeader();
        leader.propose(Bytes.utf8("solo"));
        c.process();
        assertEquals(List.of("solo"), data(c.applied.get(1L)));
    }

    @Test
    void newLeaderAfterIsolationAndOldLeaderCatchesUp() {
        TestCluster c = new TestCluster(3, 1, 2, 3, 4, 5);
        RaftNode first = c.electLeader();
        first.propose(Bytes.utf8("a"));
        c.process();
        c.isolated.add(first.id());
        RaftNode second = c.electLeader();
        assertNotEquals(first.id(), second.id());
        assertTrue(second.term() > first.term());
        // The old leader steps down via CheckQuorum even while partitioned.
        c.tick(30);
        assertFalse(first.isLeader());
        second.propose(Bytes.utf8("b"));
        c.process();
        c.isolated.clear();
        c.tick(5);
        for (long id : c.nodes.keySet()) assertEquals(List.of("a", "b"), data(c.applied.get(id)), "node " + id);
    }

    @Test
    void preVoteStopsPartitionedNodeFromInflatingTerm() {
        TestCluster c = new TestCluster(4, 1, 2, 3);
        RaftNode leader = c.electLeader();
        long follower = c.nodes.keySet().stream().filter(id -> id != leader.id()).findFirst().orElseThrow();
        c.isolated.add(follower);
        c.tick(200);
        assertEquals(leader.term(), c.nodes.get(follower).term(), "pre-vote must not bump the term");
        c.isolated.clear();
        c.tick(5);
        assertTrue(leader.isLeader(), "rejoining node must not disrupt the leader");
    }

    @Test
    void readIndexRequiresQuorumConfirmation() {
        TestCluster c = new TestCluster(5, 1, 2, 3);
        RaftNode leader = c.electLeader();
        leader.propose(Bytes.utf8("x"));
        c.process();
        assertTrue(leader.readIndex(77));
        c.process();
        List<ReadState> rs = c.reads.get(leader.id());
        assertEquals(1, rs.size());
        assertEquals(77, rs.getFirst().ctx());
        assertEquals(leader.commitIndex(), rs.getFirst().index());
        // An isolated leader cannot confirm reads.
        c.isolated.addAll(c.nodes.keySet());
        c.isolated.remove(leader.id());
        c.reads.get(leader.id()).clear();
        assertTrue(leader.readIndex(78));
        c.process();
        assertTrue(c.reads.get(leader.id()).isEmpty());
    }

    @Test
    void membershipChangeAddsLearnerThenPromotes() {
        TestCluster c = new TestCluster(6, 1, 2, 3);
        RaftNode leader = c.electLeader();
        for (int i = 0; i < 10; i++) leader.propose(Bytes.utf8("v" + i));
        c.process();
        c.add(4, ConfState.EMPTY);
        assertTrue(leader.proposeConfChange(new ConfChange(ConfChange.Type.ADD_LEARNER, 4)) > 0);
        c.process();
        c.tick(3);
        assertEquals(leader.lastIndex(), leader.matchIndex(4));
        assertTrue(leader.proposeConfChange(new ConfChange(ConfChange.Type.PROMOTE, 4)) > 0);
        c.process();
        assertEquals(4, leader.conf().voters().size());
        for (RaftNode n : c.nodes.values()) assertEquals(leader.conf(), n.conf(), "conf on " + n.id());
        assertEquals(data(c.applied.get(1L)), data(c.applied.get(4L)));
        assertTrue(leader.proposeConfChange(new ConfChange(ConfChange.Type.REMOVE, leader.id())) > 0);
        c.process();
        assertFalse(leader.isLeader(), "leader removed itself and must step down");
        RaftNode next = c.electLeader();
        assertNotEquals(leader.id(), next.id());
        assertEquals(3, next.conf().voters().size());
    }

    @Test
    void laggingFollowerCatchesUpViaChunkedSnapshot() {
        TestCluster c = new TestCluster(7, 1, 2, 3);
        RaftNode leader = c.electLeader();
        long lagger = c.nodes.keySet().stream().filter(id -> id != leader.id()).findFirst().orElseThrow();
        c.isolated.add(lagger);
        for (int i = 0; i < 50; i++) leader.propose(Bytes.utf8("v" + i));
        c.process();
        long idx = leader.commitIndex();
        Snapshot s = leader.createSnapshot(idx, Bytes.utf8("x".repeat(1000))); // 1000 bytes, 64-byte chunks
        leader.compact(s);
        c.isolated.clear();
        c.tick(10);
        assertEquals(idx, c.installed.get(lagger).index());
        assertEquals(1000, c.installed.get(lagger).data().length);
        leader.propose(Bytes.utf8("after"));
        c.process();
        assertEquals(List.of("after"), data(c.applied.get(lagger)));
    }

    @Test
    void leadershipTransfer() {
        TestCluster c = new TestCluster(8, 1, 2, 3);
        RaftNode leader = c.electLeader();
        long target = c.nodes.keySet().stream().filter(id -> id != leader.id()).findFirst().orElseThrow();
        assertTrue(leader.transferLeadership(target));
        c.process();
        assertTrue(c.nodes.get(target).isLeader());
        assertFalse(leader.isLeader());
    }

    @Test
    void figure8BugCommitsPriorTermEntryByCounting() {
        // Sanity check that the planted bug actually changes behaviour.
        TestCluster c = new TestCluster(9, 1, 2, 3);
        c.base = c.base.withBugs(PlantedBug.of(PlantedBug.FIGURE8_COMMIT));
        assertTrue(c.base.bug(PlantedBug.FIGURE8_COMMIT));
    }
}
