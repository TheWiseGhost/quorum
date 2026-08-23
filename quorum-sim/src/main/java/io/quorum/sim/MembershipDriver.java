package io.quorum.sim;

import io.quorum.common.Rng;
import io.quorum.kv.Request;
import io.quorum.kv.Response;
import io.quorum.raft.ConfState;
import io.quorum.raft.RaftNode;

import java.util.List;

/**
 * Periodically grows or shrinks the cluster with single-server changes: a spare joins with an
 * empty disk as a learner, catches up, and is promoted; or a voter is removed and shut down.
 * Every step re-reads the committed configuration, so lost responses and leader changes are
 * harmless (the driver is idempotent).
 */
final class MembershipDriver {
    private final SimCluster cluster;
    private final SimWorld world;
    private final Rng rng;
    private boolean stopped;
    private long adding = 0, removing = 0;
    private long deadline;
    private int token;
    int changes;

    MembershipDriver(SimCluster cluster, Rng rng) {
        this.cluster = cluster;
        this.world = cluster.world();
        this.rng = rng;
    }

    void start() {
        again(rng.between(1_000, 3_000));
    }

    void stop() {
        stopped = true;
    }

    /** Schedules the next step; any earlier pending step is superseded, so there is one chain. */
    private void again(long ms) {
        if (stopped) return;
        int my = ++token;
        world.schedule(ms * 1_000_000L, () -> {
            if (my == token) step();
        });
    }

    private void step() {
        if (stopped) return;
        SimNode leader = cluster.leader();
        if (leader == null) {
            again(100);
            return;
        }
        RaftNode raft = leader.node.raft();
        ConfState conf = raft.conf();
        if (adding == 0 && removing == 0) {
            List<SimNode> spares = cluster.nodes().stream().filter(n -> !n.member).toList();
            if (!spares.isEmpty() && (conf.voters().size() <= 3 || rng.chance(0.5))) {
                SimNode s = spares.get(rng.nextInt(spares.size()));
                s.wipe();
                s.bootstrap = ConfState.EMPTY;
                s.member = true;
                s.start();
                adding = s.id;
                world.log("membership: adding n" + adding);
            } else if (conf.voters().size() > 3) {
                List<Long> voters = List.copyOf(conf.voters());
                removing = voters.get(rng.nextInt(voters.size()));
                world.log("membership: removing n" + removing);
            } else {
                again(rng.between(1_000, 3_000));
                return;
            }
            deadline = world.nowNanos() + 15_000_000_000L;
        }
        if (world.nowNanos() > deadline) { // give up on this change; leave the cluster consistent
            adding = removing = 0;
            again(1_000);
            return;
        }
        if (adding != 0) {
            long s = adding;
            if (conf.voters().contains(s)) {
                changes++;
                adding = 0;
                again(rng.between(2_000, 5_000));
            } else if (conf.learners().contains(s)) {
                if (raft.matchIndex(s) >= raft.commitIndex() - 5) admin(leader, new Request.Promote(s));
                else again(100);
            } else {
                admin(leader, new Request.AddLearner(s));
            }
        } else {
            long v = removing;
            if (!conf.contains(v)) {
                cluster.node(v).stop();
                world.log("membership: n" + v + " removed and stopped");
                changes++;
                removing = 0;
                again(rng.between(2_000, 5_000));
            } else {
                admin(leader, new Request.Remove(v));
            }
        }
    }

    private void admin(SimNode leader, Request req) {
        again(500); // fallback in case the callback never fires (crash, lost leadership)
        int my = token;
        leader.submit(req, (Response r) -> {
            if (my == token) again(r.code() == Response.Code.OK ? 10 : 100);
        });
    }
}
