package io.quorum.sim;

import io.quorum.check.History;
import io.quorum.check.HistoryVisualizer;
import io.quorum.check.LinearizabilityChecker;
import io.quorum.common.Bytes;
import io.quorum.common.Rng;
import io.quorum.kv.KvStateMachine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * One deterministic run of a whole cluster under faults, followed by the safety and liveness
 * checks. Same seed + same config ⇒ same event sequence ⇒ same result.
 */
public final class Simulation {
    public record Result(long seed, boolean ok, String failure, String config, int ops, int infoOps, long events,
                         long simMillis, long traceHash, int leaderChanges, int faults, int membershipChanges,
                         long snapshots, long messages, long dropped, LinearizabilityChecker.Verdict linearizable,
                         String failureHtml, List<String> trace) {}

    private Simulation() {}

    public static Result run(long seed, SimConfig cfg, boolean keepTrace) {
        SimWorld world = new SimWorld(seed, keepTrace);
        SimCluster cluster = new SimCluster(world, cfg);
        History history = new History();
        Rng rng = world.rng().split();
        List<SimClient> clients = new ArrayList<>();
        Nemesis nemesis = new Nemesis(cluster, rng.split());
        MembershipDriver membership = new MembershipDriver(cluster, rng.split());
        String failure = null;
        String html = null;
        LinearizabilityChecker.Verdict verdict = null;
        long durationNanos = cfg.durationMs() * 1_000_000L;
        try {
            cluster.startInitial();
            for (int i = 0; i < cfg.clients(); i++) {
                SimClient c = new SimClient(i, cluster, history, rng.split(), cfg.keys());
                clients.add(c);
                world.schedule(50_000_000L, c::start);
            }
            world.schedule(300_000_000L, nemesis::start);
            if (cfg.membership()) membership.start();
            schedulePeriodicChecks(world, cluster, durationNanos);

            world.runUntil(durationNanos);

            // Recovery phase: stop faults, heal everything, let in-flight operations finish.
            nemesis.stopAndHeal();
            membership.stop();
            clients.forEach(SimClient::stop);
            world.runUntil(() -> clients.stream().allMatch(SimClient::idle), world.nowNanos() + 15_000_000_000L);

            // Liveness: after healing, every key must become readable.
            int[] done = {0};
            boolean[] timedOut = {false};
            SimClient finalClient = new SimClient(cfg.clients(), cluster, history, rng.split(), cfg.keys());
            Runnable[] readNext = new Runnable[1];
            readNext[0] = () -> {
                if (done[0] < cfg.keys()) {
                    finalClient.finalRead("k" + done[0], () -> {
                        done[0]++;
                        readNext[0].run();
                    }, () -> timedOut[0] = true);
                }
            };
            // A write first (it also commits an entry in the new leader's term), then every key.
            world.schedule(1_000_000L, () -> finalClient.finalWrite("k0", readNext[0], () -> timedOut[0] = true));
            world.runUntil(() -> done[0] == cfg.keys() || timedOut[0], world.nowNanos() + 25_000_000_000L);
            if (done[0] != cfg.keys()) {
                failure = "Liveness: cluster did not serve a write and reads within 20s after all faults were healed" + clusterState(cluster);
            }
            if (failure == null) {
                cluster.invariants().checkLogMatching();
                failure = checkReplicasConverge(world, cluster);
            }
        } catch (InvariantChecker.Violation v) {
            failure = v.getMessage();
        } catch (RuntimeException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String[] lines = sw.toString().split("\n");
            failure = "Exception: " + String.join("\n", java.util.Arrays.copyOf(lines, Math.min(lines.length, 12)));
        }

        if (failure == null || failure.startsWith("Liveness")) {
            LinearizabilityChecker.Result lr = new LinearizabilityChecker().check(history.ops());
            verdict = lr.verdict();
            if (lr.verdict() == LinearizabilityChecker.Verdict.ILLEGAL) {
                LinearizabilityChecker.KeyResult bad = lr.firstFailure();
                failure = "Linearizability: history for key " + bad.key() + " (" + bad.ops().size()
                        + " ops) has no valid linearization";
                html = HistoryVisualizer.render("Seed " + seed + ": non-linearizable history", bad);
            }
        }
        int info = (int) history.ops().stream().filter(o -> o.status() == io.quorum.check.Op.Status.INFO).count();
        long snaps = cluster.nodes().stream().filter(n -> n.node != null).mapToLong(n -> n.node.snapshotsTaken()).sum();
        return new Result(seed, failure == null, failure, cfg.describe(), history.size(), info, world.eventsRun(),
                world.nowMillis(), world.traceHash(), cluster.invariants().leaderChanges, nemesis.faults,
                membership.changes, snaps, cluster.messagesSent, cluster.messagesDropped, verdict, html, world.trace());
    }

    static String clusterState(SimCluster cluster) {
        StringBuilder sb = new StringBuilder();
        for (SimNode n : cluster.nodes()) {
            if (n.node == null) continue;
            sb.append("\n  n").append(n.id).append(n.alive ? "" : " (down)").append(n.paused ? " (paused)" : "")
                    .append(n.member ? "" : " (not member)").append(' ').append(n.node.raft())
                    .append(" applied=").append(n.node.stateMachine().appliedIndex());
            if (n.node.raft().isLeader()) sb.append("\n     progress: ").append(n.node.raft().progressSummary());
        }
        return sb.toString();
    }

    private static void schedulePeriodicChecks(SimWorld world, SimCluster cluster, long until) {
        world.schedule(1_000_000_000L, new Runnable() {
            @Override
            public void run() {
                cluster.invariants().checkLogMatching();
                if (world.nowNanos() < until) world.schedule(1_000_000_000L, this);
            }
        });
    }

    /** Once healed and caught up, every replica must hold byte-identical data. */
    private static String checkReplicasConverge(SimWorld world, SimCluster cluster) {
        SimNode leader = cluster.leader();
        if (leader == null) return null;
        long target = leader.node.raft().commitIndex();
        List<SimNode> voters = cluster.nodes().stream()
                .filter(n -> n.alive && leader.node.raft().conf().voters().contains(n.id)).toList();
        boolean caughtUp = world.runUntil(() -> voters.stream().allMatch(n -> n.node.stateMachine().appliedIndex() >= target),
                world.nowNanos() + 10_000_000_000L);
        if (!caughtUp) return "Liveness: voters did not catch up to commit index " + target + " after healing";
        List<KvStateMachine.Row> reference = null;
        long refId = 0;
        for (SimNode n : voters) {
            if (n.node.stateMachine().appliedIndex() != voters.getFirst().node.stateMachine().appliedIndex()) continue;
            List<KvStateMachine.Row> rows = n.node.stateMachine().scan(null, null, Integer.MAX_VALUE);
            if (reference == null) {
                reference = rows;
                refId = n.id;
            } else if (!sameRows(reference, rows)) {
                return "Replica divergence: n" + refId + " and n" + n.id + " differ at applied index "
                        + n.node.stateMachine().appliedIndex() + "\n  n" + refId + ": " + describe(reference)
                        + "\n  n" + n.id + ": " + describe(rows) + clusterState(cluster);
            }
        }
        return null;
    }

    private static String describe(List<KvStateMachine.Row> rows) {
        StringBuilder sb = new StringBuilder();
        for (KvStateMachine.Row r : rows) {
            sb.append(Bytes.str(r.key())).append('=').append(r.value().length == 8 ? Bytes.getLongBE(r.value(), 0) : -1).append(' ');
        }
        return sb.toString();
    }

    private static boolean sameRows(List<KvStateMachine.Row> a, List<KvStateMachine.Row> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (Bytes.compare(a.get(i).key(), b.get(i).key()) != 0 || Bytes.compare(a.get(i).value(), b.get(i).value()) != 0) return false;
        }
        return true;
    }

    static Path writeFailureArtifact(Result r) {
        if (r.failureHtml() == null) return null;
        try {
            Path dir = Path.of("sim-failures");
            Files.createDirectories(dir);
            Path p = dir.resolve("seed-" + r.seed() + ".html");
            Files.writeString(p, r.failureHtml());
            return p;
        } catch (IOException e) {
            return null;
        }
    }
}
