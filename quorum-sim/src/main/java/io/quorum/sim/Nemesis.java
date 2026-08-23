package io.quorum.sim;

import io.quorum.common.Cancellable;
import io.quorum.common.Rng;

import java.util.ArrayList;
import java.util.List;

/** Injects faults on a random schedule: crashes, partitions and process pauses. */
final class Nemesis {
    private final SimCluster cluster;
    private final SimWorld world;
    private final SimConfig cfg;
    private final Rng rng;
    private Cancellable next = Cancellable.NONE;
    private boolean stopped;
    int faults;

    Nemesis(SimCluster cluster, Rng rng) {
        this.cluster = cluster;
        this.world = cluster.world();
        this.cfg = cluster.cfg();
        this.rng = rng;
    }

    void start() {
        scheduleNext();
    }

    private void scheduleNext() {
        long interval = cfg.faultIntervalMs() * 1_000_000L;
        next = world.schedule(rng.between(interval / 2, interval * 3 / 2), () -> {
            if (stopped) return;
            inject();
            scheduleNext();
        });
    }

    private List<SimNode> members() {
        return cluster.nodes().stream().filter(n -> n.member).toList();
    }

    private void inject() {
        List<Runnable> options = new ArrayList<>();
        if (cfg.crashes()) options.add(this::crashOne);
        if (cfg.partitions()) options.add(this::partition);
        if (cfg.pauses()) options.add(this::pauseOne);
        if (options.isEmpty()) return;
        options.get(rng.nextInt(options.size())).run();
        faults++;
    }

    private void crashOne() {
        List<SimNode> up = members().stream().filter(n -> n.alive).toList();
        if (up.isEmpty()) return;
        // Bias towards the leader: crashing it exercises elections and log repair.
        SimNode leader = cluster.leader();
        SimNode victim = leader != null && rng.chance(0.4) ? leader : up.get(rng.nextInt(up.size()));
        victim.crash();
        world.schedule(rng.between(50, 2_500) * 1_000_000L, () -> {
            if (!stopped && victim.member && !victim.alive) victim.start();
        });
    }

    private void partition() {
        List<SimNode> ms = members();
        if (ms.size() < 2) return;
        cluster.healAll();
        int kind = rng.nextInt(4);
        SimNode leader = cluster.leader();
        switch (kind) {
            case 0 -> { // isolate one node (often the leader)
                SimNode n = leader != null && rng.chance(0.5) ? leader : ms.get(rng.nextInt(ms.size()));
                for (SimNode o : cluster.nodes()) {
                    if (o != n) {
                        cluster.block(n.id, o.id);
                        cluster.block(o.id, n.id);
                    }
                }
                world.log("partition: isolate n" + n.id);
            }
            case 1 -> { // random split into two sides
                List<SimNode> side = new ArrayList<>();
                for (SimNode n : ms) if (rng.chance(0.5)) side.add(n);
                for (SimNode a : side) {
                    for (SimNode b : cluster.nodes()) {
                        if (!side.contains(b)) {
                            cluster.block(a.id, b.id);
                            cluster.block(b.id, a.id);
                        }
                    }
                }
                world.log("partition: split " + side.stream().map(n -> n.id).toList());
            }
            case 2 -> { // bridge: two halves that only a single node can see
                if (ms.size() < 3) return;
                SimNode bridge = ms.get(rng.nextInt(ms.size()));
                List<SimNode> others = ms.stream().filter(n -> n != bridge).toList();
                int half = others.size() / 2;
                for (int i = 0; i < others.size(); i++) {
                    for (int j = 0; j < others.size(); j++) {
                        if ((i < half) != (j < half)) cluster.block(others.get(i).id, others.get(j).id);
                    }
                }
                world.log("partition: bridge via n" + bridge.id);
            }
            default -> { // one-way: a node can send but not receive
                SimNode n = ms.get(rng.nextInt(ms.size()));
                for (SimNode o : cluster.nodes()) if (o != n) cluster.block(o.id, n.id);
                world.log("partition: n" + n.id + " deaf");
            }
        }
        world.schedule(rng.between(100, 3_000) * 1_000_000L, () -> {
            if (!stopped) {
                cluster.healAll();
                world.log("heal");
            }
        });
    }

    private void pauseOne() {
        List<SimNode> up = members().stream().filter(n -> n.alive && !n.paused).toList();
        if (up.isEmpty()) return;
        SimNode n = up.get(rng.nextInt(up.size()));
        n.pause();
        world.log("pause n" + n.id);
        world.schedule(rng.between(50, 1_500) * 1_000_000L, n::resume);
    }

    /** Stops injecting faults and repairs everything. */
    void stopAndHeal() {
        stopped = true;
        next.cancel();
        cluster.healAll();
        for (SimNode n : cluster.nodes()) {
            if (!n.member) continue;
            if (n.paused) n.resume();
            if (!n.alive) n.start();
        }
        world.log("nemesis stopped, all healed");
    }
}
