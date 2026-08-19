package io.quorum.sim;

import io.quorum.common.Cancellable;
import io.quorum.common.Rng;
import io.quorum.common.Scheduler;
import io.quorum.common.SimFileSystem;
import io.quorum.kv.KvNode;
import io.quorum.kv.NodeConfig;
import io.quorum.kv.Request;
import io.quorum.kv.Response;
import io.quorum.raft.ConfState;
import io.quorum.raft.Message;
import io.quorum.raft.RaftConfig;
import io.quorum.storage.Options;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** One server slot in the simulated cluster: a persistent disk plus the current process incarnation. */
final class SimNode {
    final long id;
    private final SimCluster cluster;
    private final SimWorld world;
    SimFileSystem disk = new SimFileSystem();
    KvNode node;
    int incarnation;
    boolean alive;
    boolean paused;
    boolean member;
    ConfState bootstrap = ConfState.EMPTY;
    private final List<Runnable> stash = new ArrayList<>();
    private final double clockFactor;
    int crashes;

    SimNode(long id, SimCluster cluster, SimWorld world, double clockFactor) {
        this.id = id;
        this.cluster = cluster;
        this.world = world;
        this.clockFactor = clockFactor;
    }

    boolean up() {
        return alive && !paused;
    }

    void start() {
        incarnation++;
        alive = true;
        paused = false;
        stash.clear();
        int inc = incarnation;
        SimConfig c = cluster.cfg();
        long tick = (long) (c.tickMs() * 1_000_000L * clockFactor);
        NodeConfig nc = new NodeConfig(id, bootstrap,
                RaftConfig.defaults(id).withSnapshotChunkBytes(4096), Options.small(), tick, c.snapshotEvery(),
                16 << 10, c.bugs());
        Scheduler loop = scheduler(inc, 0);
        Scheduler background = scheduler(inc, 2_000_000); // compactions interleave at random points
        Rng rng = world.rng().split();
        node = new KvNode(nc, new FencedFileSystem(disk, () -> alive && incarnation == inc), loop, background,
                cluster::send, rng, cluster.invariants());
        world.log("start n" + id + " inc" + inc);
        node.start();
    }

    private Scheduler scheduler(int inc, long jitterNanos) {
        return new Scheduler() {
            @Override
            public void execute(Runnable task) {
                long jitter = jitterNanos > 0 ? world.rng().between(0, jitterNanos) : 0;
                world.schedule(jitter, () -> run(inc, task));
            }

            @Override
            public Cancellable schedule(long delayNanos, Runnable task) {
                return world.schedule(delayNanos, () -> run(inc, task));
            }
        };
    }

    private void run(int inc, Runnable task) {
        if (!alive || inc != incarnation) return;
        if (paused) {
            stash.add(() -> run(inc, task));
            return;
        }
        task.run();
    }

    /** Power loss: the process dies and the disk drops unsynced data. */
    void crash() {
        if (!alive) return;
        world.log("crash n" + id);
        node.kill();
        alive = false;
        paused = false;
        stash.clear();
        disk.crash(world.rng().split());
        crashes++;
    }

    /** Stop for good (removed from the cluster). */
    void stop() {
        if (alive) node.kill();
        alive = false;
        paused = false;
        stash.clear();
        member = false;
    }

    void wipe() {
        disk = new SimFileSystem();
    }

    void pause() {
        if (alive) paused = true;
    }

    void resume() {
        if (!paused) return;
        paused = false;
        List<Runnable> work = new ArrayList<>(stash);
        stash.clear();
        for (Runnable r : work) world.schedule(0, r);
    }

    void deliver(Message m) {
        run(incarnation, () -> node.onMessage(m));
    }

    void submit(Request req, Consumer<Response> cb) {
        if (!alive) return; // request lost: the client will time out
        run(incarnation, () -> node.submit(req, cb));
    }
}
