package io.quorum.kv;

import io.quorum.common.Cancellable;
import io.quorum.common.FileSystem;
import io.quorum.common.PlantedBug;
import io.quorum.common.Rng;
import io.quorum.common.Scheduler;
import io.quorum.raft.ConfChange;
import io.quorum.raft.Entry;
import io.quorum.raft.HardState;
import io.quorum.raft.Message;
import io.quorum.raft.RaftNode;
import io.quorum.raft.RaftStorage;
import io.quorum.raft.ReadState;
import io.quorum.raft.Ready;
import io.quorum.raft.Snapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Drives one replica: owns the {@link RaftNode}, its durable {@link RaftStorage}, and the
 * {@link KvStateMachine}, and turns client requests into proposals and ReadIndex reads.
 *
 * <p>Threading: every method must run on the single node-loop thread behind {@code loop}. That
 * is the simulator's event queue in tests and a single-threaded executor in production. Work is
 * batched: requests and messages only mark the node dirty, and one {@link #process()} pass then
 * persists everything with a single fsync (group commit at the Raft level).
 */
public final class KvNode implements AutoCloseable {
    /** Observer for invariant checks in the simulator. */
    public interface Observer {
        default void onApply(long nodeId, Entry e) {}

        default void onRoleChange(long nodeId, RaftNode.Role role, long term) {}
    }

    private record PendingWrite(long term, Consumer<Response> cb) {}

    private record PendingRead(Request req, Consumer<Response> cb) {}

    private record ReadyRead(long index, long seq, Request req, Consumer<Response> cb) {}

    private final NodeConfig cfg;
    private final FileSystem fs;
    private final Scheduler loop;
    private final Transport transport;
    private final Observer observer;
    private final RaftStorage storage;
    private final KvStateMachine sm;
    private final RaftNode raft;

    private final Map<Long, PendingWrite> pendingWrites = new TreeMap<>();
    private final Map<Long, PendingRead> pendingReads = new HashMap<>();
    private final PriorityQueue<ReadyRead> readyReads = new PriorityQueue<>(
            (a, b) -> a.index != b.index ? Long.compare(a.index, b.index) : Long.compare(a.seq, b.seq));
    private long readCtx;
    private long readSeq;
    private boolean processScheduled;
    private boolean closed;
    private Cancellable tickTimer = Cancellable.NONE;
    private RaftNode.Role lastRole;
    private long lastTerm;
    private long snapshots;

    public KvNode(NodeConfig cfg, FileSystem fs, Scheduler loop, Scheduler background, Transport transport, Rng rng,
                  Observer observer) {
        this.cfg = cfg;
        this.fs = fs;
        this.loop = loop;
        this.transport = transport;
        this.observer = observer;
        RaftStorage.Opened opened = RaftStorage.open(fs, "raft", cfg.segmentBytes());
        this.storage = opened.storage();
        this.sm = new KvStateMachine(fs, "kv", cfg.storage(), background, !cfg.bug(PlantedBug.NO_CLIENT_DEDUP));
        Snapshot snap = opened.snapshot();
        if (snap != null && sm.appliedIndex() < snap.index()) {
            sm.restore(snap.data()); // crashed between persisting a snapshot and installing it
        }
        this.raft = new RaftNode(cfg.raft().withBugs(cfg.bugs()), rng, cfg.bootstrap(),
                new RaftNode.Persisted(opened.hardState(), snap, opened.entries(), sm.appliedIndex()));
        this.lastRole = raft.role();
        this.lastTerm = raft.term();
    }

    /** Starts the tick timer. Call on the loop thread. */
    public void start() {
        scheduleTick();
        process();
    }

    private void scheduleTick() {
        tickTimer = loop.schedule(cfg.tickNanos(), () -> {
            if (closed) return;
            scheduleTick(); // first, so one failed tick cannot stop the clock forever
            raft.tick();
            process();
        });
    }

    public long id() {
        return cfg.id();
    }

    public RaftNode raft() {
        return raft;
    }

    public KvStateMachine stateMachine() {
        return sm;
    }

    public long snapshotsTaken() {
        return snapshots;
    }

    // ------------------------------------------------------------------ inputs

    public void onMessage(Message m) {
        if (closed) return;
        raft.step(m);
        requestProcess();
    }

    public void submit(Request req, Consumer<Response> cb) {
        if (closed) {
            cb.accept(Response.retry("node closed"));
            return;
        }
        switch (req) {
            case Request.Write w -> {
                long index = raft.propose(w.command().encode());
                if (index < 0) {
                    cb.accept(Response.notLeader(raft.leaderId()));
                    return;
                }
                pendingWrites.put(index, new PendingWrite(raft.term(), cb));
            }
            case Request.Get g -> read(req, cb);
            case Request.Scan s -> read(req, cb);
            case Request.AddLearner a -> confChange(new ConfChange(ConfChange.Type.ADD_LEARNER, a.nodeId()), cb);
            case Request.Promote p -> confChange(new ConfChange(ConfChange.Type.PROMOTE, p.nodeId()), cb);
            case Request.Remove r -> confChange(new ConfChange(ConfChange.Type.REMOVE, r.nodeId()), cb);
            case Request.TransferLeader t -> cb.accept(raft.transferLeadership(t.nodeId())
                    ? Response.message("transferring to " + t.nodeId()) : Response.notLeader(raft.leaderId()));
            case Request.Status s -> cb.accept(Response.message(status()));
        }
        requestProcess();
    }

    private void read(Request req, Consumer<Response> cb) {
        if (cfg.bug(PlantedBug.LEADER_LOCAL_READ) && raft.isLeader()) {
            cb.accept(serveRead(req)); // PLANTED BUG: a deposed leader can serve stale data
            return;
        }
        long ctx = ++readCtx;
        if (!raft.readIndex(ctx)) {
            cb.accept(Response.notLeader(raft.leaderId()));
            return;
        }
        pendingReads.put(ctx, new PendingRead(req, cb));
    }

    private void confChange(ConfChange cc, Consumer<Response> cb) {
        long index = raft.proposeConfChange(cc);
        if (index < 0) {
            cb.accept(raft.isLeader() ? Response.retry("conf change rejected or one already pending")
                    : Response.notLeader(raft.leaderId()));
            return;
        }
        pendingWrites.put(index, new PendingWrite(raft.term(), cb));
    }

    private Response serveRead(Request req) {
        return switch (req) {
            case Request.Get g -> Response.value(sm.get(g.key()));
            case Request.Scan s -> Response.rows(sm.scan(s.from(), s.to(), s.limit()));
            default -> throw new IllegalArgumentException();
        };
    }

    // ------------------------------------------------------------------ the Ready loop

    private void requestProcess() {
        if (!cfg.batching()) {
            process(); // ablation: one Ready (and one fsync) per request/message
            return;
        }
        if (processScheduled || closed) return;
        processScheduled = true;
        loop.execute(() -> {
            processScheduled = false;
            process();
        });
    }

    private void process() {
        if (closed) return;
        while (raft.hasReady()) {
            Ready rd = raft.ready();
            rd.sendNow().forEach(transport::send); // leader appends go out before our own fsync
            if (rd.snapshot() != null) {
                storage.saveSnapshot(rd.snapshot());
                storage.resetTo(rd.snapshot().index());
            }
            storage.append(rd.entries());
            if (!rd.entries().isEmpty() && cfg.fsync() && !cfg.bug(PlantedBug.LOG_NO_FSYNC)) storage.sync();
            if (rd.hardState() != null) {
                HardState hs = rd.hardState();
                if (cfg.bug(PlantedBug.VOTE_NOT_PERSISTED)) hs = new HardState(hs.term(), 0, hs.commit()); // PLANTED BUG
                storage.saveHardState(hs);
            }
            rd.sendAfterPersist().forEach(transport::send);

            if (rd.snapshot() != null) {
                sm.restore(rd.snapshot().data());
                failWritesUpTo(rd.snapshot().index());
            }
            for (Entry e : rd.committed()) {
                KvResult result = sm.apply(e);
                observer.onApply(cfg.id(), e);
                PendingWrite pw = pendingWrites.remove(e.index());
                if (pw != null) {
                    if (pw.term() == e.term()) pw.cb().accept(result == null ? Response.message("applied") : Response.ok(result));
                    else pw.cb().accept(Response.retry("entry overwritten by a new leader"));
                }
            }
            for (ReadState rs : rd.readStates()) {
                PendingRead pr = pendingReads.remove(rs.ctx());
                if (pr != null) readyReads.add(new ReadyRead(rs.index(), ++readSeq, pr.req(), pr.cb()));
            }
            for (long ctx : rd.droppedReads()) {
                PendingRead pr = pendingReads.remove(ctx);
                if (pr != null) pr.cb().accept(Response.retry("leadership lost during read"));
            }
            while (!readyReads.isEmpty() && readyReads.peek().index() <= sm.appliedIndex()) {
                ReadyRead r = readyReads.poll();
                r.cb().accept(serveRead(r.req()));
            }
            raft.advance(rd);
            maybeSnapshot();
        }
        if (raft.role() != lastRole || raft.term() != lastTerm) {
            lastRole = raft.role();
            lastTerm = raft.term();
            observer.onRoleChange(cfg.id(), lastRole, lastTerm);
            if (!raft.isLeader()) {
                failAllWrites("leadership lost");
                for (ReadyRead r : readyReads) r.cb().accept(Response.retry("leadership lost"));
                readyReads.clear();
            }
        }
    }

    private void failWritesUpTo(long index) {
        Iterator<Map.Entry<Long, PendingWrite>> it = pendingWrites.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, PendingWrite> e = it.next();
            if (e.getKey() > index) break;
            e.getValue().cb().accept(Response.retry("superseded by snapshot"));
            it.remove();
        }
    }

    private void failAllWrites(String why) {
        List<PendingWrite> all = new ArrayList<>(pendingWrites.values());
        pendingWrites.clear();
        for (PendingWrite pw : all) pw.cb().accept(Response.retry(why));
        List<PendingRead> reads = new ArrayList<>(pendingReads.values());
        pendingReads.clear();
        for (PendingRead pr : reads) pr.cb().accept(Response.retry(why));
    }

    private void maybeSnapshot() {
        long applied = sm.appliedIndex();
        if (applied - raft.snapshotIndex() < cfg.snapshotEvery()) return;
        Snapshot s = raft.createSnapshot(applied, sm.snapshot());
        storage.saveSnapshot(s);
        raft.compact(s);
        storage.compactTo(s.index());
        snapshots++;
    }

    public String status() {
        return String.format("{\"id\":%d,\"role\":\"%s\",\"term\":%d,\"leader\":%d,\"commit\":%d,\"applied\":%d,"
                        + "\"lastIndex\":%d,\"snapshotIndex\":%d,\"voters\":%s,\"learners\":%s}",
                cfg.id(), raft.role(), raft.term(), raft.leaderId(), raft.commitIndex(), sm.appliedIndex(),
                raft.lastIndex(), raft.snapshotIndex(), raft.conf().voters(), raft.conf().learners());
    }

    /** Stops the node. Pending requests are failed with RETRY. */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        tickTimer.cancel();
        List<PendingWrite> writes = new ArrayList<>(pendingWrites.values());
        pendingWrites.clear();
        for (PendingWrite pw : writes) pw.cb().accept(Response.retry("node closed"));
        List<PendingRead> reads = new ArrayList<>(pendingReads.values());
        pendingReads.clear();
        for (PendingRead pr : reads) pr.cb().accept(Response.retry("node closed"));
        sm.close();
        storage.close();
    }

    /** Marks the node dead without touching disk (simulated power loss). */
    public void kill() {
        closed = true;
        tickTimer.cancel();
    }
}
