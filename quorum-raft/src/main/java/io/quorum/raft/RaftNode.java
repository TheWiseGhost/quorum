package io.quorum.raft;

import io.quorum.common.PlantedBug;
import io.quorum.common.Rng;
import io.quorum.raft.Message.AppendEntries;
import io.quorum.raft.Message.AppendEntriesResponse;
import io.quorum.raft.Message.Heartbeat;
import io.quorum.raft.Message.HeartbeatResponse;
import io.quorum.raft.Message.InstallSnapshot;
import io.quorum.raft.Message.InstallSnapshotResponse;
import io.quorum.raft.Message.RequestVote;
import io.quorum.raft.Message.RequestVoteResponse;
import io.quorum.raft.Message.TimeoutNow;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The Raft consensus algorithm as a pure, single-threaded state machine (in the style of
 * etcd/raft). It performs no I/O and reads no clock: time advances only through {@link #tick()},
 * input arrives through {@link #step(Message)} and the propose/read methods, and all output is
 * collected into a {@link Ready} for the driver to persist, send and apply.
 *
 * <p>Implements leader election with PreVote and CheckQuorum, pipelined log replication with
 * fast backtracking, chunked snapshot transfer, ReadIndex linearizable reads, single-server
 * membership changes and leadership transfer.
 */
public final class RaftNode {
    public enum Role { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    /** Durable state handed back to a restarting node. */
    public record Persisted(HardState hardState, Snapshot snapshot, List<Entry> entries, long appliedIndex) {
        public static Persisted fresh() {
            return new Persisted(HardState.EMPTY, null, List.of(), 0);
        }
    }

    private record PendingRead(long ctx, long index, long round) {}

    private final long id;
    private final RaftConfig cfg;
    private final Rng rng;
    private final RaftLog log;

    private long term;
    private long votedFor;
    private Role role = Role.FOLLOWER;
    private long leaderId;
    private ConfState conf;
    /** Configuration in effect after each committed conf-change index (for snapshot metadata). */
    private final TreeMap<Long, ConfState> confHistory = new TreeMap<>();

    private final Map<Long, Progress> progress = new TreeMap<>();
    private final Map<Long, Boolean> votes = new TreeMap<>();
    private int electionElapsed;
    private int heartbeatElapsed;
    private int randomizedElectionTimeout;
    private long leadTransferee;
    private long pendingConfIndex;

    private long readRound;
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    private final List<Long> readsAwaitingCommit = new ArrayList<>();
    private List<ReadState> readStates = new ArrayList<>();
    private List<Long> droppedReads = new ArrayList<>();

    private List<Message> sendNow = new ArrayList<>();
    private List<Message> sendAfterPersist = new ArrayList<>();
    private HardState prevHardState;

    private Snapshot latestSnapshot;
    private Snapshot snapshotToInstall;
    private SnapshotMeta incomingMeta;
    private ByteArrayOutputStream incoming;

    public RaftNode(RaftConfig cfg, Rng rng, ConfState bootstrap, Persisted p) {
        this.id = cfg.id();
        this.cfg = cfg;
        this.rng = rng;
        this.log = new RaftLog(cfg.bug(PlantedBug.NO_TRUNCATE_CONFLICT));
        Snapshot snap = p.snapshot();
        HardState hs = p.hardState() == null ? HardState.EMPTY : p.hardState();
        if (snap != null) {
            log.load(snap.meta().index(), snap.meta().term(), p.entries());
            conf = snap.meta().conf();
            latestSnapshot = snap;
        } else {
            log.load(0, 0, p.entries());
            conf = ConfState.EMPTY;
            boolean fresh = p.entries().isEmpty() && hs.term() == 0;
            if (fresh && !bootstrap.voters().isEmpty()) {
                // Every founding member writes the same committed entry #1 at term 1.
                log.leaderAppend(new Entry(1, 1, Entry.Type.CONF_CHANGE, ConfChange.bootstrap(bootstrap).encode()));
                log.stabled = 0;
                hs = new HardState(1, 0, 1);
            }
        }
        confHistory.put(log.snapshotIndex(), conf);
        term = hs.term();
        votedFor = hs.votedFor();
        long commit = Math.min(Math.max(hs.commit(), log.snapshotIndex()), log.lastIndex());
        commitTo(commit);
        log.handedOut = Math.max(log.snapshotIndex(), Math.min(p.appliedIndex(), log.committed));
        prevHardState = p.hardState() == null ? HardState.EMPTY : p.hardState();
        becomeFollower(term, 0);
    }

    // ------------------------------------------------------------------ accessors

    public long id() {
        return id;
    }

    public long term() {
        return term;
    }

    public Role role() {
        return role;
    }

    public long leaderId() {
        return leaderId;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public long commitIndex() {
        return log.committed;
    }

    public long lastIndex() {
        return log.lastIndex();
    }

    public long stabledIndex() {
        return log.stabled;
    }

    public long termAt(long index) {
        return log.term(index);
    }

    public long snapshotIndex() {
        return log.snapshotIndex();
    }

    public ConfState conf() {
        return conf;
    }

    public long votedFor() {
        return votedFor;
    }

    public HardState hardState() {
        return new HardState(term, votedFor, log.committed);
    }

    /** Copy of in-memory entries after the snapshot (for invariant checks). */
    public List<Entry> entries() {
        return log.all();
    }

    public boolean transferring() {
        return leadTransferee != 0;
    }

    /** True once the leader has committed an entry from its own term (required for reads and conf changes). */
    public boolean committedInCurrentTerm() {
        return log.term(log.committed) == term;
    }

    /** Leader-only: how far {@code peer} has replicated, or -1. */
    public long matchIndex(long peer) {
        Progress pr = progress.get(peer);
        return pr == null ? -1 : pr.match;
    }

    private boolean promotable() {
        return conf.voters().contains(id);
    }

    // ------------------------------------------------------------------ role transitions

    private void reset(long newTerm) {
        if (term != newTerm) {
            term = newTerm;
            votedFor = 0;
        }
        leaderId = 0;
        electionElapsed = 0;
        heartbeatElapsed = 0;
        randomizedElectionTimeout = cfg.electionTicks() + rng.nextInt(cfg.electionTicks());
        votes.clear();
        leadTransferee = 0;
        progress.clear();
        dropPendingReads();
    }

    private void dropPendingReads() {
        for (PendingRead r : pendingReads) droppedReads.add(r.ctx());
        pendingReads.clear();
        droppedReads.addAll(readsAwaitingCommit);
        readsAwaitingCommit.clear();
    }

    private void becomeFollower(long newTerm, long leader) {
        reset(newTerm);
        role = Role.FOLLOWER;
        leaderId = leader;
    }

    private void becomePreCandidate() {
        // Pre-vote does not touch term or vote: a partitioned node cannot inflate the term.
        votes.clear();
        leaderId = 0;
        role = Role.PRE_CANDIDATE;
        electionElapsed = 0;
        randomizedElectionTimeout = cfg.electionTicks() + rng.nextInt(cfg.electionTicks());
    }

    private void becomeCandidate() {
        reset(term + 1);
        votedFor = id;
        role = Role.CANDIDATE;
    }

    private void becomeLeader() {
        reset(term);
        role = Role.LEADER;
        leaderId = id;
        for (long peer : conf.voters()) progress.put(peer, newProgress(peer));
        for (long peer : conf.learners()) progress.put(peer, newProgress(peer));
        // Conservative: no conf change until everything before our term is committed.
        pendingConfIndex = log.lastIndex();
        if (!cfg.bug(PlantedBug.FIGURE8_COMMIT)) {
            appendAsLeader(Entry.Type.NORMAL, new byte[0]); // no-op commits prior-term entries
        }
        bcastAppend();
    }

    private Progress newProgress(long peer) {
        Progress pr = new Progress(log.lastIndex() + 1);
        pr.recentActive = true;
        if (peer == id) pr.match = log.stabled;
        return pr;
    }

    // ------------------------------------------------------------------ ticks

    public void tick() {
        if (role == Role.LEADER) tickLeader();
        else tickElection();
    }

    private void tickElection() {
        electionElapsed++;
        if (promotable() && electionElapsed >= randomizedElectionTimeout) {
            electionElapsed = 0;
            campaign(false);
        }
    }

    private void tickLeader() {
        heartbeatElapsed++;
        electionElapsed++;
        if (electionElapsed >= cfg.electionTicks()) {
            electionElapsed = 0;
            if (cfg.checkQuorum()) {
                int active = 0;
                for (long v : conf.voters()) {
                    Progress pr = progress.get(v);
                    if (v == id || (pr != null && pr.recentActive)) active++;
                }
                if (active < conf.quorum()) {
                    becomeFollower(term, 0); // we may be partitioned away from a newer leader
                    return;
                }
                progress.forEach((peer, pr) -> pr.recentActive = peer == id);
            }
            if (leadTransferee != 0) leadTransferee = 0; // transfer timed out
        }
        if (heartbeatElapsed >= cfg.heartbeatTicks()) {
            heartbeatElapsed = 0;
            bcastHeartbeat(0);
            // Re-send a stalled snapshot chunk (the message may have been dropped).
            progress.forEach((peer, pr) -> {
                if (peer != id && pr.state == Progress.State.SNAPSHOT && ++pr.snapshotStallTicks >= 3) {
                    pr.snapshotStallTicks = 0;
                    sendSnapshotChunk(peer, pr);
                }
            });
        }
    }

    /** Starts an election; {@code transfer} bypasses pre-vote and leader stickiness. */
    public void campaign(boolean transfer) {
        if (!promotable() || role == Role.LEADER) return;
        if (cfg.preVote() && !transfer) {
            becomePreCandidate();
            sendVoteRequests(true, false, term + 1);
        } else {
            becomeCandidate();
            sendVoteRequests(false, transfer, term);
        }
    }

    private void sendVoteRequests(boolean pre, boolean transfer, long voteTerm) {
        votes.put(id, true);
        if (tallyAndMaybeWin(pre)) return;
        for (long peer : conf.voters()) {
            if (peer == id) continue;
            send(new RequestVote(id, peer, voteTerm, log.lastIndex(), log.lastTerm(), pre, transfer));
        }
    }

    /** Returns true if the election concluded (won or lost). */
    private boolean tallyAndMaybeWin(boolean pre) {
        int granted = 0, rejected = 0;
        for (long v : conf.voters()) {
            Boolean b = votes.get(v);
            if (b == null) continue;
            if (b) granted++;
            else rejected++;
        }
        if (granted >= conf.quorum()) {
            if (pre) campaignAfterPreVote();
            else becomeLeader();
            return true;
        }
        if (rejected >= conf.quorum()) {
            becomeFollower(term, 0);
            return true;
        }
        return false;
    }

    private void campaignAfterPreVote() {
        becomeCandidate();
        sendVoteRequests(false, false, term);
    }

    // ------------------------------------------------------------------ message handling

    public void step(Message m) {
        if (m.to() != id) return;
        if (m.term() > term) {
            if (m instanceof RequestVote rv) {
                boolean inLease = cfg.checkQuorum() && leaderId != 0 && electionElapsed < cfg.electionTicks();
                if (!rv.transfer() && inLease) return; // leader stickiness: ignore disruptive candidates
            }
            boolean preVoteTraffic = (m instanceof RequestVote rv && rv.preVote())
                    || (m instanceof RequestVoteResponse r && r.preVote() && r.granted());
            if (!preVoteTraffic) {
                long leader = (m instanceof AppendEntries || m instanceof Heartbeat || m instanceof InstallSnapshot)
                        ? m.from() : 0;
                becomeFollower(m.term(), leader);
            }
        } else if (m.term() < term) {
            if (m instanceof AppendEntries || m instanceof Heartbeat) {
                // Let a stale leader learn the new term (needed with PreVote/CheckQuorum).
                send(new AppendEntriesResponse(id, m.from(), term, false, 0, 0, 0));
            } else if (m instanceof RequestVote rv && rv.preVote()) {
                send(new RequestVoteResponse(id, m.from(), term, false, true));
            }
            return;
        }

        switch (m) {
            case RequestVote rv -> handleRequestVote(rv);
            case RequestVoteResponse r -> handleVoteResponse(r);
            case AppendEntries ae -> handleAppendEntries(ae);
            case AppendEntriesResponse r -> handleAppendResponse(r);
            case Heartbeat hb -> handleHeartbeat(hb);
            case HeartbeatResponse r -> handleHeartbeatResponse(r);
            case InstallSnapshot is -> handleInstallSnapshot(is);
            case InstallSnapshotResponse r -> handleInstallSnapshotResponse(r);
            case TimeoutNow t -> {
                if (role != Role.LEADER) campaign(true);
            }
        }
    }

    private void handleRequestVote(RequestVote m) {
        boolean canVote = votedFor == m.from()
                || (votedFor == 0 && leaderId == 0)
                || (m.preVote() && m.term() > term);
        if (canVote && log.isUpToDate(m.lastIndex(), m.lastTerm())) {
            send(new RequestVoteResponse(id, m.from(), m.term(), true, m.preVote()));
            if (!m.preVote()) {
                electionElapsed = 0;
                votedFor = m.from();
            }
        } else {
            send(new RequestVoteResponse(id, m.from(), term, false, m.preVote()));
        }
    }

    private void handleVoteResponse(RequestVoteResponse m) {
        boolean relevant = m.preVote() ? role == Role.PRE_CANDIDATE : role == Role.CANDIDATE;
        if (!relevant) return;
        votes.put(m.from(), m.granted());
        tallyAndMaybeWin(m.preVote());
    }

    private void handleAppendEntries(AppendEntries m) {
        becomeFollowerOfCurrentLeader(m.from());
        if (m.prevIndex() < log.committed) {
            send(new AppendEntriesResponse(id, m.from(), term, true, log.committed, 0, 0));
            return;
        }
        long lastNew = log.maybeAppend(m.prevIndex(), m.prevTerm(), m.entries());
        if (lastNew >= 0) {
            commitTo(Math.min(m.commit(), lastNew));
            send(new AppendEntriesResponse(id, m.from(), term, true, lastNew, 0, 0));
        } else {
            long hintIndex, hintTerm;
            if (m.prevIndex() > log.lastIndex()) {
                hintIndex = log.lastIndex() + 1;
                hintTerm = 0;
            } else {
                hintTerm = log.term(m.prevIndex());
                hintIndex = log.firstIndexOfTermAt(m.prevIndex());
            }
            send(new AppendEntriesResponse(id, m.from(), term, false, m.prevIndex(), hintIndex, hintTerm));
        }
    }

    private void becomeFollowerOfCurrentLeader(long leader) {
        if (role != Role.FOLLOWER) becomeFollower(term, leader);
        leaderId = leader;
        electionElapsed = 0;
    }

    private void handleHeartbeat(Heartbeat m) {
        becomeFollowerOfCurrentLeader(m.from());
        commitTo(Math.min(m.commit(), log.lastIndex()));
        send(new HeartbeatResponse(id, m.from(), term, m.readCtx()));
    }

    private void handleAppendResponse(AppendEntriesResponse m) {
        if (role != Role.LEADER) return;
        Progress pr = progress.get(m.from());
        if (pr == null) return;
        pr.recentActive = true;
        if (!m.success()) {
            if (m.index() == 0 && m.hintIndex() == 0) return; // stale-term notice, already handled
            boolean stale = pr.state == Progress.State.REPLICATE ? m.index() <= pr.match : m.index() != pr.next - 1;
            if (stale) return;
            long next = m.hintIndex();
            if (m.hintTerm() > 0) {
                // If we have entries from the follower's conflicting term, skip past them.
                long last = lastIndexOfTerm(m.hintTerm(), m.index());
                if (last > 0) next = last + 1;
            }
            pr.next = Math.max(pr.match + 1, Math.min(next, m.index()));
            if (pr.state == Progress.State.REPLICATE) {
                pr.becomeProbe();
                pr.next = Math.max(pr.match + 1, Math.min(next, m.index()));
            }
            pr.probeSent = false;
            sendAppend(m.from());
            return;
        }
        boolean wasPaused = pr.isPaused(cfg.maxInflight());
        if (pr.maybeUpdate(m.index())) {
            if (pr.state == Progress.State.PROBE) pr.becomeReplicate();
            else if (pr.state == Progress.State.REPLICATE) pr.freeInflightsTo(m.index());
            if (maybeCommit()) bcastAppend();
            else if (wasPaused || pr.next <= log.lastIndex()) sendAppend(m.from());
            if (m.from() == leadTransferee && pr.match == log.lastIndex()) {
                send(new TimeoutNow(id, m.from(), term));
            }
        } else if (pr.state == Progress.State.REPLICATE) {
            pr.freeInflightsTo(m.index());
        }
    }

    private long lastIndexOfTerm(long t, long atOrBelow) {
        for (long i = Math.min(atOrBelow, log.lastIndex()); i > log.snapshotIndex(); i--) {
            long ti = log.term(i);
            if (ti == t) return i;
            if (ti < t) return 0;
        }
        return 0;
    }

    private void handleHeartbeatResponse(HeartbeatResponse m) {
        if (role != Role.LEADER) return;
        Progress pr = progress.get(m.from());
        if (pr == null) return;
        pr.recentActive = true;
        pr.probeSent = false;
        // Appends may have been lost: free a pipeline slot and resend. In REPLICATE an empty
        // append at next-1 makes a follower with a gap reject, which drops us back to PROBE.
        if (pr.state == Progress.State.REPLICATE && pr.inflight.size() >= cfg.maxInflight()) pr.inflight.pollFirst();
        if (pr.match < log.lastIndex()) sendAppend(m.from(), true);
        if (m.readCtx() > pr.maxAckedReadCtx) {
            pr.maxAckedReadCtx = m.readCtx();
            resolveReads();
        }
    }

    // ------------------------------------------------------------------ replication (leader)

    private void bcastAppend() {
        for (long peer : progress.keySet()) if (peer != id) sendAppend(peer, true);
    }

    private void bcastHeartbeat(long readCtx) {
        for (Map.Entry<Long, Progress> e : progress.entrySet()) {
            long peer = e.getKey();
            if (peer == id) continue;
            long commit = Math.min(e.getValue().match, log.committed);
            send(new Heartbeat(id, peer, term, commit, readCtx));
        }
    }

    private void sendAppend(long to) {
        sendAppend(to, false);
    }

    /**
     * @param sendIfEmpty also send when there are no new entries (to carry the commit index, or
     *                    to provoke a rejection that resynchronises a follower after lost messages)
     */
    private void sendAppend(long to, boolean sendIfEmpty) {
        Progress pr = progress.get(to);
        if (pr == null || pr.isPaused(cfg.maxInflight())) return;
        long prevIndex = pr.next - 1;
        long prevTerm = log.term(prevIndex);
        if (prevTerm < 0 || pr.next < log.firstIndex()) {
            if (latestSnapshot == null) return;
            pr.becomeSnapshot(latestSnapshot.index());
            sendSnapshotChunk(to, pr);
            return;
        }
        List<Entry> ents = pr.next <= log.lastIndex()
                ? log.slice(pr.next, log.lastIndex(), cfg.maxEntriesPerMessage(), cfg.maxBytesPerMessage())
                : List.of();
        if (ents.isEmpty() && !sendIfEmpty && pr.state == Progress.State.REPLICATE) return;
        send(new AppendEntries(id, to, term, prevIndex, prevTerm, ents, log.committed));
        if (pr.state == Progress.State.REPLICATE) {
            if (!ents.isEmpty()) {
                long last = ents.getLast().index();
                pr.next = last + 1;
                pr.inflight.addLast(last);
            }
        } else {
            pr.probeSent = true;
        }
    }

    private void sendSnapshotChunk(long to, Progress pr) {
        Snapshot s = latestSnapshot;
        if (s == null) return;
        if (pr.pendingSnapshotIndex != s.index()) {
            pr.pendingSnapshotIndex = s.index();
            pr.snapshotOffset = 0;
        }
        int off = (int) pr.snapshotOffset;
        int len = Math.min(cfg.snapshotChunkBytes(), s.data().length - off);
        byte[] chunk = Arrays.copyOfRange(s.data(), off, off + Math.max(len, 0));
        send(new InstallSnapshot(id, to, term, s.meta(), off, chunk, s.data().length));
    }

    private void handleInstallSnapshotResponse(InstallSnapshotResponse m) {
        if (role != Role.LEADER) return;
        Progress pr = progress.get(m.from());
        if (pr == null || pr.state != Progress.State.SNAPSHOT) return;
        pr.recentActive = true;
        if (m.snapshotIndex() != pr.pendingSnapshotIndex) return;
        pr.snapshotStallTicks = 0;
        if (m.done()) {
            pr.match = Math.max(pr.match, m.snapshotIndex());
            pr.becomeProbe();
            sendAppend(m.from());
        } else {
            pr.snapshotOffset = m.nextOffset();
            sendSnapshotChunk(m.from(), pr);
        }
    }

    private boolean maybeCommit() {
        boolean any = false;
        while (true) {
            long[] matches = conf.voters().stream().mapToLong(v -> {
                Progress pr = progress.get(v);
                return pr == null ? 0 : pr.match;
            }).sorted().toArray();
            if (matches.length == 0) return any;
            long candidate = matches[matches.length - conf.quorum()];
            boolean currentTerm = log.term(candidate) == term;
            if (candidate <= log.committed || !(currentTerm || cfg.bug(PlantedBug.FIGURE8_COMMIT))) return any;
            ConfState before = conf;
            commitTo(candidate);
            any = true;
            if (role != Role.LEADER) return any;
            if (before == conf) return any; // quorum unchanged: done
        }
    }

    // ------------------------------------------------------------------ snapshots (follower)

    private void handleInstallSnapshot(InstallSnapshot m) {
        becomeFollowerOfCurrentLeader(m.from());
        SnapshotMeta meta = m.meta();
        if (meta.index() <= log.committed) {
            send(new InstallSnapshotResponse(id, m.from(), term, meta.index(), m.totalSize(), true));
            return;
        }
        if (m.offset() == 0 || incomingMeta == null || incomingMeta.index() != meta.index()) {
            if (m.offset() != 0) {
                send(new InstallSnapshotResponse(id, m.from(), term, meta.index(), 0, false));
                return;
            }
            incomingMeta = meta;
            incoming = new ByteArrayOutputStream();
        }
        if (m.offset() != incoming.size()) {
            send(new InstallSnapshotResponse(id, m.from(), term, meta.index(), incoming.size(), false));
            return;
        }
        incoming.writeBytes(m.chunk());
        if (incoming.size() < m.totalSize()) {
            send(new InstallSnapshotResponse(id, m.from(), term, meta.index(), incoming.size(), false));
            return;
        }
        Snapshot snap = new Snapshot(meta, incoming.toByteArray());
        incomingMeta = null;
        incoming = null;
        if (log.matchTerm(meta.index(), meta.term()) && meta.index() <= log.lastIndex()) {
            commitTo(meta.index()); // we already have the entries; just fast-forward
        } else {
            log.restore(meta);
            conf = meta.conf();
            confHistory.clear();
            confHistory.put(meta.index(), conf);
            latestSnapshot = snap;
            snapshotToInstall = snap;
        }
        send(new InstallSnapshotResponse(id, m.from(), term, meta.index(), m.totalSize(), true));
    }

    /** Builds snapshot metadata for an applied index (the driver then persists it). */
    public Snapshot createSnapshot(long index, byte[] data) {
        if (index > log.handedOut || index < log.snapshotIndex()) {
            throw new IllegalArgumentException("snapshot index " + index + " not applied");
        }
        ConfState c = confHistory.floorEntry(index).getValue();
        return new Snapshot(new SnapshotMeta(index, log.term(index), c), data);
    }

    /** The driver persisted {@code s}; drop the log prefix it covers. */
    public void compact(Snapshot s) {
        if (s.index() <= log.snapshotIndex()) return;
        log.compact(s.index());
        latestSnapshot = s;
        Map.Entry<Long, ConfState> floor = confHistory.floorEntry(s.index());
        confHistory.headMap(s.index(), true).clear();
        confHistory.put(s.index(), floor.getValue());
    }

    // ------------------------------------------------------------------ commit & membership

    private void commitTo(long index) {
        while (log.committed < index) {
            long i = ++log.committed;
            if (i > log.snapshotIndex()) {
                Entry e = log.entry(i);
                if (e.type() == Entry.Type.CONF_CHANGE) applyConfChange(i, ConfChange.decode(e.data()));
            }
            if (role == Role.LEADER && log.term(i) == term && !readsAwaitingCommit.isEmpty()) {
                List<Long> waiting = new ArrayList<>(readsAwaitingCommit);
                readsAwaitingCommit.clear();
                for (long ctx : waiting) registerRead(ctx);
            }
        }
    }

    private void applyConfChange(long index, ConfChange cc) {
        ConfState next = conf.apply(cc);
        if (next == null) return; // invalid change: committed but a no-op on every node
        conf = next;
        confHistory.put(index, conf);
        if (role == Role.LEADER) {
            if (cc.type() == ConfChange.Type.ADD_LEARNER) {
                Progress pr = newProgress(cc.nodeId());
                pr.match = 0;
                progress.put(cc.nodeId(), pr);
            } else if (cc.type() == ConfChange.Type.REMOVE) {
                progress.remove(cc.nodeId());
            }
            if (!conf.voters().contains(id)) {
                becomeFollower(term, 0); // removed ourselves: step down
            }
        }
    }

    // ------------------------------------------------------------------ client API

    /** Appends a command. Returns its log index, or -1 if not the leader (or transferring). */
    public long propose(byte[] data) {
        if (role != Role.LEADER || leadTransferee != 0) return -1;
        long index = appendAsLeader(Entry.Type.NORMAL, data);
        bcastAppend();
        return index;
    }

    /** Returns the entry index, or -1 if rejected (not leader, change pending, or invalid). */
    public long proposeConfChange(ConfChange cc) {
        if (role != Role.LEADER || leadTransferee != 0) return -1;
        if (pendingConfIndex > log.committed || !committedInCurrentTerm()) return -1;
        if (conf.apply(cc) == null) return -1;
        long index = appendAsLeader(Entry.Type.CONF_CHANGE, cc.encode());
        pendingConfIndex = index;
        bcastAppend();
        return index;
    }

    private long appendAsLeader(Entry.Type type, byte[] data) {
        Entry e = new Entry(term, log.lastIndex() + 1, type, data);
        log.leaderAppend(e);
        return e.index();
    }

    /**
     * Requests a linearizable read. The returned {@link ReadState} (in a later Ready) carries the
     * commit index that the state machine must reach before serving it. Returns false if not leader.
     */
    public boolean readIndex(long ctx) {
        if (role != Role.LEADER) return false;
        if (!committedInCurrentTerm()) {
            readsAwaitingCommit.add(ctx); // the no-op must commit before commitIndex is trustworthy
            return true;
        }
        registerRead(ctx);
        return true;
    }

    private void registerRead(long ctx) {
        long round = ++readRound;
        pendingReads.addLast(new PendingRead(ctx, log.committed, round));
        bcastHeartbeat(round);
        resolveReads();
    }

    private void resolveReads() {
        while (!pendingReads.isEmpty()) {
            PendingRead head = pendingReads.peekFirst();
            int acks = 0;
            for (long v : conf.voters()) {
                Progress pr = progress.get(v);
                if (v == id || (pr != null && pr.maxAckedReadCtx >= head.round())) acks++;
            }
            if (acks < conf.quorum()) return;
            pendingReads.pollFirst();
            readStates.add(new ReadState(head.ctx(), head.index()));
        }
    }

    public boolean transferLeadership(long target) {
        if (role != Role.LEADER || target == id || !conf.voters().contains(target)) return false;
        Progress pr = progress.get(target);
        if (pr == null) return false;
        leadTransferee = target;
        electionElapsed = 0;
        if (pr.match == log.lastIndex()) send(new TimeoutNow(id, target, term));
        else sendAppend(target);
        return true;
    }

    // ------------------------------------------------------------------ ready / advance

    private void send(Message m) {
        if (m instanceof AppendEntries || m instanceof Heartbeat || m instanceof InstallSnapshot
                || m instanceof TimeoutNow) {
            sendNow.add(m);
        } else {
            sendAfterPersist.add(m);
        }
    }

    public boolean hasReady() {
        return !hardState().equals(prevHardState) || log.stabled < log.lastIndex() || snapshotToInstall != null
                || !log.toHandOut().isEmpty() || !sendNow.isEmpty() || !sendAfterPersist.isEmpty()
                || !readStates.isEmpty() || !droppedReads.isEmpty();
    }

    public Ready ready() {
        HardState hs = hardState();
        Ready rd = new Ready(hs.equals(prevHardState) ? null : hs, log.unstable(), snapshotToInstall, log.toHandOut(),
                sendNow, sendAfterPersist, readStates, droppedReads);
        sendNow = new ArrayList<>();
        sendAfterPersist = new ArrayList<>();
        readStates = new ArrayList<>();
        droppedReads = new ArrayList<>();
        return rd;
    }

    public void advance(Ready rd) {
        if (rd.hardState() != null) prevHardState = rd.hardState();
        if (rd.snapshot() != null && snapshotToInstall == rd.snapshot()) snapshotToInstall = null;
        if (!rd.entries().isEmpty()) {
            Entry last = rd.entries().getLast();
            log.stableTo(last.index(), last.term());
        }
        if (!rd.committed().isEmpty()) {
            log.handedOut = Math.max(log.handedOut, rd.committed().getLast().index());
        }
        if (role == Role.LEADER) {
            Progress self = progress.get(id);
            if (self != null && self.maybeUpdate(log.stabled) && maybeCommit()) bcastAppend();
        }
    }

    /** Leader's per-peer replication state, for diagnostics. */
    public String progressSummary() {
        StringBuilder sb = new StringBuilder();
        progress.forEach((peer, pr) -> sb.append(peer).append('=').append(pr).append(' '));
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        return "Raft{id=" + id + " " + role + " term=" + term + " leader=" + leaderId + " commit=" + log.committed
                + " last=" + log.lastIndex() + " stabled=" + log.stabled + " conf=" + conf.voters() + "/" + conf.learners() + "}";
    }
}
