package io.quorum.raft;

import java.util.ArrayDeque;

/** Leader's view of one follower's replication state. */
final class Progress {
    enum State { PROBE, REPLICATE, SNAPSHOT }

    long match;
    long next;
    State state = State.PROBE;
    boolean probeSent;
    boolean recentActive;
    long maxAckedReadCtx;
    final ArrayDeque<Long> inflight = new ArrayDeque<>();
    long pendingSnapshotIndex;
    long snapshotOffset;
    int snapshotStallTicks;

    Progress(long next) {
        this.next = next;
    }

    boolean isPaused(int maxInflight) {
        return switch (state) {
            case PROBE -> probeSent;
            case REPLICATE -> inflight.size() >= maxInflight;
            case SNAPSHOT -> true;
        };
    }

    void becomeProbe() {
        if (state == State.SNAPSHOT) next = Math.max(match + 1, pendingSnapshotIndex + 1);
        else next = match + 1;
        state = State.PROBE;
        probeSent = false;
        inflight.clear();
        pendingSnapshotIndex = 0;
    }

    void becomeReplicate() {
        state = State.REPLICATE;
        next = match + 1;
        inflight.clear();
    }

    void becomeSnapshot(long index) {
        state = State.SNAPSHOT;
        pendingSnapshotIndex = index;
        snapshotOffset = 0;
        snapshotStallTicks = 0;
        inflight.clear();
    }

    boolean maybeUpdate(long n) {
        boolean updated = false;
        if (match < n) {
            match = n;
            updated = true;
            probeSent = false;
        }
        next = Math.max(next, n + 1);
        return updated;
    }

    void freeInflightsTo(long n) {
        while (!inflight.isEmpty() && inflight.peekFirst() <= n) inflight.pollFirst();
    }

    @Override
    public String toString() {
        return state + "{match=" + match + " next=" + next + " inflight=" + inflight.size() + "}";
    }
}
