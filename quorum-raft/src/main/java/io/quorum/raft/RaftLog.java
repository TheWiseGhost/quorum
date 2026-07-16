package io.quorum.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-memory view of the log from the last snapshot onwards.
 *
 * <pre>
 * snapshotIndex  handedOut ≤ committed       stabled            lastIndex
 *      |------------|------------|--------------|--------------------|
 *                   applied      committed      persisted            unstable tail
 * </pre>
 */
final class RaftLog {
    private long snapshotIndex;
    private long snapshotTerm;
    private final ArrayList<Entry> entries = new ArrayList<>();
    long committed;
    long stabled;
    long handedOut;
    private final boolean noTruncateBug;

    RaftLog(boolean noTruncateBug) {
        this.noTruncateBug = noTruncateBug;
    }

    long snapshotIndex() {
        return snapshotIndex;
    }

    long snapshotTerm() {
        return snapshotTerm;
    }

    long firstIndex() {
        return snapshotIndex + 1;
    }

    long lastIndex() {
        return snapshotIndex + entries.size();
    }

    long lastTerm() {
        return term(lastIndex());
    }

    /** Term at {@code i}, or -1 if compacted away or beyond the end. */
    long term(long i) {
        if (i == snapshotIndex) return snapshotTerm;
        if (i < snapshotIndex || i > lastIndex()) return -1;
        return entries.get((int) (i - snapshotIndex - 1)).term();
    }

    Entry entry(long i) {
        return entries.get((int) (i - snapshotIndex - 1));
    }

    boolean matchTerm(long i, long t) {
        if (i < snapshotIndex) return true; // compacted entries are committed, hence matching
        return term(i) == t;
    }

    boolean isUpToDate(long lastIdx, long lastT) {
        long myTerm = lastTerm();
        return lastT > myTerm || (lastT == myTerm && lastIdx >= lastIndex());
    }

    /** First index of the run of entries sharing {@code term(i)} (for rejection hints). */
    long firstIndexOfTermAt(long i) {
        long t = term(i);
        long j = i;
        while (j - 1 > Math.max(snapshotIndex, committed) && term(j - 1) == t) j--;
        return j;
    }

    void leaderAppend(Entry e) {
        if (e.index() != lastIndex() + 1) throw new IllegalStateException("non-contiguous append " + e.index());
        entries.add(e);
    }

    /**
     * Follower append. Returns the last index known to match the leader, or -1 if
     * {@code (prevIndex, prevTerm)} does not match.
     */
    long maybeAppend(long prevIndex, long prevTerm, List<Entry> ents) {
        if (!matchTerm(prevIndex, prevTerm)) return -1;
        long lastNew = prevIndex + ents.size();
        for (int k = 0; k < ents.size(); k++) {
            Entry e = ents.get(k);
            if (e.index() <= snapshotIndex) continue;
            if (e.index() <= lastIndex()) {
                if (term(e.index()) == e.term()) continue;
                // Conflict: everything from here on was never committed, so drop it.
                if (e.index() <= committed) {
                    throw new IllegalStateException("conflict at committed index " + e.index());
                }
                if (noTruncateBug) {
                    continue; // PLANTED BUG: keep our conflicting entry
                }
                truncateFrom(e.index());
            }
            entries.add(e);
        }
        return lastNew;
    }

    private void truncateFrom(long index) {
        int keep = (int) (index - snapshotIndex - 1);
        while (entries.size() > keep) entries.removeLast();
        stabled = Math.min(stabled, index - 1);
    }

    List<Entry> slice(long lo, long hiInclusive, int maxCount, long maxBytes) {
        List<Entry> out = new ArrayList<>();
        long bytes = 0;
        for (long i = lo; i <= hiInclusive && out.size() < maxCount; i++) {
            Entry e = entry(i);
            if (!out.isEmpty() && bytes + e.sizeBytes() > maxBytes) break;
            out.add(e);
            bytes += e.sizeBytes();
        }
        return out;
    }

    List<Entry> unstable() {
        if (stabled >= lastIndex()) return List.of();
        return List.copyOf(entries.subList((int) (Math.max(stabled, snapshotIndex) - snapshotIndex), entries.size()));
    }

    void stableTo(long index, long term) {
        if (term(index) == term && index > stabled) stabled = index;
    }

    List<Entry> toHandOut() {
        long hi = Math.min(committed, stabled);
        if (hi <= handedOut) return List.of();
        return List.copyOf(entries.subList((int) (handedOut - snapshotIndex), (int) (hi - snapshotIndex)));
    }

    void restore(SnapshotMeta meta) {
        snapshotIndex = meta.index();
        snapshotTerm = meta.term();
        entries.clear();
        committed = Math.max(committed, meta.index());
        stabled = meta.index();
        handedOut = meta.index();
    }

    /** Drops entries up to and including {@code index} (which must be applied and persisted). */
    void compact(long index) {
        if (index <= snapshotIndex) return;
        if (index > handedOut || index > stabled) throw new IllegalStateException("compact beyond applied/stable");
        long t = term(index);
        int drop = (int) (index - snapshotIndex);
        entries.subList(0, drop).clear();
        snapshotIndex = index;
        snapshotTerm = t;
    }

    /** Restores persisted entries at startup. */
    void load(long snapIndex, long snapTerm, List<Entry> persisted) {
        snapshotIndex = snapIndex;
        snapshotTerm = snapTerm;
        entries.clear();
        for (Entry e : persisted) if (e.index() > snapIndex) entries.add(e);
        stabled = lastIndex();
        committed = snapIndex;
        handedOut = snapIndex;
    }

    List<Entry> all() {
        return List.copyOf(entries);
    }
}
