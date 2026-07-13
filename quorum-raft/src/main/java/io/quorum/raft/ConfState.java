package io.quorum.raft;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

/** Cluster membership: voters decide elections and commits; learners only receive the log. */
public record ConfState(SortedSet<Long> voters, SortedSet<Long> learners) {
    public ConfState {
        voters = Collections.unmodifiableSortedSet(new TreeSet<>(voters));
        learners = Collections.unmodifiableSortedSet(new TreeSet<>(learners));
    }

    public static ConfState of(long... voterIds) {
        TreeSet<Long> v = new TreeSet<>();
        for (long id : voterIds) v.add(id);
        return new ConfState(v, new TreeSet<>());
    }

    public static final ConfState EMPTY = new ConfState(new TreeSet<>(), new TreeSet<>());

    public int quorum() {
        return voters.size() / 2 + 1;
    }

    public boolean contains(long id) {
        return voters.contains(id) || learners.contains(id);
    }

    /** Returns null if the change is not valid against this configuration. */
    public ConfState apply(ConfChange cc) {
        TreeSet<Long> v = new TreeSet<>(voters), l = new TreeSet<>(learners);
        switch (cc.type()) {
            case BOOTSTRAP -> {
                return voters.isEmpty() && learners.isEmpty() ? cc.initial() : null;
            }
            case ADD_LEARNER -> {
                if (contains(cc.nodeId())) return null;
                l.add(cc.nodeId());
            }
            case PROMOTE -> {
                if (!l.remove(cc.nodeId())) return null;
                v.add(cc.nodeId());
            }
            case REMOVE -> {
                if (!v.remove(cc.nodeId()) && !l.remove(cc.nodeId())) return null;
                if (v.isEmpty()) return null;
            }
        }
        return new ConfState(v, l);
    }

    public void encode(ByteWriter w) {
        w.putVarInt(voters.size());
        for (long id : voters) w.putVarLong(id);
        w.putVarInt(learners.size());
        for (long id : learners) w.putVarLong(id);
    }

    public static ConfState decode(ByteReader r) {
        TreeSet<Long> v = new TreeSet<>(), l = new TreeSet<>();
        for (int i = r.getVarInt(); i > 0; i--) v.add(r.getVarLong());
        for (int i = r.getVarInt(); i > 0; i--) l.add(r.getVarLong());
        return new ConfState(v, l);
    }
}
