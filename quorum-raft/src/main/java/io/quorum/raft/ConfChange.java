package io.quorum.raft;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;

/**
 * Single-server membership change (Raft dissertation §4.1). New servers join as learners, catch
 * up, then get promoted, so a lagging new voter never stalls commits. {@code BOOTSTRAP} is the
 * first entry of every cluster's log and carries the initial voters, so a brand-new server can
 * rebuild the membership history purely from the log.
 */
public record ConfChange(Type type, long nodeId, ConfState initial) {
    public enum Type { ADD_LEARNER, PROMOTE, REMOVE, BOOTSTRAP }

    public ConfChange(Type type, long nodeId) {
        this(type, nodeId, null);
    }

    public static ConfChange bootstrap(ConfState conf) {
        return new ConfChange(Type.BOOTSTRAP, 0, conf);
    }

    public byte[] encode() {
        ByteWriter w = new ByteWriter(16).putByte(type.ordinal()).putVarLong(nodeId);
        if (type == Type.BOOTSTRAP) initial.encode(w);
        return w.toByteArray();
    }

    public static ConfChange decode(byte[] b) {
        ByteReader r = new ByteReader(b);
        Type t = Type.values()[r.getByte()];
        long node = r.getVarLong();
        return new ConfChange(t, node, t == Type.BOOTSTRAP ? ConfState.decode(r) : null);
    }
}
