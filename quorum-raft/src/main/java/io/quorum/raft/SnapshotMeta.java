package io.quorum.raft;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;

/** Last log position covered by a snapshot, plus the membership at that point. */
public record SnapshotMeta(long index, long term, ConfState conf) {
    public void encode(ByteWriter w) {
        w.putVarLong(index).putVarLong(term);
        conf.encode(w);
    }

    public static SnapshotMeta decode(ByteReader r) {
        return new SnapshotMeta(r.getVarLong(), r.getVarLong(), ConfState.decode(r));
    }
}
