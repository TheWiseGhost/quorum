package io.quorum.storage;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.CorruptionException;

import java.util.ArrayList;
import java.util.List;

/** A delta between versions, persisted as one MANIFEST record. */
final class VersionEdit {
    record Deleted(int level, long number) {}

    record Added(int level, FileMeta file) {}

    Long logNumber;
    Long nextFileNumber;
    Long lastSequence;
    final List<Deleted> deleted = new ArrayList<>();
    final List<Added> added = new ArrayList<>();

    private static final int LOG = 1, NEXT_FILE = 2, LAST_SEQ = 3, DELETED = 4, ADDED = 5;

    VersionEdit addFile(int level, FileMeta f) {
        added.add(new Added(level, f));
        return this;
    }

    VersionEdit deleteFile(int level, long number) {
        deleted.add(new Deleted(level, number));
        return this;
    }

    byte[] encode() {
        ByteWriter w = new ByteWriter();
        if (logNumber != null) w.putVarInt(LOG).putVarLong(logNumber);
        if (nextFileNumber != null) w.putVarInt(NEXT_FILE).putVarLong(nextFileNumber);
        if (lastSequence != null) w.putVarInt(LAST_SEQ).putVarLong(lastSequence);
        for (Deleted d : deleted) w.putVarInt(DELETED).putVarInt(d.level()).putVarLong(d.number());
        for (Added a : added) {
            FileMeta f = a.file();
            w.putVarInt(ADDED).putVarInt(a.level()).putVarLong(f.number()).putVarLong(f.size())
                    .putBytes(f.smallest()).putBytes(f.largest()).putVarLong(f.entries());
        }
        return w.toByteArray();
    }

    static VersionEdit decode(byte[] b) {
        VersionEdit e = new VersionEdit();
        ByteReader r = new ByteReader(b);
        while (r.hasRemaining()) {
            int tag = r.getVarInt();
            switch (tag) {
                case LOG -> e.logNumber = r.getVarLong();
                case NEXT_FILE -> e.nextFileNumber = r.getVarLong();
                case LAST_SEQ -> e.lastSequence = r.getVarLong();
                case DELETED -> e.deleted.add(new Deleted(r.getVarInt(), r.getVarLong()));
                case ADDED -> {
                    int level = r.getVarInt();
                    e.added.add(new Added(level, new FileMeta(r.getVarLong(), r.getVarLong(), r.getBytes(),
                            r.getBytes(), r.getVarLong())));
                }
                default -> throw new CorruptionException("unknown manifest tag " + tag);
            }
        }
        return e;
    }
}
