package io.quorum.raft;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.CorruptionException;
import io.quorum.raft.Message.AppendEntries;
import io.quorum.raft.Message.AppendEntriesResponse;
import io.quorum.raft.Message.Heartbeat;
import io.quorum.raft.Message.HeartbeatResponse;
import io.quorum.raft.Message.InstallSnapshot;
import io.quorum.raft.Message.InstallSnapshotResponse;
import io.quorum.raft.Message.RequestVote;
import io.quorum.raft.Message.RequestVoteResponse;
import io.quorum.raft.Message.TimeoutNow;

import java.util.ArrayList;
import java.util.List;

/** Compact binary encoding of {@link Message}s: {@code type:u8 | from | to | term | body}. */
public final class MessageCodec {
    private MessageCodec() {}

    public static byte[] encode(Message m) {
        ByteWriter w = new ByteWriter(64);
        int type = switch (m) {
            case RequestVote x -> 1;
            case RequestVoteResponse x -> 2;
            case AppendEntries x -> 3;
            case AppendEntriesResponse x -> 4;
            case Heartbeat x -> 5;
            case HeartbeatResponse x -> 6;
            case InstallSnapshot x -> 7;
            case InstallSnapshotResponse x -> 8;
            case TimeoutNow x -> 9;
        };
        w.putByte(type).putVarLong(m.from()).putVarLong(m.to()).putVarLong(m.term());
        switch (m) {
            case RequestVote x -> w.putVarLong(x.lastIndex()).putVarLong(x.lastTerm()).putBool(x.preVote()).putBool(x.transfer());
            case RequestVoteResponse x -> w.putBool(x.granted()).putBool(x.preVote());
            case AppendEntries x -> {
                w.putVarLong(x.prevIndex()).putVarLong(x.prevTerm()).putVarLong(x.commit()).putVarInt(x.entries().size());
                for (Entry e : x.entries()) w.putBytes(RaftStorage.encodeEntry(e));
            }
            case AppendEntriesResponse x -> w.putBool(x.success()).putVarLong(x.index()).putVarLong(x.hintIndex()).putVarLong(x.hintTerm());
            case Heartbeat x -> w.putVarLong(x.commit()).putVarLong(x.readCtx());
            case HeartbeatResponse x -> w.putVarLong(x.readCtx());
            case InstallSnapshot x -> {
                x.meta().encode(w);
                w.putVarLong(x.offset()).putVarLong(x.totalSize()).putBytes(x.chunk());
            }
            case InstallSnapshotResponse x -> w.putVarLong(x.snapshotIndex()).putVarLong(x.nextOffset()).putBool(x.done());
            case TimeoutNow x -> { }
        }
        return w.toByteArray();
    }

    public static Message decode(byte[] b) {
        ByteReader r = new ByteReader(b);
        int type = r.getByte();
        long from = r.getVarLong(), to = r.getVarLong(), term = r.getVarLong();
        Message m = switch (type) {
            case 1 -> new RequestVote(from, to, term, r.getVarLong(), r.getVarLong(), r.getBool(), r.getBool());
            case 2 -> new RequestVoteResponse(from, to, term, r.getBool(), r.getBool());
            case 3 -> {
                long prevIndex = r.getVarLong(), prevTerm = r.getVarLong(), commit = r.getVarLong();
                int n = r.getVarInt();
                List<Entry> ents = new ArrayList<>(n);
                for (int i = 0; i < n; i++) ents.add(RaftStorage.decodeEntry(r.getBytes()));
                yield new AppendEntries(from, to, term, prevIndex, prevTerm, List.copyOf(ents), commit);
            }
            case 4 -> new AppendEntriesResponse(from, to, term, r.getBool(), r.getVarLong(), r.getVarLong(), r.getVarLong());
            case 5 -> new Heartbeat(from, to, term, r.getVarLong(), r.getVarLong());
            case 6 -> new HeartbeatResponse(from, to, term, r.getVarLong());
            case 7 -> {
                SnapshotMeta meta = SnapshotMeta.decode(r);
                long offset = r.getVarLong(), total = r.getVarLong();
                yield new InstallSnapshot(from, to, term, meta, offset, r.getBytes(), total);
            }
            case 8 -> new InstallSnapshotResponse(from, to, term, r.getVarLong(), r.getVarLong(), r.getBool());
            case 9 -> new TimeoutNow(from, to, term);
            default -> throw new CorruptionException("unknown message type " + type);
        };
        if (r.hasRemaining()) throw new CorruptionException("trailing bytes in message type " + type);
        return m;
    }
}
