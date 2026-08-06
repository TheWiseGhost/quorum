package io.quorum.kv;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.CorruptionException;

import java.util.ArrayList;
import java.util.List;

/** Wire format for client requests and responses, each tagged with a request id for pipelining. */
public final class ClientCodec {
    private ClientCodec() {}

    public record Framed<T>(long requestId, T body) {}

    public static byte[] encodeRequest(long requestId, Request req) {
        ByteWriter w = new ByteWriter(64).putVarLong(requestId);
        switch (req) {
            case Request.Get g -> w.putByte(1).putBytes(g.key());
            case Request.Scan s -> w.putByte(2).putNullableBytes(s.from()).putNullableBytes(s.to()).putVarInt(s.limit());
            case Request.Write wr -> w.putByte(3).putBytes(wr.command().encode());
            case Request.AddLearner a -> w.putByte(4).putVarLong(a.nodeId());
            case Request.Promote p -> w.putByte(5).putVarLong(p.nodeId());
            case Request.Remove r -> w.putByte(6).putVarLong(r.nodeId());
            case Request.TransferLeader t -> w.putByte(7).putVarLong(t.nodeId());
            case Request.Status s -> w.putByte(8);
        }
        return w.toByteArray();
    }

    public static Framed<Request> decodeRequest(byte[] b) {
        ByteReader r = new ByteReader(b);
        long id = r.getVarLong();
        int t = r.getByte();
        Request req = switch (t) {
            case 1 -> new Request.Get(r.getBytes());
            case 2 -> new Request.Scan(r.getNullableBytes(), r.getNullableBytes(), r.getVarInt());
            case 3 -> new Request.Write(Command.decode(r.getBytes()));
            case 4 -> new Request.AddLearner(r.getVarLong());
            case 5 -> new Request.Promote(r.getVarLong());
            case 6 -> new Request.Remove(r.getVarLong());
            case 7 -> new Request.TransferLeader(r.getVarLong());
            case 8 -> new Request.Status();
            default -> throw new CorruptionException("bad request type " + t);
        };
        return new Framed<>(id, req);
    }

    public static byte[] encodeResponse(long requestId, Response resp) {
        ByteWriter w = new ByteWriter(64).putVarLong(requestId).putByte(resp.code().ordinal())
                .putVarLong(resp.leaderHint()).putBool(resp.ok()).putNullableBytes(resp.value())
                .putVarInt(resp.rows().size());
        for (KvStateMachine.Row row : resp.rows()) w.putBytes(row.key()).putBytes(row.value());
        w.putString(resp.message());
        return w.toByteArray();
    }

    public static Framed<Response> decodeResponse(byte[] b) {
        ByteReader r = new ByteReader(b);
        long id = r.getVarLong();
        Response.Code code = Response.Code.values()[r.getByte()];
        long hint = r.getVarLong();
        boolean ok = r.getBool();
        byte[] value = r.getNullableBytes();
        int n = r.getVarInt();
        List<KvStateMachine.Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rows.add(new KvStateMachine.Row(r.getBytes(), r.getBytes()));
        return new Framed<>(id, new Response(code, hint, ok, value, rows, r.getString()));
    }
}
