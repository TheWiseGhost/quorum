package io.quorum.kv;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.CorruptionException;

/**
 * A replicated write. {@code (clientId, seq)} identifies it for exactly-once semantics: the state
 * machine remembers the last sequence applied per client and answers retries from that cache.
 */
public sealed interface Command {
    long clientId();

    long seq();

    byte[] key();

    record Put(long clientId, long seq, byte[] key, byte[] value) implements Command {}

    record Delete(long clientId, long seq, byte[] key) implements Command {}

    /** Sets {@code key} to {@code newValue} (null deletes) iff its current value equals {@code expected} (null = absent). */
    record Cas(long clientId, long seq, byte[] key, byte[] expected, byte[] newValue) implements Command {}

    default byte[] encode() {
        ByteWriter w = new ByteWriter(64);
        switch (this) {
            case Put p -> w.putByte(1).putVarLong(p.clientId()).putVarLong(p.seq()).putBytes(p.key()).putBytes(p.value());
            case Delete d -> w.putByte(2).putVarLong(d.clientId()).putVarLong(d.seq()).putBytes(d.key());
            case Cas c -> w.putByte(3).putVarLong(c.clientId()).putVarLong(c.seq()).putBytes(c.key())
                    .putNullableBytes(c.expected()).putNullableBytes(c.newValue());
        }
        return w.toByteArray();
    }

    static Command decode(byte[] b) {
        ByteReader r = new ByteReader(b);
        int t = r.getByte();
        long client = r.getVarLong(), seq = r.getVarLong();
        byte[] key = r.getBytes();
        return switch (t) {
            case 1 -> new Put(client, seq, key, r.getBytes());
            case 2 -> new Delete(client, seq, key);
            case 3 -> new Cas(client, seq, key, r.getNullableBytes(), r.getNullableBytes());
            default -> throw new CorruptionException("bad command type " + t);
        };
    }
}
