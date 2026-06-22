package io.quorum.storage;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.CorruptionException;

import java.util.ArrayList;
import java.util.List;

/** An ordered list of puts and deletes applied atomically. */
public final class WriteBatch {
    public record Op(int type, byte[] key, byte[] value) {}

    private final List<Op> ops = new ArrayList<>();
    private long approximateBytes;

    public WriteBatch put(byte[] key, byte[] value) {
        ops.add(new Op(InternalKey.TYPE_PUT, key, value));
        approximateBytes += key.length + value.length + 8;
        return this;
    }

    public WriteBatch delete(byte[] key) {
        ops.add(new Op(InternalKey.TYPE_DELETE, key, null));
        approximateBytes += key.length + 8;
        return this;
    }

    public WriteBatch append(WriteBatch other) {
        ops.addAll(other.ops);
        approximateBytes += other.approximateBytes;
        return this;
    }

    public List<Op> ops() {
        return ops;
    }

    public int count() {
        return ops.size();
    }

    public boolean isEmpty() {
        return ops.isEmpty();
    }

    public long approximateBytes() {
        return approximateBytes;
    }

    /** Log record format: {@code baseSeq:u64 | count:u32 | (type:u8 | key | [value])*}. */
    byte[] encode(long baseSeq) {
        ByteWriter w = new ByteWriter((int) Math.min(Integer.MAX_VALUE - 64, approximateBytes + 16));
        w.putLong(baseSeq).putInt(ops.size());
        for (Op op : ops) {
            w.putByte(op.type()).putBytes(op.key());
            if (op.type() == InternalKey.TYPE_PUT) w.putBytes(op.value());
        }
        return w.toByteArray();
    }

    record Decoded(long baseSeq, WriteBatch batch) {}

    static Decoded decode(byte[] record) {
        ByteReader r = new ByteReader(record);
        long seq = r.getLong();
        int count = r.getInt();
        WriteBatch b = new WriteBatch();
        for (int i = 0; i < count; i++) {
            int type = r.getByte();
            byte[] key = r.getBytes();
            switch (type) {
                case InternalKey.TYPE_PUT -> b.put(key, r.getBytes());
                case InternalKey.TYPE_DELETE -> b.delete(key);
                default -> throw new CorruptionException("bad op type " + type);
            }
        }
        if (r.hasRemaining()) throw new CorruptionException("trailing bytes in batch");
        return new Decoded(seq, b);
    }
}
