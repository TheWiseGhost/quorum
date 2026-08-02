package io.quorum.kv;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;

/**
 * Outcome of applying a command or serving a read.
 *
 * @param ok    false only for a failed CAS (or a stale retry)
 * @param value for reads the value (null if absent); for a failed CAS the current value
 */
public record KvResult(boolean ok, byte[] value) {
    public static final KvResult OK = new KvResult(true, null);
    public static final KvResult STALE = new KvResult(false, null);

    byte[] encode() {
        return new ByteWriter(16).putBool(ok).putNullableBytes(value).toByteArray();
    }

    static KvResult decode(ByteReader r) {
        return new KvResult(r.getBool(), r.getNullableBytes());
    }
}
