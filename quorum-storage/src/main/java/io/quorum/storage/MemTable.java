package io.quorum.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/** Sorted in-memory write buffer. Concurrent readers, one writer at a time (the commit leader). */
final class MemTable {
    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(InternalKey.COMPARATOR);
    private final AtomicLong bytes = new AtomicLong();
    private static final byte[] TOMBSTONE = new byte[0];

    void add(long seq, int type, byte[] key, byte[] value) {
        byte[] ik = InternalKey.of(key, seq, type);
        byte[] v = type == InternalKey.TYPE_DELETE ? TOMBSTONE : value;
        map.put(ik, v);
        bytes.addAndGet(ik.length + v.length + 32);
    }

    long approximateBytes() {
        return bytes.get();
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int entries() {
        return map.size();
    }

    /** Newest version of {@code key} with sequence ≤ {@code seq}, or null if none here. */
    Lookup get(byte[] key, long seq) {
        Map.Entry<byte[], byte[]> e = map.ceilingEntry(InternalKey.seekKey(key, seq));
        if (e == null || !InternalKey.sameUserKey(e.getKey(), key)) return null;
        return InternalKey.type(e.getKey()) == InternalKey.TYPE_DELETE ? Lookup.DELETED : Lookup.found(e.getValue());
    }

    InternalIterator iterator() {
        return new InternalIterator() {
            private Iterator<Map.Entry<byte[], byte[]>> it;
            private Map.Entry<byte[], byte[]> cur;

            private void from(NavigableMap<byte[], byte[]> m) {
                it = m.entrySet().iterator();
                cur = it.hasNext() ? it.next() : null;
            }

            @Override
            public boolean valid() {
                return cur != null;
            }

            @Override
            public void seekToFirst() {
                from(map);
            }

            @Override
            public void seek(byte[] target) {
                from(map.tailMap(target, true));
            }

            @Override
            public void next() {
                cur = it.hasNext() ? it.next() : null;
            }

            @Override
            public byte[] key() {
                return cur.getKey();
            }

            @Override
            public byte[] value() {
                return cur.getValue();
            }
        };
    }
}
