package io.quorum.storage;

import io.quorum.common.Bytes;

import java.util.NoSuchElementException;

/** Turns the internal merged stream into user entries visible at a snapshot within bounds. */
final class DbIterator implements KvIterator {
    private final InternalIterator it;
    private final long seq;
    private final byte[] to;
    private final Runnable onClose;
    private byte[] lastUserKey;
    private Entry pending;
    private boolean closed;

    DbIterator(InternalIterator it, long seq, byte[] from, byte[] to, Runnable onClose) {
        this.it = it;
        this.seq = seq;
        this.to = to;
        this.onClose = onClose;
        if (from == null) it.seekToFirst();
        else it.seek(InternalKey.seekKey(from, InternalKey.MAX_SEQUENCE));
        advance();
    }

    private void advance() {
        pending = null;
        while (it.valid()) {
            byte[] ik = it.key();
            if (InternalKey.sequence(ik) > seq) {
                it.next();
                continue;
            }
            if (to != null && InternalKey.compareUserKey(ik, to) >= 0) return;
            if (lastUserKey != null && InternalKey.sameUserKey(ik, lastUserKey)) {
                it.next();
                continue;
            }
            lastUserKey = InternalKey.userKey(ik);
            boolean deleted = InternalKey.type(ik) == InternalKey.TYPE_DELETE;
            byte[] value = it.value();
            it.next();
            if (!deleted) {
                pending = new Entry(lastUserKey, value);
                return;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return pending != null;
    }

    @Override
    public Entry next() {
        if (pending == null) throw new NoSuchElementException();
        Entry e = pending;
        advance();
        return e;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            it.close();
            onClose.run();
        }
    }

    @Override
    public String toString() {
        return "DbIterator@" + (pending == null ? "end" : Bytes.str(pending.key()));
    }
}
