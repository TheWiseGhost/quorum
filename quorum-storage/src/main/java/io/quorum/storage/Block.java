package io.quorum.storage;

import io.quorum.common.ByteReader;
import io.quorum.common.Bytes;
import io.quorum.common.CorruptionException;

import java.util.Arrays;
import java.util.Comparator;

/** Immutable, parsed view of a block produced by {@link BlockBuilder}. */
final class Block {
    private final byte[] data;
    private final int restartsOffset;
    private final int numRestarts;

    Block(byte[] data) {
        if (data.length < 4) throw new CorruptionException("block too small");
        this.data = data;
        this.numRestarts = Bytes.getIntLE(data, data.length - 4);
        this.restartsOffset = data.length - 4 - numRestarts * 4;
        if (numRestarts < 1 || restartsOffset < 0) throw new CorruptionException("bad restart array");
    }

    int sizeInBytes() {
        return data.length;
    }

    private int restartPoint(int i) {
        return Bytes.getIntLE(data, restartsOffset + i * 4);
    }

    Iter iterator(Comparator<byte[]> cmp) {
        return new Iter(cmp);
    }

    final class Iter implements InternalIterator {
        private final Comparator<byte[]> cmp;
        private int next; // offset of the entry after the current one
        private byte[] key;
        private byte[] value;
        private boolean valid;

        Iter(Comparator<byte[]> cmp) {
            this.cmp = cmp;
        }

        private void seekToRestart(int i) {
            next = restartPoint(i);
            key = new byte[0];
            valid = false;
        }

        /** Decodes the entry at {@code next}; returns false at end of block. */
        private boolean parseNext() {
            if (next >= restartsOffset) {
                valid = false;
                return false;
            }
            ByteReader r = new ByteReader(data, next, restartsOffset - next);
            int shared = r.getVarInt();
            int unshared = r.getVarInt();
            int vlen = r.getVarInt();
            if (shared > key.length) throw new CorruptionException("bad shared prefix");
            byte[] k = Arrays.copyOf(key, shared + unshared);
            System.arraycopy(data, r.position(), k, shared, unshared);
            int voff = r.position() + unshared;
            value = Arrays.copyOfRange(data, voff, voff + vlen);
            key = k;
            next = voff + vlen;
            valid = true;
            return true;
        }

        @Override
        public boolean valid() {
            return valid;
        }

        @Override
        public void seekToFirst() {
            seekToRestart(0);
            parseNext();
        }

        @Override
        public void seek(byte[] target) {
            // Binary search for the last restart whose key is < target.
            int lo = 0, hi = numRestarts - 1;
            while (lo < hi) {
                int mid = (lo + hi + 1) >>> 1;
                seekToRestart(mid);
                parseNext();
                if (cmp.compare(key, target) < 0) lo = mid;
                else hi = mid - 1;
            }
            seekToRestart(lo);
            while (parseNext()) {
                if (cmp.compare(key, target) >= 0) return;
            }
        }

        @Override
        public void next() {
            parseNext();
        }

        @Override
        public byte[] key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }
    }
}
