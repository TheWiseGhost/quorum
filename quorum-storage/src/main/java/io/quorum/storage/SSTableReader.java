package io.quorum.storage;

import io.quorum.common.ByteReader;
import io.quorum.common.Bytes;
import io.quorum.common.CorruptionException;
import io.quorum.common.Crc;
import io.quorum.common.FileHandle;

import java.util.concurrent.atomic.AtomicLong;

/** Reads a table written by {@link SSTableWriter}. Index and filter stay in memory. */
final class SSTableReader implements AutoCloseable {
    private final FileHandle file;
    private final long number;
    private final Block index;
    private final byte[] filter;
    private final BlockCache cache;
    private final AtomicLong bloomUseful;

    SSTableReader(FileHandle file, long number, BlockCache cache, AtomicLong bloomUseful) {
        this.file = file;
        this.number = number;
        this.cache = cache;
        this.bloomUseful = bloomUseful;
        long size = file.size();
        if (size < SSTableWriter.FOOTER_SIZE) throw new CorruptionException("table " + number + " too small");
        ByteReader footer = new ByteReader(file.readFully(size - SSTableWriter.FOOTER_SIZE, SSTableWriter.FOOTER_SIZE));
        long filterOff = footer.getLong(), filterLen = footer.getLong();
        long indexOff = footer.getLong(), indexLen = footer.getLong();
        footer.getLong(); // entries
        if (footer.getLong() != SSTableWriter.MAGIC) throw new CorruptionException("bad magic in table " + number);
        this.filter = readChecked(filterOff, (int) filterLen);
        this.index = new Block(readChecked(indexOff, (int) indexLen));
    }

    private byte[] readChecked(long off, int len) {
        byte[] raw = file.readFully(off, len + 4);
        int stored = Crc.unmask(Bytes.getIntLE(raw, len));
        if (stored != Crc.crc32c(raw, 0, len)) {
            throw new CorruptionException("checksum mismatch in table " + number + " at " + off);
        }
        return java.util.Arrays.copyOf(raw, len);
    }

    private Block dataBlock(byte[] handle) {
        ByteReader r = new ByteReader(handle);
        long off = r.getVarLong();
        int len = (int) r.getVarLong();
        Block b = cache.get(number, off);
        if (b == null) {
            b = new Block(readChecked(off, len));
            cache.put(number, off, b);
        }
        return b;
    }

    /** Newest entry for {@code userKey} visible at {@code seq}, or null if absent from this table. */
    Lookup get(byte[] userKey, long seq) {
        if (!BloomFilter.mayContain(filter, BloomFilter.hash(userKey))) {
            bloomUseful.incrementAndGet();
            return null;
        }
        byte[] target = InternalKey.seekKey(userKey, seq);
        Block.Iter idx = index.iterator(InternalKey.COMPARATOR);
        idx.seek(target);
        if (!idx.valid()) return null;
        Block.Iter it = dataBlock(idx.value()).iterator(InternalKey.COMPARATOR);
        it.seek(target);
        if (!it.valid() || !InternalKey.sameUserKey(it.key(), userKey)) return null;
        return InternalKey.type(it.key()) == InternalKey.TYPE_DELETE ? Lookup.DELETED : Lookup.found(it.value());
    }

    /** Two-level iterator: index block selects data blocks lazily. */
    InternalIterator iterator() {
        return new InternalIterator() {
            private final Block.Iter idx = index.iterator(InternalKey.COMPARATOR);
            private Block.Iter data;

            private void loadBlock() {
                data = idx.valid() ? dataBlock(idx.value()).iterator(InternalKey.COMPARATOR) : null;
            }

            private void skipEmpty() {
                while (data != null && !data.valid()) {
                    idx.next();
                    loadBlock();
                    if (data != null) data.seekToFirst();
                }
            }

            @Override
            public boolean valid() {
                return data != null && data.valid();
            }

            @Override
            public void seekToFirst() {
                idx.seekToFirst();
                loadBlock();
                if (data != null) data.seekToFirst();
                skipEmpty();
            }

            @Override
            public void seek(byte[] target) {
                idx.seek(target);
                loadBlock();
                if (data != null) data.seek(target);
                skipEmpty();
            }

            @Override
            public void next() {
                data.next();
                skipEmpty();
            }

            @Override
            public byte[] key() {
                return data.key();
            }

            @Override
            public byte[] value() {
                return data.value();
            }
        };
    }

    @Override
    public void close() {
        file.close();
    }
}
