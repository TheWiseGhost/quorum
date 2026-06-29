package io.quorum.storage;

import io.quorum.common.ByteWriter;
import io.quorum.common.Bytes;
import io.quorum.common.Crc;
import io.quorum.common.FileHandle;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Writes an immutable sorted table:
 * <pre>
 * [data block]* [filter block] [index block] [footer]
 * </pre>
 * Every block is followed by a masked CRC32C. The index maps the last key of each data block
 * to its handle. Footer (48 bytes): filterOff, filterLen, indexOff, indexLen, entries, magic.
 */
final class SSTableWriter {
    static final long MAGIC = 0x51554f52554d5354L; // "QUORUMST"
    static final int FOOTER_SIZE = 48;

    private final FileHandle file;
    private final Options options;
    private final BlockBuilder data;
    private final BlockBuilder index;
    private long[] hashes = new long[1024];
    private int hashCount;
    private byte[] lastUserKey;
    private long offset;
    private long entries;
    private byte[] smallest, largest;

    SSTableWriter(FileHandle file, Options options) {
        this.file = file;
        this.options = options;
        this.data = new BlockBuilder(options.blockRestartInterval());
        this.index = new BlockBuilder(1);
    }

    void add(byte[] internalKey, byte[] value) {
        if (smallest == null) smallest = internalKey;
        largest = internalKey;
        entries++;
        int ulen = internalKey.length - 8;
        if (lastUserKey == null || !Arrays.equals(lastUserKey, 0, lastUserKey.length, internalKey, 0, ulen)) {
            lastUserKey = Arrays.copyOf(internalKey, ulen);
            if (hashCount == hashes.length) hashes = Arrays.copyOf(hashes, hashCount * 2);
            hashes[hashCount++] = BloomFilter.hash(lastUserKey);
        }
        data.add(internalKey, value);
        if (data.sizeEstimate() >= options.blockSize()) flushDataBlock();
    }

    private void flushDataBlock() {
        if (data.isEmpty()) return;
        byte[] lastKey = data.lastKey();
        long[] handle = writeBlock(data.finish());
        index.add(lastKey, encodeHandle(handle[0], handle[1]));
        data.reset();
    }

    private long[] writeBlock(byte[] contents) {
        byte[] withCrc = Arrays.copyOf(contents, contents.length + 4);
        Bytes.putIntLE(withCrc, contents.length, Crc.mask(Crc.crc32c(contents)));
        file.write(offset, ByteBuffer.wrap(withCrc));
        long[] handle = {offset, contents.length};
        offset += withCrc.length;
        return handle;
    }

    static byte[] encodeHandle(long off, long size) {
        return new ByteWriter(16).putVarLong(off).putVarLong(size).toByteArray();
    }

    long entries() {
        return entries;
    }

    long fileSize() {
        return offset + data.sizeEstimate();
    }

    byte[] smallest() {
        return smallest;
    }

    byte[] largest() {
        return largest;
    }

    /** Writes filter, index and footer, then fsyncs. Returns the final file size. */
    long finish() {
        flushDataBlock();
        long[] filter = writeBlock(BloomFilter.build(hashes, hashCount, options.bloomBitsPerKey()));
        long[] idx = writeBlock(index.finish());
        byte[] footer = new ByteWriter(FOOTER_SIZE)
                .putLong(filter[0]).putLong(filter[1]).putLong(idx[0]).putLong(idx[1])
                .putLong(entries).putLong(MAGIC).toByteArray();
        file.write(offset, ByteBuffer.wrap(footer));
        offset += footer.length;
        file.fsync();
        return offset;
    }
}
