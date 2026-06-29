package io.quorum.storage;

import io.quorum.common.FileSystem;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Keeps open readers (with their in-memory index and filter) keyed by file number. */
final class TableCache implements AutoCloseable {
    private final FileSystem fs;
    private final String dir;
    private final BlockCache blockCache;
    private final Map<Long, SSTableReader> open = new HashMap<>();
    final AtomicLong bloomUseful = new AtomicLong();

    TableCache(FileSystem fs, String dir, BlockCache blockCache) {
        this.fs = fs;
        this.dir = dir;
        this.blockCache = blockCache;
    }

    synchronized SSTableReader get(long number) {
        return open.computeIfAbsent(number, n ->
                new SSTableReader(fs.open(FileSystem.join(dir, FileNames.table(n)), false), n, blockCache, bloomUseful));
    }

    synchronized void evict(long number) {
        SSTableReader r = open.remove(number);
        if (r != null) r.close();
        blockCache.evictFile(number);
    }

    @Override
    public synchronized void close() {
        open.values().forEach(SSTableReader::close);
        open.clear();
    }
}
