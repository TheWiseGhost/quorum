package io.quorum.storage;

/**
 * Counters for write/space amplification.
 *
 * @param userBytes      key+value bytes submitted by callers
 * @param walBytes       bytes appended to the write-ahead log
 * @param flushBytes     bytes written by memtable flushes (L0 tables)
 * @param compactionReadBytes  bytes read by compactions
 * @param compactionWriteBytes bytes written by compactions
 * @param filesPerLevel  table count per level
 * @param bytesPerLevel  table bytes per level
 */
public record EngineStats(long userBytes, long walBytes, long flushBytes, long compactionReadBytes,
                          long compactionWriteBytes, long compactions, long flushes, int[] filesPerLevel,
                          long[] bytesPerLevel, long blockCacheHits, long blockCacheMisses,
                          long bloomUseful) {
    /** Bytes written to tables per user byte (excludes WAL). */
    public double writeAmplification() {
        return userBytes == 0 ? 0 : (double) (flushBytes + compactionWriteBytes) / userBytes;
    }

    public long liveTableBytes() {
        long t = 0;
        for (long b : bytesPerLevel) t += b;
        return t;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("user=%,dB wal=%,dB flush=%,dB compactW=%,dB compactR=%,dB WA=%.2f flushes=%d compactions=%d%n",
                userBytes, walBytes, flushBytes, compactionWriteBytes, compactionReadBytes, writeAmplification(),
                flushes, compactions));
        for (int i = 0; i < filesPerLevel.length; i++) {
            if (filesPerLevel[i] > 0) sb.append(String.format("  L%d: %d files %,d bytes%n", i, filesPerLevel[i], bytesPerLevel[i]));
        }
        sb.append(String.format("  block cache hits=%d misses=%d, bloom filtered=%d", blockCacheHits, blockCacheMisses, bloomUseful));
        return sb.toString();
    }
}
