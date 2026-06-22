package io.quorum.storage;

/** Engine tuning knobs. Defaults follow LevelDB; tests shrink them to exercise compaction. */
public record Options(
        long writeBufferSize,
        int blockSize,
        int blockRestartInterval,
        int bloomBitsPerKey,
        long targetFileSize,
        int l0CompactionTrigger,
        int l0StopWritesTrigger,
        long maxBytesForLevelBase,
        int levelMultiplier,
        long blockCacheBytes,
        long maxManifestBytes) {

    public static final int NUM_LEVELS = 7;

    public static Options defaults() {
        return new Options(4 << 20, 4096, 16, 10, 2 << 20, 4, 12, 10L << 20, 10, 8L << 20, 4 << 20);
    }

    /** Tiny sizes so a few thousand writes exercise flushes and multi-level compaction. */
    public static Options small() {
        return new Options(16 << 10, 512, 4, 10, 16 << 10, 4, 12, 64 << 10, 4, 256 << 10, 64 << 10);
    }

    public Options withWriteBufferSize(long v) {
        return new Options(v, blockSize, blockRestartInterval, bloomBitsPerKey, targetFileSize, l0CompactionTrigger,
                l0StopWritesTrigger, maxBytesForLevelBase, levelMultiplier, blockCacheBytes, maxManifestBytes);
    }

    public Options withBloomBitsPerKey(int v) {
        return new Options(writeBufferSize, blockSize, blockRestartInterval, v, targetFileSize, l0CompactionTrigger,
                l0StopWritesTrigger, maxBytesForLevelBase, levelMultiplier, blockCacheBytes, maxManifestBytes);
    }

    public long maxBytesForLevel(int level) {
        long bytes = maxBytesForLevelBase;
        for (int i = 1; i < level; i++) bytes *= levelMultiplier;
        return bytes;
    }
}
