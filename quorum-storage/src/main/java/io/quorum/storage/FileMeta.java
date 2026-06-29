package io.quorum.storage;

/** An SSTable as recorded in the manifest. Keys are internal keys. */
record FileMeta(long number, long size, byte[] smallest, byte[] largest, long entries) {
    boolean containsUserKey(byte[] userKey) {
        return InternalKey.compareUserKey(smallest, userKey) <= 0 && InternalKey.compareUserKey(largest, userKey) >= 0;
    }

    /** Overlaps the inclusive user-key range {@code [lo, hi]}; null bounds are unbounded. */
    boolean overlaps(byte[] loUser, byte[] hiUser) {
        if (hiUser != null && InternalKey.compareUserKey(smallest, hiUser) > 0) return false;
        return loUser == null || InternalKey.compareUserKey(largest, loUser) >= 0;
    }
}
