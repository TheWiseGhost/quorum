package io.quorum.storage;

/** Result of probing one layer of the LSM tree for a key. */
record Lookup(boolean deleted, byte[] value) {
    static final Lookup DELETED = new Lookup(true, null);

    static Lookup found(byte[] value) {
        return new Lookup(false, value);
    }
}
