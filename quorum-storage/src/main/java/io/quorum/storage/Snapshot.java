package io.quorum.storage;

/** A consistent point-in-time view. Old versions it can see survive compaction until closed. */
public final class Snapshot implements AutoCloseable {
    private final long sequence;
    private final Runnable release;
    private boolean released;

    Snapshot(long sequence, Runnable release) {
        this.sequence = sequence;
        this.release = release;
    }

    public long sequence() {
        return sequence;
    }

    @Override
    public synchronized void close() {
        if (!released) {
            released = true;
            release.run();
        }
    }
}
