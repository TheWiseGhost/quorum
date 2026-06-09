package io.quorum.common;

/**
 * Where asynchronous work runs. Core code never creates threads; it asks the scheduler.
 * The simulator implements this with a single-threaded virtual-time event queue.
 */
public interface Scheduler {
    void execute(Runnable task);

    Cancellable schedule(long delayNanos, Runnable task);

    /** Runs everything inline on the calling thread. Useful for unit tests. */
    Scheduler INLINE = new Scheduler() {
        @Override
        public void execute(Runnable task) {
            task.run();
        }

        @Override
        public Cancellable schedule(long delayNanos, Runnable task) {
            throw new UnsupportedOperationException("inline scheduler cannot delay");
        }
    };
}
