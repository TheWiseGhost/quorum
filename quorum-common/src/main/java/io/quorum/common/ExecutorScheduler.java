package io.quorum.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Real-mode {@link Scheduler} backed by a scheduled thread pool. */
public final class ExecutorScheduler implements Scheduler, AutoCloseable {
    private final ScheduledExecutorService executor;

    public ExecutorScheduler(String name, int threads) {
        this.executor = Executors.newScheduledThreadPool(threads, Thread.ofPlatform().name(name + "-", 0).daemon().factory());
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(guarded(task));
    }

    @Override
    public Cancellable schedule(long delayNanos, Runnable task) {
        ScheduledFuture<?> f = executor.schedule(guarded(task), delayNanos, TimeUnit.NANOSECONDS);
        return () -> f.cancel(false);
    }

    /** Thread pools swallow task exceptions silently; surface them instead. */
    private static Runnable guarded(Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                System.err.println("[" + Thread.currentThread().getName() + "] task failed: " + t);
                t.printStackTrace();
                throw t;
            }
        };
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
