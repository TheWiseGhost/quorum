package io.quorum.common;

/** Source of monotonic time. Real mode reads {@link System#nanoTime()}; the simulator supplies virtual time. */
@FunctionalInterface
public interface Clock {
    long nowNanos();

    Clock SYSTEM = System::nanoTime;
}
