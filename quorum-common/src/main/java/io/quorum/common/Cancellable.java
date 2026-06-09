package io.quorum.common;

@FunctionalInterface
public interface Cancellable {
    void cancel();

    Cancellable NONE = () -> {};
}
