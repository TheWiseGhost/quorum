package io.quorum.common;

/** Durable data failed a checksum or structural check. Never silently ignored. */
public final class CorruptionException extends RuntimeException {
    public CorruptionException(String message) {
        super(message);
    }
}
