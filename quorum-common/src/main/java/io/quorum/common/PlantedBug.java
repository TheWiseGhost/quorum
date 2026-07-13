package io.quorum.common;

import java.util.EnumSet;
import java.util.Set;

/**
 * Deliberately broken behaviours, off by default. The simulator's mutation suite turns each on
 * and asserts it gets caught, proving the tests have teeth.
 */
public enum PlantedBug {
    /**
     * Leader skips the election no-op and commits an entry from an earlier term by counting
     * replicas (the Raft paper's Figure 8): a later leader can then overwrite a "committed" entry.
     */
    FIGURE8_COMMIT,
    /** votedFor is never made durable, so after a crash a node can vote twice in one term. */
    VOTE_NOT_PERSISTED,
    /** Follower acknowledges AppendEntries without fsyncing the log. */
    LOG_NO_FSYNC,
    /** State machine ignores client sessions, so a retried CAS can apply twice. */
    NO_CLIENT_DEDUP,
    /** Leader serves reads from local state without confirming leadership (ReadIndex skipped). */
    LEADER_LOCAL_READ,
    /** Follower keeps conflicting entries instead of truncating its log suffix. */
    NO_TRUNCATE_CONFLICT;

    public static final Set<PlantedBug> NONE = Set.of();

    public static Set<PlantedBug> of(PlantedBug... bugs) {
        return bugs.length == 0 ? NONE : Set.copyOf(EnumSet.of(bugs[0], bugs));
    }
}
