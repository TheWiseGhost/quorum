package io.quorum.raft;

import java.util.List;

/**
 * A batch of work for the driver. Required order:
 * <ol>
 *   <li>send {@link #sendNow()} (leader replication; safe before the local disk write)</li>
 *   <li>persist {@link #snapshot()}, {@link #entries()} and {@link #hardState()}, then fsync</li>
 *   <li>send {@link #sendAfterPersist()} (votes and acknowledgements promise durability)</li>
 *   <li>apply {@link #snapshot()} then {@link #committed()} to the state machine</li>
 *   <li>serve {@link #readStates()} once applied, fail {@link #droppedReads()}</li>
 *   <li>call {@link RaftNode#advance(Ready)}</li>
 * </ol>
 */
public record Ready(HardState hardState, List<Entry> entries, Snapshot snapshot, List<Entry> committed,
                    List<Message> sendNow, List<Message> sendAfterPersist, List<ReadState> readStates,
                    List<Long> droppedReads) {}
