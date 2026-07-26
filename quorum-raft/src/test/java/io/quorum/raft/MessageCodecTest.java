package io.quorum.raft;

import io.quorum.common.Bytes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecTest {
    @Test
    void roundTripsEveryMessageType() {
        SnapshotMeta meta = new SnapshotMeta(10, 3, ConfState.of(1, 2, 3));
        List<Message> all = List.of(
                new Message.RequestVote(1, 2, 5, 10, 4, true, false),
                new Message.RequestVoteResponse(2, 1, 5, true, true),
                new Message.AppendEntries(1, 2, 5, 9, 4, List.of(Entry.normal(5, 10, Bytes.utf8("x")),
                        new Entry(5, 11, Entry.Type.CONF_CHANGE, new ConfChange(ConfChange.Type.ADD_LEARNER, 4).encode())), 8),
                new Message.AppendEntriesResponse(2, 1, 5, false, 9, 7, 3),
                new Message.Heartbeat(1, 3, 5, 8, 42),
                new Message.HeartbeatResponse(3, 1, 5, 42),
                new Message.InstallSnapshot(1, 3, 5, meta, 100, Bytes.utf8("chunk"), 1000),
                new Message.InstallSnapshotResponse(3, 1, 5, 10, 105, false),
                new Message.TimeoutNow(1, 2, 5));
        for (Message m : all) {
            Message back = MessageCodec.decode(MessageCodec.encode(m));
            if (m instanceof Message.InstallSnapshot is) {
                Message.InstallSnapshot b = (Message.InstallSnapshot) back;
                assertEquals(is.meta(), b.meta());
                assertArrayEquals(is.chunk(), b.chunk());
            } else {
                assertEquals(m, back);
            }
        }
    }
}
