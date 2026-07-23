package io.quorum.raft;

import io.quorum.common.Bytes;
import io.quorum.common.SimFileSystem;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftStorageTest {
    private static List<Entry> entries(long from, long to, long term) {
        List<Entry> out = new ArrayList<>();
        for (long i = from; i <= to; i++) out.add(Entry.normal(term, i, Bytes.utf8("e" + i + "t" + term)));
        return out;
    }

    @Test
    void appendTruncateReload() {
        SimFileSystem fs = new SimFileSystem();
        RaftStorage.Opened o = RaftStorage.open(fs, "raft", 256);
        RaftStorage st = o.storage();
        st.append(entries(1, 50, 1));
        st.append(entries(30, 40, 2)); // overwrites 30..50 with 30..40 at term 2
        st.sync();
        st.saveHardState(new HardState(2, 3, 25));
        st.close();
        RaftStorage.Opened again = RaftStorage.open(fs, "raft", 256);
        assertEquals(40, again.entries().size());
        assertEquals(1, again.entries().get(28).term());
        assertEquals(2, again.entries().get(29).term());
        assertEquals(new HardState(2, 3, 25), again.hardState());
        again.storage().append(entries(41, 45, 2));
        again.storage().sync();
        again.storage().close();
        assertEquals(45, RaftStorage.open(fs, "raft", 256).entries().size());
    }

    @Test
    void snapshotThenResetAndCompact() {
        SimFileSystem fs = new SimFileSystem();
        RaftStorage st = RaftStorage.open(fs, "raft", 128).storage();
        st.append(entries(1, 100, 1));
        st.sync();
        st.saveSnapshot(new Snapshot(new SnapshotMeta(60, 1, ConfState.of(1, 2, 3)), Bytes.utf8("state@60")));
        st.compactTo(60);
        st.close();
        RaftStorage.Opened o = RaftStorage.open(fs, "raft", 128);
        assertEquals(60, o.snapshot().index());
        assertEquals("state@60", Bytes.str(o.snapshot().data()));
        assertEquals(61, o.entries().getFirst().index());
        assertEquals(100, o.entries().getLast().index());
        // Follower installs a newer snapshot from the leader: the whole log is discarded.
        o.storage().saveSnapshot(new Snapshot(new SnapshotMeta(500, 4, ConfState.of(1, 2, 3)), Bytes.utf8("s500")));
        o.storage().resetTo(500);
        o.storage().append(entries(501, 510, 4));
        o.storage().sync();
        o.storage().close();
        RaftStorage.Opened p = RaftStorage.open(fs, "raft", 128);
        assertEquals(500, p.snapshot().index());
        assertEquals(10, p.entries().size());
        assertEquals(501, p.entries().getFirst().index());
    }
}
