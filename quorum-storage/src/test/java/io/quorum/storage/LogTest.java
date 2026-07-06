package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.CorruptionException;
import io.quorum.common.FileHandle;
import io.quorum.common.LogReader;
import io.quorum.common.LogWriter;
import io.quorum.common.SimFileSystem;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class LogTest {
    @Test
    void tornTailIsTruncatedSilently() {
        SimFileSystem fs = new SimFileSystem();
        try (FileHandle h = fs.open("log", true)) {
            LogWriter w = new LogWriter(h);
            for (int i = 0; i < 10; i++) w.add(Bytes.utf8("record-" + i));
            long full = h.size();
            h.truncate(full - 3); // tear the last record
            LogReader.Result r = LogReader.readAll(h);
            assertEquals(9, r.records().size());
            assertTrue(r.tornTail());
        }
    }

    @Test
    void corruptionInTheMiddleIsDetected() {
        SimFileSystem fs = new SimFileSystem();
        try (FileHandle h = fs.open("log", true)) {
            LogWriter w = new LogWriter(h);
            for (int i = 0; i < 10; i++) w.add(Bytes.utf8("record-" + i));
            byte[] one = h.readFully(20, 1);
            one[0] ^= 0x10; // flip a payload bit inside record #1
            h.write(20, ByteBuffer.wrap(one));
            assertThrows(CorruptionException.class, () -> LogReader.readAll(h));
        }
    }
}
