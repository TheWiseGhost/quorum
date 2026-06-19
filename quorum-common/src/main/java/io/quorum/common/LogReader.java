package io.quorum.common;


import java.util.ArrayList;
import java.util.List;

/**
 * Reads records written by {@link LogWriter}. A torn or partial frame at the tail is expected
 * after a crash and ends the log. A bad frame followed by a valid frame cannot be produced by a
 * crash and is reported as corruption.
 */
public final class LogReader {
    private LogReader() {}

    /** @param validLength offset just past the last intact record */
    public record Result(List<byte[]> records, long validLength, boolean tornTail) {}

    public static Result readAll(FileHandle file) {
        byte[] data = file.readAll();
        List<byte[]> out = new ArrayList<>();
        int pos = 0;
        while (pos < data.length) {
            byte[] rec = frameAt(data, pos);
            if (rec == null) {
                if (validFrameFollows(data, pos)) {
                    throw new CorruptionException("corrupt log record at offset " + pos + " followed by valid data");
                }
                return new Result(out, pos, true);
            }
            out.add(rec);
            pos += LogWriter.HEADER + rec.length;
        }
        return new Result(out, pos, false);
    }

    private static byte[] frameAt(byte[] data, int pos) {
        if (data.length - pos < LogWriter.HEADER) return null;
        int len = Bytes.getIntLE(data, pos + 4);
        if (len < 0 || len > data.length - pos - LogWriter.HEADER) return null;
        if (data[pos + 8] != LogWriter.FULL) return null;
        int crc = Crc.crc32c(data, pos + 8, len + 1);
        if (Crc.unmask(Bytes.getIntLE(data, pos)) != crc) return null;
        byte[] rec = new byte[len];
        System.arraycopy(data, pos + LogWriter.HEADER, rec, 0, len);
        return rec;
    }

    /** True if the bad frame's declared length lands on another valid frame. */
    private static boolean validFrameFollows(byte[] data, int pos) {
        if (data.length - pos < LogWriter.HEADER) return false;
        int len = Bytes.getIntLE(data, pos + 4);
        if (len < 0 || len > data.length - pos - LogWriter.HEADER) return false;
        int next = pos + LogWriter.HEADER + len;
        return next < data.length && frameAt(data, next) != null;
    }
}
