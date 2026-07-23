package io.quorum.raft;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.Bytes;
import io.quorum.common.CorruptionException;
import io.quorum.common.Crc;
import io.quorum.common.FileHandle;
import io.quorum.common.FileSystem;
import io.quorum.common.LogReader;
import io.quorum.common.LogWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Durable Raft state: a segmented append-only log ({@code log/<firstIndex>.seg}), plus the
 * hard state and latest snapshot, each replaced atomically (write temp, fsync, rename, fsync dir).
 *
 * <p>Suffix truncation fsyncs the directory before any new entry is appended, so a deleted
 * segment of stale entries can never reappear after a crash and be mistaken for valid log.
 */
public final class RaftStorage implements AutoCloseable {
    private static final String LOG_DIR = "log";
    private static final String HARD_STATE = "hardstate";
    private static final String SNAPSHOT = "snapshot";

    private static final class Segment {
        final String path;
        final long first;
        final FileHandle file;
        final LogWriter writer;
        final List<Long> offsets = new ArrayList<>(); // offset of entry (first + i)
        boolean dirty;

        Segment(String path, long first, FileHandle file) {
            this.path = path;
            this.first = first;
            this.file = file;
            this.writer = new LogWriter(file);
        }

        long last() {
            return first + offsets.size() - 1;
        }
    }

    private final FileSystem fs;
    private final String dir;
    private final long segmentBytes;
    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    private long nextIndex = 1;
    private long fsyncs;

    private RaftStorage(FileSystem fs, String dir, long segmentBytes) {
        this.fs = fs;
        this.dir = dir;
        this.segmentBytes = segmentBytes;
    }

    public record Opened(RaftStorage storage, HardState hardState, Snapshot snapshot, List<Entry> entries) {}

    public static Opened open(FileSystem fs, String dir, long segmentBytes) {
        fs.mkdirs(FileSystem.join(dir, LOG_DIR));
        RaftStorage st = new RaftStorage(fs, dir, segmentBytes);
        HardState hs = st.readHardState();
        Snapshot snap = st.readSnapshot();
        List<Entry> entries = st.loadSegments(snap == null ? 0 : snap.index());
        return new Opened(st, hs, snap, entries);
    }

    public long fsyncs() {
        return fsyncs;
    }

    // ------------------------------------------------------------------ small atomic files

    private static byte[] withCrc(byte[] payload) {
        byte[] out = new byte[payload.length + 4];
        Bytes.putIntLE(out, 0, Crc.crc32c(payload));
        System.arraycopy(payload, 0, out, 4, payload.length);
        return out;
    }

    private static byte[] checkCrc(byte[] data, String what) {
        if (data.length < 4) throw new CorruptionException(what + " truncated");
        byte[] payload = java.util.Arrays.copyOfRange(data, 4, data.length);
        if (Bytes.getIntLE(data, 0) != Crc.crc32c(payload)) throw new CorruptionException(what + " checksum mismatch");
        return payload;
    }

    private HardState readHardState() {
        String p = FileSystem.join(dir, HARD_STATE);
        if (!fs.exists(p)) return HardState.EMPTY;
        ByteReader r = new ByteReader(checkCrc(FileSystem.readFile(fs, p), "hardstate"));
        return new HardState(r.getVarLong(), r.getVarLong(), r.getVarLong());
    }

    public void saveHardState(HardState hs) {
        byte[] data = withCrc(new ByteWriter(32).putVarLong(hs.term()).putVarLong(hs.votedFor())
                .putVarLong(hs.commit()).toByteArray());
        FileSystem.writeAtomically(fs, FileSystem.join(dir, HARD_STATE), data);
        fsyncs++;
    }

    private Snapshot readSnapshot() {
        String p = FileSystem.join(dir, SNAPSHOT);
        if (!fs.exists(p)) return null;
        ByteReader r = new ByteReader(checkCrc(FileSystem.readFile(fs, p), "snapshot"));
        SnapshotMeta meta = SnapshotMeta.decode(r);
        return new Snapshot(meta, r.getBytes());
    }

    public void saveSnapshot(Snapshot s) {
        ByteWriter w = new ByteWriter(s.data().length + 64);
        s.meta().encode(w);
        w.putBytes(s.data());
        FileSystem.writeAtomically(fs, FileSystem.join(dir, SNAPSHOT), withCrc(w.toByteArray()));
        fsyncs++;
    }

    // ------------------------------------------------------------------ log segments

    static byte[] encodeEntry(Entry e) {
        return new ByteWriter(e.data().length + 24).putVarLong(e.term()).putVarLong(e.index())
                .putByte(e.type().ordinal()).putBytes(e.data()).toByteArray();
    }

    static Entry decodeEntry(byte[] b) {
        ByteReader r = new ByteReader(b);
        return new Entry(r.getVarLong(), r.getVarLong(), Entry.Type.values()[r.getByte()], r.getBytes());
    }

    private List<Entry> loadSegments(long snapIndex) {
        String logDir = FileSystem.join(dir, LOG_DIR);
        TreeMap<Long, String> files = new TreeMap<>();
        for (String name : fs.list(logDir)) {
            if (name.endsWith(".seg")) files.put(Long.parseLong(name.substring(0, name.length() - 4)), name);
        }
        List<Entry> out = new ArrayList<>();
        long expected = -1;
        boolean stop = false;
        for (Map.Entry<Long, String> f : files.entrySet()) {
            String path = FileSystem.join(logDir, f.getValue());
            if (stop) {
                fs.delete(path); // stale leftovers after a gap or torn segment
                continue;
            }
            FileHandle h = fs.open(path, false);
            LogReader.Result r = LogReader.readAll(h);
            Segment seg = new Segment(path, f.getKey(), h);
            long offset = 0;
            for (byte[] rec : r.records()) {
                Entry e = decodeEntry(rec);
                if (e.index() != seg.first + seg.offsets.size() || (expected >= 0 && e.index() != expected)) {
                    stop = true;
                    break;
                }
                seg.offsets.add(offset);
                offset += LogWriter.HEADER + rec.length;
                expected = e.index() + 1;
                if (e.index() > snapIndex) out.add(e);
            }
            if (stop || r.tornTail() || offset < h.size()) {
                h.truncate(offset);
                h.fsync();
                stop = true;
            }
            if (seg.offsets.isEmpty()) {
                h.close();
                fs.delete(path);
                continue;
            }
            seg.writer.close(); // reopen writer at the (possibly truncated) end
            Segment fresh = new Segment(path, seg.first, fs.open(path, false));
            fresh.offsets.addAll(seg.offsets);
            segments.put(fresh.first, fresh);
        }
        fs.syncDir(logDir);
        // Entries must continue right after the snapshot; anything else is unusable.
        List<Entry> usable = new ArrayList<>();
        long want = snapIndex + 1;
        for (Entry e : out) {
            if (e.index() != want) break;
            usable.add(e);
            want++;
        }
        nextIndex = segments.isEmpty() ? snapIndex + 1 : segments.lastEntry().getValue().last() + 1;
        if (usable.isEmpty()) resetTo(snapIndex);
        else if (usable.getLast().index() + 1 != nextIndex) truncateFrom(usable.getLast().index() + 1);
        return usable;
    }

    public long lastIndex() {
        return nextIndex - 1;
    }

    /** Appends entries, first truncating any existing suffix they overwrite. Not durable until {@link #sync()}. */
    public void append(List<Entry> entries) {
        if (entries.isEmpty()) return;
        long first = entries.getFirst().index();
        if (first < nextIndex) truncateFrom(first);
        if (first != nextIndex) throw new IllegalStateException("log gap: next " + nextIndex + " got " + first);
        for (Entry e : entries) {
            Segment seg = segments.isEmpty() ? null : segments.lastEntry().getValue();
            if (seg == null || seg.writer.size() >= segmentBytes) seg = newSegment(e.index());
            long off = seg.writer.size();
            seg.writer.add(encodeEntry(e));
            seg.offsets.add(off);
            seg.dirty = true;
            nextIndex = e.index() + 1;
        }
    }

    private Segment newSegment(long first) {
        String logDir = FileSystem.join(dir, LOG_DIR);
        String path = FileSystem.join(logDir, String.format("%020d.seg", first));
        Segment seg = new Segment(path, first, fs.open(path, true));
        fs.syncDir(logDir);
        segments.put(first, seg);
        return seg;
    }

    private void truncateFrom(long index) {
        String logDir = FileSystem.join(dir, LOG_DIR);
        boolean deleted = false;
        while (!segments.isEmpty() && segments.lastKey() >= index) {
            Segment s = segments.pollLastEntry().getValue();
            s.file.close();
            fs.delete(s.path);
            deleted = true;
        }
        if (!segments.isEmpty()) {
            Segment s = segments.lastEntry().getValue();
            if (s.last() >= index) {
                int keep = (int) (index - s.first);
                long off = s.offsets.get(keep);
                while (s.offsets.size() > keep) s.offsets.removeLast();
                s.writer.close();
                Segment re = new Segment(s.path, s.first, fs.open(s.path, false));
                re.file.truncate(off);
                re.file.fsync();
                re.offsets.addAll(s.offsets);
                segments.put(re.first, re);
            }
        }
        if (deleted) fs.syncDir(logDir);
        nextIndex = Math.min(nextIndex, index);
    }

    /** Durably persists all appended entries. */
    public void sync() {
        for (Segment s : segments.values()) {
            if (s.dirty) {
                s.file.fsync();
                s.dirty = false;
                fsyncs++;
            }
        }
    }

    /** Drops whole segments that lie entirely at or below {@code index} (after a snapshot). */
    public void compactTo(long index) {
        boolean deleted = false;
        while (segments.size() > 1) {
            Map.Entry<Long, Segment> firstSeg = segments.firstEntry();
            Long nextFirst = segments.higherKey(firstSeg.getKey());
            if (nextFirst == null || nextFirst - 1 > index) break;
            Segment s = segments.pollFirstEntry().getValue();
            s.file.close();
            fs.delete(s.path);
            deleted = true;
        }
        if (deleted) fs.syncDir(FileSystem.join(dir, LOG_DIR));
    }

    /** Discards the whole log; the next append must start at {@code snapshotIndex + 1}. */
    public void resetTo(long snapshotIndex) {
        for (Segment s : segments.values()) {
            s.file.close();
            fs.delete(s.path);
        }
        segments.clear();
        fs.syncDir(FileSystem.join(dir, LOG_DIR));
        nextIndex = snapshotIndex + 1;
    }

    @Override
    public void close() {
        for (Segment s : segments.values()) s.file.close();
        segments.clear();
    }
}
