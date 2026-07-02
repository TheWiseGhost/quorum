package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.CorruptionException;
import io.quorum.common.FileSystem;
import io.quorum.common.LogReader;
import io.quorum.common.LogWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Owns the current {@link Version}, the MANIFEST log, and file/sequence counters. */
final class VersionSet {
    private final FileSystem fs;
    private final String dir;
    private final Options options;
    private Version current = Version.empty();
    private final List<Version> live = new ArrayList<>();
    private final byte[][] compactPointer = new byte[Options.NUM_LEVELS][];

    long nextFileNumber = 2;
    long lastSequence;
    long logNumber;
    private long manifestNumber;
    private LogWriter manifest;

    VersionSet(FileSystem fs, String dir, Options options) {
        this.fs = fs;
        this.dir = dir;
        this.options = options;
        current.refs = 1;
        live.add(current);
    }

    Version current() {
        return current;
    }

    long manifestNumber() {
        return manifestNumber;
    }

    long newFileNumber() {
        return nextFileNumber++;
    }

    /** Loads state from CURRENT → MANIFEST. Returns false for a brand-new database. */
    boolean recover() {
        String currentPath = FileSystem.join(dir, FileNames.CURRENT);
        if (!fs.exists(currentPath)) return false;
        String name = new String(FileSystem.readFile(fs, currentPath), StandardCharsets.UTF_8).trim();
        String path = FileSystem.join(dir, name);
        if (!fs.exists(path)) throw new CorruptionException("CURRENT points to missing " + name);
        LogReader.Result r;
        try (var h = fs.open(path, false)) {
            r = LogReader.readAll(h);
        }
        Version v = Version.empty();
        boolean sawNext = false;
        for (byte[] rec : r.records()) {
            VersionEdit e = VersionEdit.decode(rec);
            if (e.logNumber != null) logNumber = e.logNumber;
            if (e.nextFileNumber != null) {
                nextFileNumber = e.nextFileNumber;
                sawNext = true;
            }
            if (e.lastSequence != null) lastSequence = e.lastSequence;
            v = v.apply(e);
        }
        if (!sawNext) throw new CorruptionException("manifest " + name + " has no file counter");
        manifestNumber = FileNames.parse(name).number();
        nextFileNumber = Math.max(nextFileNumber, manifestNumber + 1);
        install(v);
        return true;
    }

    /** Starts a fresh MANIFEST containing a full snapshot and atomically points CURRENT at it. */
    void writeSnapshotManifest() {
        if (manifest != null) manifest.close();
        manifestNumber = newFileNumber();
        String name = FileNames.manifest(manifestNumber);
        manifest = new LogWriter(fs.open(FileSystem.join(dir, name), true));
        manifest.add(snapshotEdit().encode());
        manifest.sync();
        FileSystem.writeAtomically(fs, FileSystem.join(dir, FileNames.CURRENT), Bytes.utf8(name + "\n"));
    }

    VersionEdit snapshotEdit() {
        VersionEdit e = new VersionEdit();
        e.logNumber = logNumber;
        e.nextFileNumber = nextFileNumber;
        e.lastSequence = lastSequence;
        for (int level = 0; level < Options.NUM_LEVELS; level++) {
            for (FileMeta f : current.levels.get(level)) e.addFile(level, f);
        }
        return e;
    }

    /** Durably records {@code edit} and makes the resulting version current. */
    void logAndApply(VersionEdit edit) {
        if (edit.logNumber == null) edit.logNumber = logNumber;
        edit.nextFileNumber = nextFileNumber;
        edit.lastSequence = lastSequence;
        Version next = current.apply(edit);
        if (manifest == null || manifest.size() > options.maxManifestBytes()) {
            Version prev = current;
            install(next);
            logNumber = edit.logNumber;
            try {
                writeSnapshotManifest();
            } catch (RuntimeException e) {
                install(prev);
                throw e;
            }
            return;
        }
        manifest.add(edit.encode());
        manifest.sync();
        logNumber = edit.logNumber;
        install(next);
    }

    private void install(Version v) {
        v.refs++;
        live.add(v);
        Version old = current;
        current = v;
        unref(old);
    }

    void ref(Version v) {
        v.refs++;
    }

    void unref(Version v) {
        if (--v.refs == 0) live.removeIf(x -> x == v);
    }

    /** Table numbers referenced by any version still in use by a reader or compaction. */
    Set<Long> liveFiles() {
        Set<Long> out = new HashSet<>();
        for (Version v : live) for (List<FileMeta> level : v.levels) for (FileMeta f : level) out.add(f.number());
        return out;
    }

    double score(int level) {
        if (level == 0) return (double) current.files(0) / options.l0CompactionTrigger();
        return (double) current.bytes(level) / options.maxBytesForLevel(level);
    }

    boolean needsCompaction() {
        for (int level = 0; level < Options.NUM_LEVELS - 1; level++) if (score(level) >= 1) return true;
        return false;
    }

    Compaction pickCompaction() {
        int best = -1;
        double bestScore = 1;
        for (int level = 0; level < Options.NUM_LEVELS - 1; level++) {
            double s = score(level);
            if (s >= bestScore) {
                bestScore = s;
                best = level;
            }
        }
        if (best < 0) return null;
        Version v = current;
        List<FileMeta> inputs;
        if (best == 0) {
            // All of L0: overlapping L0 tables must move together or an older version could end
            // up shadowing a newer one.
            inputs = new ArrayList<>(v.levels.get(0));
        } else {
            List<FileMeta> files = v.levels.get(best);
            FileMeta pick = files.get(0);
            if (compactPointer[best] != null) {
                for (FileMeta f : files) {
                    if (InternalKey.COMPARATOR.compare(f.largest(), compactPointer[best]) > 0) {
                        pick = f;
                        break;
                    }
                }
            }
            inputs = List.of(pick);
            compactPointer[best] = pick.largest();
        }
        byte[] lo = null, hi = null;
        for (FileMeta f : inputs) {
            byte[] s = InternalKey.userKey(f.smallest()), l = InternalKey.userKey(f.largest());
            if (lo == null || Bytes.compare(s, lo) < 0) lo = s;
            if (hi == null || Bytes.compare(l, hi) > 0) hi = l;
        }
        ref(v);
        return new Compaction(best, inputs, v.overlapping(best + 1, lo, hi), v);
    }

    void close() {
        if (manifest != null) manifest.close();
    }
}
