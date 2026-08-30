package io.quorum.storage;

import io.quorum.common.Bytes;
import io.quorum.common.FileHandle;
import io.quorum.common.FileSystem;
import io.quorum.common.LogReader;
import io.quorum.common.LogWriter;
import io.quorum.common.Scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LevelDB-style log-structured merge tree.
 *
 * <p>Write path: writers queue up; the writer at the head becomes the commit leader, merges the
 * queued batches into one WAL record (group commit), fsyncs once if any writer asked for it,
 * inserts into the memtable and wakes the group. Full memtables become immutable and are flushed
 * to L0 tables in the background; leveled compaction keeps read amplification bounded.
 *
 * <p>Concurrency: one mutex guards metadata. Long I/O (WAL append, flush, compaction) runs with
 * the mutex released. Background work goes through the {@link Scheduler}; when a writer would
 * stall and no background work is in flight it does the work itself ("caller runs"), so the
 * engine never blocks forever under the single-threaded simulator.
 */
public final class LsmEngine implements KvEngine {
    private static final long MAX_GROUP_BYTES = 1 << 20;

    private final FileSystem fs;
    private final String dir;
    private final Options options;
    private final Scheduler scheduler;
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition bgCond = mutex.newCondition();
    private final Condition writerCond = mutex.newCondition();
    private final VersionSet versions;
    private final BlockCache blockCache;
    private final TableCache tables;

    private MemTable mem = new MemTable();
    private MemTable imm;
    private LogWriter log;
    private long memLogNumber;
    private final ArrayDeque<Writer> writers = new ArrayDeque<>();
    private final TreeMap<Long, Integer> snapshots = new TreeMap<>();
    private final Set<Long> pendingOutputs = new HashSet<>();
    private boolean bgScheduled;
    private boolean bgRunning;
    private boolean closed;
    private RuntimeException bgError;

    private long userBytes, walBytes, flushBytes, compactionReadBytes, compactionWriteBytes, compactions, flushes;

    private static final class Writer {
        final WriteBatch batch;
        final boolean sync;
        final Runnable leaderAction;
        boolean done;
        RuntimeException error;

        Writer(WriteBatch batch, boolean sync, Runnable leaderAction) {
            this.batch = batch;
            this.sync = sync;
            this.leaderAction = leaderAction;
        }
    }

    private LsmEngine(FileSystem fs, String dir, Options options, Scheduler scheduler) {
        this.fs = fs;
        this.dir = dir;
        this.options = options;
        this.scheduler = scheduler;
        this.versions = new VersionSet(fs, dir, options);
        this.blockCache = new BlockCache(options.blockCacheBytes());
        this.tables = new TableCache(fs, dir, blockCache);
    }

    public static LsmEngine open(FileSystem fs, String dir, Options options, Scheduler scheduler) {
        fs.mkdirs(dir);
        LsmEngine e = new LsmEngine(fs, dir, options, scheduler);
        e.mutex.lock();
        try {
            e.recover();
        } finally {
            e.mutex.unlock();
        }
        return e;
    }

    // ---------------------------------------------------------------- recovery

    private void recover() {
        versions.recover();
        List<Long> logs = new ArrayList<>();
        for (String name : fs.list(dir)) {
            FileNames.Parsed p = FileNames.parse(name);
            if (p.kind() == FileNames.Kind.LOG && p.number() >= versions.logNumber) logs.add(p.number());
            if (p.number() >= versions.nextFileNumber) versions.nextFileNumber = p.number() + 1;
        }
        logs.sort(null);
        MemTable recovered = new MemTable();
        long maxSeq = versions.lastSequence;
        for (long number : logs) {
            LogReader.Result r;
            try (FileHandle h = fs.open(FileSystem.join(dir, FileNames.log(number)), false)) {
                r = LogReader.readAll(h);
            }
            for (byte[] rec : r.records()) {
                WriteBatch.Decoded d = WriteBatch.decode(rec);
                long seq = d.baseSeq();
                for (WriteBatch.Op op : d.batch().ops()) recovered.add(seq++, op.type(), op.key(), op.value());
                maxSeq = Math.max(maxSeq, seq - 1);
            }
        }
        versions.lastSequence = maxSeq;
        VersionEdit edit = new VersionEdit();
        if (!recovered.isEmpty()) {
            long number = versions.newFileNumber();
            FileMeta f = buildTable(number, recovered.iterator());
            if (f != null) {
                edit.addFile(0, f);
                flushBytes += f.size();
            }
        }
        memLogNumber = versions.newFileNumber();
        log = new LogWriter(fs.open(FileSystem.join(dir, FileNames.log(memLogNumber)), true));
        edit.logNumber = memLogNumber;
        versions.logAndApply(edit); // also syncs the directory, making the new log durable
        deleteObsoleteFiles();
        maybeScheduleBackground();
    }

    // ---------------------------------------------------------------- writes

    @Override
    public void write(WriteBatch batch, WriteOptions options) {
        if (batch.isEmpty()) return;
        runAsWriter(new Writer(batch, options.sync(), null));
    }

    private void runAsWriter(Writer w) {
        mutex.lock();
        try {
            writers.addLast(w);
            while (!w.done && writers.peekFirst() != w) writerCond.awaitUninterruptibly();
            if (w.done) {
                if (w.error != null) throw w.error;
                return;
            }
            try {
                makeRoomForWrite(w.batch == null);
                if (w.leaderAction != null) {
                    waitForImmutableFlush();
                    w.leaderAction.run();
                    return;
                }
            } catch (RuntimeException e) {
                w.error = e;
                throw e;
            } finally {
                if (w.leaderAction != null || w.error != null) {
                    writers.pollFirst();
                    w.done = true;
                    writerCond.signalAll();
                }
            }
            commitGroup(w);
        } finally {
            mutex.unlock();
        }
    }

    /** Leader path: called with the mutex held and {@code leader} at the head of the queue. */
    private void commitGroup(Writer leader) {
        List<Writer> group = new ArrayList<>();
        long bytes = 0;
        for (Writer x : writers) {
            if (x.batch == null) break;
            if (x.sync && !leader.sync) break;
            if (!group.isEmpty() && bytes + x.batch.approximateBytes() > MAX_GROUP_BYTES) break;
            group.add(x);
            bytes += x.batch.approximateBytes();
        }
        WriteBatch combined = group.size() == 1 ? leader.batch : new WriteBatch();
        if (group.size() > 1) for (Writer x : group) combined.append(x.batch);
        long seq = versions.lastSequence + 1;
        MemTable m = mem;
        LogWriter lw = log;

        RuntimeException err = null;
        mutex.unlock();
        int walWritten = 0;
        try {
            walWritten = lw.add(combined.encode(seq));
            if (leader.sync) lw.sync();
            long s = seq;
            for (WriteBatch.Op op : combined.ops()) m.add(s++, op.type(), op.key(), op.value());
        } catch (RuntimeException e) {
            err = e;
        } finally {
            mutex.lock();
        }
        if (err == null) {
            versions.lastSequence = seq + combined.count() - 1;
            userBytes += combined.approximateBytes();
            walBytes += walWritten;
        } else if (bgError == null) {
            bgError = err; // the WAL is in an unknown state: refuse further writes
        }
        for (Writer x : group) {
            writers.pollFirst();
            x.done = true;
            x.error = err;
        }
        writerCond.signalAll();
        if (err != null) throw err;
    }

    private void makeRoomForWrite(boolean force) {
        while (true) {
            if (bgError != null) throw bgError;
            if (closed) throw new IllegalStateException("engine closed");
            if (!force && mem.approximateBytes() < options.writeBufferSize()) return;
            if (force && mem.isEmpty()) return;
            if (imm != null) {
                waitOrWorkInline();
            } else if (versions.current().files(0) >= options.l0StopWritesTrigger()) {
                waitOrWorkInline();
            } else {
                long newLog = versions.newFileNumber();
                FileHandle h = fs.open(FileSystem.join(dir, FileNames.log(newLog)), true);
                fs.syncDir(dir);
                log.close();
                log = new LogWriter(h);
                imm = mem;
                mem = new MemTable();
                memLogNumber = newLog;
                force = false;
                maybeScheduleBackground();
            }
        }
    }

    private void waitForImmutableFlush() {
        while (imm != null) {
            if (bgError != null) throw bgError;
            waitOrWorkInline();
        }
    }

    private void waitOrWorkInline() {
        if (bgRunning) bgCond.awaitUninterruptibly();
        else doOneUnitOfWork();
        if (bgError != null) throw bgError;
    }

    // ---------------------------------------------------------------- background work

    private void maybeScheduleBackground() {
        if (bgScheduled || bgRunning || closed || bgError != null) return;
        if (imm == null && !versions.needsCompaction()) return;
        bgScheduled = true;
        scheduler.execute(this::backgroundCall);
    }

    private void backgroundCall() {
        mutex.lock();
        try {
            bgScheduled = false;
            if (closed || bgError != null || bgRunning) return;
            doOneUnitOfWork();
        } finally {
            mutex.unlock();
        }
    }

    /** Mutex held. Flushes the immutable memtable, else runs one compaction. */
    private void doOneUnitOfWork() {
        bgRunning = true;
        try {
            if (imm != null) {
                flushImmutable();
            } else {
                Compaction c = versions.pickCompaction();
                if (c != null) runCompaction(c);
            }
        } catch (RuntimeException e) {
            if (bgError == null) bgError = e;
        } finally {
            bgRunning = false;
            bgCond.signalAll();
            maybeScheduleBackground();
        }
    }

    private void flushImmutable() {
        MemTable m = imm;
        long number = versions.newFileNumber();
        pendingOutputs.add(number);
        FileMeta f;
        mutex.unlock();
        try {
            f = buildTable(number, m.iterator());
        } finally {
            mutex.lock();
        }
        VersionEdit e = new VersionEdit();
        if (f != null) e.addFile(0, f);
        e.logNumber = memLogNumber;
        versions.logAndApply(e);
        pendingOutputs.remove(number);
        imm = null;
        flushes++;
        if (f != null) flushBytes += f.size();
        deleteObsoleteFiles();
    }

    /** Writes all entries of {@code it} into table {@code number}; null if there were none. */
    private FileMeta buildTable(long number, InternalIterator it) {
        String path = FileSystem.join(dir, FileNames.table(number));
        it.seekToFirst();
        if (!it.valid()) return null;
        try (FileHandle h = fs.open(path, true)) {
            SSTableWriter w = new SSTableWriter(h, options);
            for (; it.valid(); it.next()) w.add(it.key(), it.value());
            long size = w.finish();
            return new FileMeta(number, size, w.smallest(), w.largest(), w.entries());
        }
    }

    private void runCompaction(Compaction c) {
        try {
            if (c.isTrivialMove()) {
                FileMeta f = c.inputs().getFirst();
                VersionEdit e = new VersionEdit().deleteFile(c.level(), f.number()).addFile(c.level() + 1, f);
                versions.logAndApply(e);
                compactions++;
                return;
            }
            long smallestSnapshot = snapshots.isEmpty() ? versions.lastSequence : snapshots.firstKey();
            List<FileMeta> outputs = new ArrayList<>();
            mutex.unlock();
            try {
                mergeInputs(c, smallestSnapshot, outputs);
            } finally {
                mutex.lock();
            }
            VersionEdit e = new VersionEdit();
            long readBytes = 0;
            for (FileMeta f : c.inputs()) {
                e.deleteFile(c.level(), f.number());
                readBytes += f.size();
            }
            for (FileMeta f : c.overlapping()) {
                e.deleteFile(c.level() + 1, f.number());
                readBytes += f.size();
            }
            long written = 0;
            for (FileMeta f : outputs) {
                e.addFile(c.level() + 1, f);
                written += f.size();
            }
            versions.logAndApply(e);
            for (FileMeta f : outputs) pendingOutputs.remove(f.number());
            compactions++;
            compactionReadBytes += readBytes;
            compactionWriteBytes += written;
        } finally {
            versions.unref(c.version());
            deleteObsoleteFiles();
        }
    }

    /** Runs without the mutex. Applies LevelDB's drop rules for shadowed versions and tombstones. */
    private void mergeInputs(Compaction c, long smallestSnapshot, List<FileMeta> outputs) {
        List<InternalIterator> children = new ArrayList<>();
        if (c.level() == 0) for (FileMeta f : c.inputs()) children.add(tables.get(f.number()).iterator());
        else children.add(new LevelIterator(c.inputs(), tables));
        if (!c.overlapping().isEmpty()) children.add(new LevelIterator(c.overlapping(), tables));
        try (MergingIterator it = new MergingIterator(children)) {
            it.seekToFirst();
            byte[] currentUser = null;
            long lastSeqForKey = Long.MAX_VALUE;
            FileHandle out = null;
            SSTableWriter writer = null;
            long outNumber = 0;
            try {
                for (; it.valid(); it.next()) {
                    byte[] ik = it.key();
                    if (currentUser == null || !InternalKey.sameUserKey(ik, currentUser)) {
                        // Only cut output files between user keys, never inside one.
                        if (writer != null && writer.fileSize() >= options.targetFileSize()) {
                            outputs.add(finishOutput(writer, outNumber));
                            out.close();
                            writer = null;
                            out = null;
                        }
                        currentUser = InternalKey.userKey(ik);
                        lastSeqForKey = Long.MAX_VALUE;
                    }
                    long seq = InternalKey.sequence(ik);
                    boolean drop;
                    if (lastSeqForKey <= smallestSnapshot) {
                        drop = true; // a newer version visible to every snapshot hides this one
                    } else {
                        drop = InternalKey.type(ik) == InternalKey.TYPE_DELETE && seq <= smallestSnapshot
                                && !c.version().keyMayExistAtOrBelow(c.level() + 2, currentUser);
                    }
                    lastSeqForKey = seq;
                    if (drop) continue;
                    if (writer == null) {
                        mutex.lock();
                        try {
                            outNumber = versions.newFileNumber();
                            pendingOutputs.add(outNumber);
                        } finally {
                            mutex.unlock();
                        }
                        out = fs.open(FileSystem.join(dir, FileNames.table(outNumber)), true);
                        writer = new SSTableWriter(out, options);
                    }
                    writer.add(ik, it.value());
                }
                if (writer != null) outputs.add(finishOutput(writer, outNumber));
            } finally {
                if (out != null) out.close();
            }
        }
    }

    private static FileMeta finishOutput(SSTableWriter w, long number) {
        long size = w.finish();
        return new FileMeta(number, size, w.smallest(), w.largest(), w.entries());
    }

    /** Mutex held. Removes tables, logs and manifests no live version needs. */
    private void deleteObsoleteFiles() {
        if (bgError != null) return;
        Set<Long> live = versions.liveFiles();
        live.addAll(pendingOutputs);
        for (String name : fs.list(dir)) {
            FileNames.Parsed p = FileNames.parse(name);
            boolean keep = switch (p.kind()) {
                case TABLE -> live.contains(p.number());
                case LOG -> p.number() >= versions.logNumber;
                case MANIFEST -> p.number() == versions.manifestNumber();
                default -> true;
            };
            if (!keep) {
                if (p.kind() == FileNames.Kind.TABLE) tables.evict(p.number());
                fs.delete(FileSystem.join(dir, name));
            }
        }
    }

    // ---------------------------------------------------------------- reads

    @Override
    public Optional<byte[]> get(byte[] key, ReadOptions options) {
        MemTable m, i;
        Version v;
        long seq;
        mutex.lock();
        try {
            if (closed) throw new IllegalStateException("engine closed");
            seq = options.snapshot() != null ? options.snapshot().sequence() : versions.lastSequence;
            m = mem;
            i = imm;
            v = versions.current();
            versions.ref(v);
        } finally {
            mutex.unlock();
        }
        try {
            Lookup r = m.get(key, seq);
            if (r == null && i != null) r = i.get(key, seq);
            if (r == null) r = v.get(key, seq, tables);
            return r == null || r.deleted() ? Optional.empty() : Optional.of(r.value());
        } finally {
            mutex.lock();
            try {
                versions.unref(v);
            } finally {
                mutex.unlock();
            }
        }
    }

    @Override
    public KvIterator scan(byte[] from, byte[] to, ReadOptions options) {
        List<InternalIterator> children = new ArrayList<>();
        Version v;
        long seq;
        mutex.lock();
        try {
            if (closed) throw new IllegalStateException("engine closed");
            seq = options.snapshot() != null ? options.snapshot().sequence() : versions.lastSequence;
            children.add(mem.iterator());
            if (imm != null) children.add(imm.iterator());
            v = versions.current();
            versions.ref(v);
        } finally {
            mutex.unlock();
        }
        v.addIterators(children, tables);
        Version pinned = v;
        return new DbIterator(new MergingIterator(children), seq, from, to, () -> {
            mutex.lock();
            try {
                versions.unref(pinned);
            } finally {
                mutex.unlock();
            }
        });
    }

    @Override
    public Snapshot snapshot() {
        mutex.lock();
        try {
            long seq = versions.lastSequence;
            snapshots.merge(seq, 1, Integer::sum);
            return new Snapshot(seq, () -> {
                mutex.lock();
                try {
                    snapshots.computeIfPresent(seq, (k, n) -> n == 1 ? null : n - 1);
                } finally {
                    mutex.unlock();
                }
            });
        } finally {
            mutex.unlock();
        }
    }

    /** Forces the memtable to an L0 table and waits for the flush. */
    public void flush() {
        runAsWriter(new Writer(null, false, () -> {}));
    }

    /** Compacts until no level is over its size target (used by tests and benchmarks). */
    public void compactUntilIdle() {
        flush();
        mutex.lock();
        try {
            while (true) {
                if (bgError != null) throw bgError;
                if (bgRunning) {
                    bgCond.awaitUninterruptibly();
                    continue;
                }
                if (imm == null && !versions.needsCompaction()) return;
                doOneUnitOfWork();
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public List<String> checkpoint(String target) {
        List<String> written = new ArrayList<>();
        runAsWriter(new Writer(null, false, () -> {
            // Runs as commit leader after the flush: no write can slip in between.
            if (fs.exists(target) && !fs.list(target).isEmpty()) {
                throw new IllegalStateException("checkpoint target " + target + " is not empty");
            }
            Version v = versions.current();
            fs.mkdirs(target);
            VersionEdit e = new VersionEdit();
            e.logNumber = 0L;
            e.nextFileNumber = versions.nextFileNumber;
            e.lastSequence = versions.lastSequence;
            for (int level = 0; level < Options.NUM_LEVELS; level++) {
                for (FileMeta f : v.levels.get(level)) {
                    String name = FileNames.table(f.number());
                    fs.link(FileSystem.join(dir, name), FileSystem.join(target, name));
                    e.addFile(level, f);
                    written.add(name);
                }
            }
            String manifestName = FileNames.manifest(1);
            FileHandle mh = fs.open(FileSystem.join(target, manifestName), true);
            mh.truncate(0); // never append to a stale manifest
            try (LogWriter mw = new LogWriter(mh)) {
                mw.add(e.encode());
                mw.sync();
            }
            written.add(manifestName);
            FileSystem.writeAtomically(fs, FileSystem.join(target, FileNames.CURRENT), Bytes.utf8(manifestName + "\n"));
            written.add(FileNames.CURRENT);
        }));
        return written;
    }

    @Override
    public EngineStats stats() {
        mutex.lock();
        try {
            Version v = versions.current();
            int[] files = new int[Options.NUM_LEVELS];
            long[] bytes = new long[Options.NUM_LEVELS];
            for (int l = 0; l < Options.NUM_LEVELS; l++) {
                files[l] = v.files(l);
                bytes[l] = v.bytes(l);
            }
            return new EngineStats(userBytes, walBytes, flushBytes, compactionReadBytes, compactionWriteBytes,
                    compactions, flushes, files, bytes, blockCache.hits(), blockCache.misses(),
                    tables.bloomUseful.get());
        } finally {
            mutex.unlock();
        }
    }

    /** Test hook: asserts structural invariants of the current version. */
    void checkInvariants() {
        mutex.lock();
        try {
            versions.current().checkInvariants();
        } finally {
            mutex.unlock();
        }
    }

    public long lastSequence() {
        mutex.lock();
        try {
            return versions.lastSequence;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void close() {
        mutex.lock();
        try {
            if (closed) return;
            while (bgRunning || !writers.isEmpty()) {
                if (bgRunning) bgCond.awaitUninterruptibly();
                else writerCond.awaitUninterruptibly();
            }
            closed = true;
            log.close();
            versions.close();
            tables.close();
        } finally {
            mutex.unlock();
        }
    }
}
