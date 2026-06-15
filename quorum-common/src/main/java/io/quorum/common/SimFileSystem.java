package io.quorum.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * In-memory {@link FileSystem} that models what a real disk may lose on power failure.
 *
 * <ul>
 *   <li>File contents: every write/truncate since the last {@code fsync} is pending. On
 *       {@link #crash(Rng)} a random prefix of pending operations survives, and the last
 *       surviving write may be torn at a random byte.
 *   <li>Namespace: create, rename, link and delete are pending until {@code syncDir} of the
 *       affected directory. On crash a random prefix of pending namespace ops survives.
 * </ul>
 *
 * Single-threaded by design (the simulator), but synchronized so unit tests may share it.
 */
public final class SimFileSystem implements FileSystem {
    private static final class Inode {
        final long id;
        byte[] durable = Bytes.EMPTY;
        byte[] current = new byte[0];
        int currentLen;
        final List<Op> pending = new ArrayList<>();

        Inode(long id) {
            this.id = id;
        }

        byte[] snapshotCurrent() {
            return Arrays.copyOf(current, currentLen);
        }
    }

    /** Pending content op: either a write or a truncate. */
    private record Op(long position, byte[] data, long truncateTo) {}

    private enum NsKind { LINK, UNLINK }

    private record NsOp(NsKind kind, String path, Inode inode) {}

    private final Map<String, Inode> current = new TreeMap<>();
    private final Map<String, Inode> durable = new TreeMap<>();
    private final List<NsOp> pendingNs = new ArrayList<>();
    private final TreeSet<String> dirs = new TreeSet<>(List.of(""));
    private long nextInode = 1;
    private long epoch;
    private long bytesWritten;
    private long fsyncs;

    public synchronized long bytesWritten() {
        return bytesWritten;
    }

    public synchronized long fsyncs() {
        return fsyncs;
    }

    @Override
    public synchronized FileHandle open(String path, boolean create) {
        Inode inode = current.get(path);
        if (inode == null) {
            if (!create) throw new java.io.UncheckedIOException(new java.io.FileNotFoundException(path));
            requireDir(FileSystem.parent(path));
            inode = new Inode(nextInode++);
            current.put(path, inode);
            pendingNs.add(new NsOp(NsKind.LINK, path, inode));
        }
        return new Handle(inode, epoch);
    }

    private void requireDir(String dir) {
        if (!dirs.contains(dir)) throw new java.io.UncheckedIOException(new java.io.FileNotFoundException("no dir " + dir));
    }

    @Override
    public synchronized boolean exists(String path) {
        return current.containsKey(path) || dirs.contains(path);
    }

    @Override
    public synchronized boolean isDirectory(String path) {
        return dirs.contains(path);
    }

    @Override
    public synchronized void rename(String from, String to) {
        Inode inode = current.remove(from);
        if (inode == null) throw new java.io.UncheckedIOException(new java.io.FileNotFoundException(from));
        current.put(to, inode);
        pendingNs.add(new NsOp(NsKind.UNLINK, from, inode));
        pendingNs.add(new NsOp(NsKind.LINK, to, inode));
    }

    @Override
    public synchronized void delete(String path) {
        if (dirs.contains(path) && !path.isEmpty()) {
            if (!list(path).isEmpty()) throw new IllegalStateException("directory not empty: " + path);
            dirs.remove(path);
            return;
        }
        Inode inode = current.remove(path);
        if (inode != null) pendingNs.add(new NsOp(NsKind.UNLINK, path, inode));
    }

    @Override
    public synchronized void link(String from, String to) {
        Inode inode = current.get(from);
        if (inode == null) throw new java.io.UncheckedIOException(new java.io.FileNotFoundException(from));
        current.put(to, inode);
        pendingNs.add(new NsOp(NsKind.LINK, to, inode));
    }

    @Override
    public synchronized List<String> list(String dir) {
        TreeSet<String> out = new TreeSet<>();
        String prefix = dir.isEmpty() ? "" : dir + "/";
        for (String p : current.keySet()) addChild(out, p, prefix);
        for (String d : dirs) if (!d.isEmpty()) addChild(out, d, prefix);
        return new ArrayList<>(out);
    }

    private static void addChild(TreeSet<String> out, String p, String prefix) {
        if (!p.startsWith(prefix) || p.length() == prefix.length()) return;
        String rest = p.substring(prefix.length());
        int slash = rest.indexOf('/');
        out.add(slash < 0 ? rest : rest.substring(0, slash));
    }

    @Override
    public synchronized void mkdirs(String dir) {
        // Directories are modelled as always-durable; only file entries are lost on crash.
        String d = dir;
        while (!d.isEmpty()) {
            dirs.add(d);
            d = FileSystem.parent(d);
        }
    }

    @Override
    public synchronized void syncDir(String dir) {
        List<NsOp> keep = new ArrayList<>();
        for (NsOp op : pendingNs) {
            if (FileSystem.parent(op.path()).equals(dir)) applyNs(op);
            else keep.add(op);
        }
        pendingNs.clear();
        pendingNs.addAll(keep);
    }

    private void applyNs(NsOp op) {
        if (op.kind() == NsKind.LINK) durable.put(op.path(), op.inode());
        else if (durable.get(op.path()) == op.inode()) durable.remove(op.path());
    }

    /**
     * Simulates power loss: drops a random suffix of unsynced data and namespace ops. All open
     * handles become invalid.
     */
    public synchronized void crash(Rng rng) {
        epoch++;
        int keepNs = pendingNs.isEmpty() ? 0 : rng.nextInt(pendingNs.size() + 1);
        for (int i = 0; i < keepNs; i++) applyNs(pendingNs.get(i));
        pendingNs.clear();

        Map<Long, Inode> seen = new HashMap<>();
        for (Inode inode : durable.values()) seen.putIfAbsent(inode.id, inode);
        for (Inode inode : seen.values()) {
            byte[] img = inode.durable;
            int n = inode.pending.size();
            int keep = n == 0 ? 0 : rng.nextInt(n + 1);
            for (int i = 0; i < keep; i++) {
                Op op = inode.pending.get(i);
                boolean last = i == keep - 1;
                if (op.data() == null) {
                    img = Arrays.copyOf(img, (int) op.truncateTo());
                } else {
                    byte[] data = op.data();
                    if (last && data.length > 1 && rng.chance(0.5)) {
                        data = Arrays.copyOf(data, rng.nextInt(data.length)); // torn write
                    }
                    int end = (int) op.position() + data.length;
                    if (end > img.length) img = Arrays.copyOf(img, end);
                    System.arraycopy(data, 0, img, (int) op.position(), data.length);
                }
            }
            inode.durable = img;
            inode.current = img.clone();
            inode.currentLen = img.length;
            inode.pending.clear();
        }
        current.clear();
        current.putAll(durable);
    }

    /** Flips one random bit in a durable file (for corruption-detection tests). */
    public synchronized void corruptRandomBit(String path, Rng rng) {
        Inode inode = current.get(path);
        if (inode == null || inode.currentLen == 0) return;
        int at = rng.nextInt(inode.currentLen);
        int bit = 1 << rng.nextInt(8);
        inode.current[at] ^= (byte) bit;
        if (at < inode.durable.length) inode.durable[at] ^= (byte) bit;
    }

    /** Total bytes of all reachable files. */
    public synchronized long totalBytes() {
        Map<Long, Inode> seen = new HashMap<>();
        for (Inode inode : current.values()) seen.putIfAbsent(inode.id, inode);
        long total = 0;
        for (Inode i : seen.values()) total += i.currentLen;
        return total;
    }

    private final class Handle implements FileHandle {
        private final Inode inode;
        private final long openedEpoch;
        private boolean closed;

        Handle(Inode inode, long epoch) {
            this.inode = inode;
            this.openedEpoch = epoch;
        }

        private void check() {
            if (closed) throw new IllegalStateException("handle closed");
            if (openedEpoch != epoch) throw new IllegalStateException("handle from before crash");
        }

        @Override
        public void write(long position, ByteBuffer src) {
            synchronized (SimFileSystem.this) {
                check();
                byte[] data = new byte[src.remaining()];
                src.get(data);
                int end = (int) position + data.length;
                if (end > inode.current.length) inode.current = Arrays.copyOf(inode.current, Math.max(end, inode.current.length * 2));
                if (position > inode.currentLen) Arrays.fill(inode.current, inode.currentLen, (int) position, (byte) 0);
                System.arraycopy(data, 0, inode.current, (int) position, data.length);
                inode.currentLen = Math.max(inode.currentLen, end);
                inode.pending.add(new Op(position, data, -1));
                bytesWritten += data.length;
            }
        }

        @Override
        public int read(long position, ByteBuffer dst) {
            synchronized (SimFileSystem.this) {
                check();
                if (position >= inode.currentLen) return -1;
                int n = (int) Math.min(dst.remaining(), inode.currentLen - position);
                dst.put(inode.current, (int) position, n);
                return n;
            }
        }

        @Override
        public long size() {
            synchronized (SimFileSystem.this) {
                check();
                return inode.currentLen;
            }
        }

        @Override
        public void truncate(long size) {
            synchronized (SimFileSystem.this) {
                check();
                if (size < inode.currentLen) {
                    inode.currentLen = (int) size;
                } else if (size > inode.currentLen) {
                    write(inode.currentLen, ByteBuffer.wrap(new byte[(int) (size - inode.currentLen)]));
                    return;
                }
                inode.pending.add(new Op(0, null, size));
            }
        }

        @Override
        public void fsync() {
            synchronized (SimFileSystem.this) {
                check();
                inode.durable = inode.snapshotCurrent();
                inode.pending.clear();
                fsyncs++;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
