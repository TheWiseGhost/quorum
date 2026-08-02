package io.quorum.kv;

import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.Bytes;
import io.quorum.common.FileHandle;
import io.quorum.common.FileSystem;
import io.quorum.common.Scheduler;
import io.quorum.raft.Entry;
import io.quorum.storage.KvIterator;
import io.quorum.storage.LsmEngine;
import io.quorum.storage.Options;
import io.quorum.storage.ReadOptions;
import io.quorum.storage.WriteBatch;
import io.quorum.storage.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The replicated key-value state machine on top of the LSM engine.
 *
 * <p>Layout: {@code d<key>} user data, {@code s<clientId>} client sessions, {@code m:applied}.
 * Each Raft entry is applied as one atomic {@link WriteBatch} that also advances the applied
 * index, so a crash leaves the state machine at some exact log position and replay is
 * idempotent. Writes skip fsync: the Raft log is the durable source of truth.
 *
 * <p>Snapshots are LSM checkpoints (hard links to immutable tables) bundled into one byte array.
 * Installing one writes a new generation directory and atomically repoints {@code CURRENT_DATA}.
 */
public final class KvStateMachine implements AutoCloseable {
    private static final byte[] APPLIED = Bytes.utf8("m:applied");
    private static final byte DATA = 'd', SESSION = 's';
    private static final String POINTER = "CURRENT_DATA";

    private final FileSystem fs;
    private final String dir;
    private final Options options;
    private final Scheduler background;
    private final boolean dedup;
    private LsmEngine engine;
    private String dataDir;
    private long applied;

    public KvStateMachine(FileSystem fs, String dir, Options options, Scheduler background, boolean dedup) {
        this.fs = fs;
        this.dir = dir;
        this.options = options;
        this.background = background;
        this.dedup = dedup;
        fs.mkdirs(dir);
        String pointer = FileSystem.join(dir, POINTER);
        if (fs.exists(pointer)) {
            dataDir = new String(FileSystem.readFile(fs, pointer), StandardCharsets.UTF_8).trim();
        } else {
            dataDir = "data-000001";
            FileSystem.writeAtomically(fs, pointer, Bytes.utf8(dataDir));
        }
        for (String name : fs.list(dir)) {
            if ((name.startsWith("data-") && !name.equals(dataDir)) || name.equals("snap-tmp")) {
                FileSystem.deleteRecursively(fs, FileSystem.join(dir, name));
            }
        }
        engine = LsmEngine.open(fs, FileSystem.join(dir, dataDir), options, background);
        applied = engine.get(APPLIED).map(b -> Bytes.getLongBE(b, 0)).orElse(0L);
    }

    public long appliedIndex() {
        return applied;
    }

    public LsmEngine engine() {
        return engine;
    }

    static byte[] dataKey(byte[] key) {
        byte[] out = new byte[key.length + 1];
        out[0] = DATA;
        System.arraycopy(key, 0, out, 1, key.length);
        return out;
    }

    private static byte[] sessionKey(long clientId) {
        byte[] out = new byte[9];
        out[0] = SESSION;
        Bytes.putLongBE(out, 1, clientId);
        return out;
    }

    /** Applies one committed entry; returns the result for NORMAL commands, else null. */
    public KvResult apply(Entry e) {
        if (e.index() <= applied) return null; // already applied before a restart
        WriteBatch batch = new WriteBatch();
        KvResult result = null;
        if (e.type() == Entry.Type.NORMAL && e.data().length > 0) {
            Command cmd = Command.decode(e.data());
            result = applyCommand(cmd, batch);
        }
        batch.put(APPLIED, Bytes.longBE(e.index()));
        engine.write(batch, WriteOptions.NO_SYNC);
        applied = e.index();
        return result;
    }

    private KvResult applyCommand(Command cmd, WriteBatch batch) {
        byte[] sk = sessionKey(cmd.clientId());
        if (dedup) {
            byte[] session = engine.get(sk).orElse(null);
            if (session != null) {
                ByteReader r = new ByteReader(session);
                long lastSeq = r.getVarLong();
                if (cmd.seq() == lastSeq) return KvResult.decode(r); // retry: answer from cache
                if (cmd.seq() < lastSeq) return KvResult.STALE;
            }
        }
        byte[] dk = dataKey(cmd.key());
        KvResult result = switch (cmd) {
            case Command.Put p -> {
                batch.put(dk, p.value());
                yield KvResult.OK;
            }
            case Command.Delete d -> {
                batch.delete(dk);
                yield KvResult.OK;
            }
            case Command.Cas c -> {
                byte[] current = engine.get(dk).orElse(null);
                if (Arrays.equals(current, c.expected())) {
                    if (c.newValue() == null) batch.delete(dk);
                    else batch.put(dk, c.newValue());
                    yield KvResult.OK;
                }
                yield new KvResult(false, current);
            }
        };
        if (dedup) {
            batch.put(sk, new ByteWriter(32).putVarLong(cmd.seq()).putRaw(result.encode()).toByteArray());
        }
        return result;
    }

    public byte[] get(byte[] key) {
        return engine.get(dataKey(key)).orElse(null);
    }

    public record Row(byte[] key, byte[] value) {}

    public List<Row> scan(byte[] from, byte[] to, int limit) {
        byte[] lo = dataKey(from == null ? Bytes.EMPTY : from);
        byte[] hi = to == null ? new byte[]{(byte) (DATA + 1)} : dataKey(to);
        List<Row> out = new ArrayList<>();
        try (KvIterator it = engine.scan(lo, hi, ReadOptions.DEFAULT)) {
            while (it.hasNext() && out.size() < limit) {
                KvIterator.Entry e = it.next();
                out.add(new Row(Arrays.copyOfRange(e.key(), 1, e.key().length), e.value()));
            }
        }
        return out;
    }

    /** A consistent image as of {@link #appliedIndex()}: the checkpoint's files bundled together. */
    public byte[] snapshot() {
        String tmp = FileSystem.join(dir, "snap-tmp");
        FileSystem.deleteRecursively(fs, tmp);
        List<String> names = engine.checkpoint(tmp);
        ByteWriter w = new ByteWriter(1 << 16);
        w.putVarLong(applied).putVarInt(names.size());
        for (String name : names) {
            w.putString(name).putBytes(FileSystem.readFile(fs, FileSystem.join(tmp, name)));
        }
        FileSystem.deleteRecursively(fs, tmp);
        return w.toByteArray();
    }

    /** Replaces the whole state with a snapshot image produced by {@link #snapshot()}. */
    public void restore(byte[] image) {
        ByteReader r = new ByteReader(image);
        long index = r.getVarLong();
        int n = r.getVarInt();
        int gen = Integer.parseInt(dataDir.substring(5)) + 1;
        String nextDir = String.format("data-%06d", gen);
        String path = FileSystem.join(dir, nextDir);
        FileSystem.deleteRecursively(fs, path);
        fs.mkdirs(path);
        for (int i = 0; i < n; i++) {
            String name = r.getString();
            byte[] content = r.getBytes();
            try (FileHandle h = fs.open(FileSystem.join(path, name), true)) {
                h.append(content);
                h.fsync();
            }
        }
        fs.syncDir(path);
        fs.syncDir(dir);
        FileSystem.writeAtomically(fs, FileSystem.join(dir, POINTER), Bytes.utf8(nextDir));
        engine.close();
        String old = dataDir;
        dataDir = nextDir;
        engine = LsmEngine.open(fs, path, options, background);
        applied = engine.get(APPLIED).map(b -> Bytes.getLongBE(b, 0)).orElse(0L);
        if (applied != index) throw new IllegalStateException("snapshot applied index " + applied + " != " + index);
        FileSystem.deleteRecursively(fs, FileSystem.join(dir, old));
    }

    @Override
    public void close() {
        engine.close();
    }
}
