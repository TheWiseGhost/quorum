package io.quorum.common;

import java.util.List;

/**
 * A directory tree addressed by '/'-separated relative paths. Namespace changes (create,
 * rename, delete) are only durable after {@link #syncDir(String)} on the parent directory,
 * mirroring POSIX semantics. The simulator's disk enforces this on crash.
 */
public interface FileSystem {
    /** Opens for read/write, creating the file if {@code create}. */
    FileHandle open(String path, boolean create);

    boolean exists(String path);

    /** Atomically replaces {@code to} with {@code from}. */
    void rename(String from, String to);

    void delete(String path);

    /** Hard-links (or, in the simulator, durably copies) an immutable file. */
    void link(String from, String to);

    /** Child names (not paths) of {@code dir}, sorted. Empty if it does not exist. */
    List<String> list(String dir);

    boolean isDirectory(String path);

    void mkdirs(String dir);

    void syncDir(String dir);

    static String parent(String path) {
        int i = path.lastIndexOf('/');
        return i < 0 ? "" : path.substring(0, i);
    }

    static String join(String dir, String name) {
        return dir.isEmpty() ? name : dir + "/" + name;
    }

    /** Writes a small file atomically and durably: write temp, fsync, rename, fsync dir. */
    static void writeAtomically(FileSystem fs, String path, byte[] content) {
        String tmp = path + ".tmp";
        if (fs.exists(tmp)) fs.delete(tmp);
        try (FileHandle h = fs.open(tmp, true)) {
            h.truncate(0);
            h.append(content);
            h.fsync();
        }
        fs.rename(tmp, path);
        fs.syncDir(parent(path));
    }

    static byte[] readFile(FileSystem fs, String path) {
        try (FileHandle h = fs.open(path, false)) {
            return h.readAll();
        }
    }

    static void deleteRecursively(FileSystem fs, String dir) {
        if (!fs.exists(dir)) return;
        if (fs.isDirectory(dir)) {
            for (String name : fs.list(dir)) deleteRecursively(fs, join(dir, name));
        }
        fs.delete(dir);
    }
}
