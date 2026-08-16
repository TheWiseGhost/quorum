package io.quorum.sim;

import io.quorum.common.FileHandle;
import io.quorum.common.FileSystem;

import java.util.List;
import java.util.function.BooleanSupplier;

/** Refuses all I/O once a node incarnation is dead, so stray callbacks can't touch the next incarnation's disk. */
final class FencedFileSystem implements FileSystem {
    static final class DeadNodeException extends RuntimeException {
        DeadNodeException() {
            super("I/O from a dead node incarnation");
        }
    }

    private final FileSystem fs;
    private final BooleanSupplier alive;

    FencedFileSystem(FileSystem fs, BooleanSupplier alive) {
        this.fs = fs;
        this.alive = alive;
    }

    private void check() {
        if (!alive.getAsBoolean()) throw new DeadNodeException();
    }

    @Override
    public FileHandle open(String path, boolean create) {
        check();
        return fs.open(path, create);
    }

    @Override
    public boolean exists(String path) {
        check();
        return fs.exists(path);
    }

    @Override
    public void rename(String from, String to) {
        check();
        fs.rename(from, to);
    }

    @Override
    public void delete(String path) {
        check();
        fs.delete(path);
    }

    @Override
    public void link(String from, String to) {
        check();
        fs.link(from, to);
    }

    @Override
    public List<String> list(String dir) {
        check();
        return fs.list(dir);
    }

    @Override
    public boolean isDirectory(String path) {
        check();
        return fs.isDirectory(path);
    }

    @Override
    public void mkdirs(String dir) {
        check();
        fs.mkdirs(dir);
    }

    @Override
    public void syncDir(String dir) {
        check();
        fs.syncDir(dir);
    }
}
