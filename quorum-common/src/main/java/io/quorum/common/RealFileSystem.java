package io.quorum.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** {@link FileSystem} backed by the host OS via {@link FileChannel}. */
public final class RealFileSystem implements FileSystem {
    private final Path root;

    public RealFileSystem(Path root) {
        this.root = root.toAbsolutePath();
        try {
            Files.createDirectories(this.root);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path root() {
        return root;
    }

    private Path resolve(String path) {
        return path.isEmpty() ? root : root.resolve(path);
    }

    @Override
    public FileHandle open(String path, boolean create) {
        try {
            FileChannel ch = create
                    ? FileChannel.open(resolve(path), StandardOpenOption.READ, StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE)
                    : FileChannel.open(resolve(path), StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new Handle(ch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean exists(String path) {
        return Files.exists(resolve(path));
    }

    @Override
    public boolean isDirectory(String path) {
        return Files.isDirectory(resolve(path));
    }

    @Override
    public void rename(String from, String to) {
        try {
            Files.move(resolve(from), resolve(to), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            Files.deleteIfExists(resolve(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void link(String from, String to) {
        try {
            Files.createLink(resolve(to), resolve(from));
        } catch (UnsupportedOperationException | IOException e) {
            try {
                Files.copy(resolve(from), resolve(to), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e2) {
                throw new UncheckedIOException(e2);
            }
        }
    }

    @Override
    public List<String> list(String dir) {
        Path d = resolve(dir);
        if (!Files.isDirectory(d)) return List.of();
        try (Stream<Path> s = Files.list(d)) {
            List<String> out = new ArrayList<>();
            s.forEach(p -> out.add(p.getFileName().toString()));
            Collections.sort(out);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void mkdirs(String dir) {
        try {
            Files.createDirectories(resolve(dir));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void syncDir(String dir) {
        // Directory fsync makes renames/creates durable on Linux. Some platforms refuse to open
        // directories as channels; there the rename is as durable as the OS makes it.
        try (FileChannel ch = FileChannel.open(resolve(dir), StandardOpenOption.READ)) {
            ch.force(true);
        } catch (AccessDeniedException e) {
            // Windows
        } catch (IOException e) {
            // macOS without F_FULLFSYNC / unsupported: best effort.
        }
    }

    private static final class Handle implements FileHandle {
        private final FileChannel ch;

        Handle(FileChannel ch) {
            this.ch = ch;
        }

        @Override
        public void write(long position, ByteBuffer src) {
            try {
                long pos = position;
                while (src.hasRemaining()) pos += ch.write(src, pos);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public int read(long position, ByteBuffer dst) {
            try {
                return ch.read(dst, position);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public long size() {
            try {
                return ch.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void truncate(long size) {
            try {
                ch.truncate(size);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void fsync() {
            try {
                ch.force(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            try {
                ch.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
