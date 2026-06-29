package io.quorum.storage;

import java.util.List;

/** Concatenates the non-overlapping, sorted tables of one level (L1+), opening each lazily. */
final class LevelIterator implements InternalIterator {
    private final List<FileMeta> files;
    private final TableCache tables;
    private int fileIndex;
    private InternalIterator cur;

    LevelIterator(List<FileMeta> files, TableCache tables) {
        this.files = files;
        this.tables = tables;
    }

    private void open(int i) {
        fileIndex = i;
        cur = i < files.size() ? tables.get(files.get(i).number()).iterator() : null;
    }

    private void skipEmpty() {
        while (cur != null && !cur.valid()) {
            open(fileIndex + 1);
            if (cur != null) cur.seekToFirst();
        }
    }

    @Override
    public boolean valid() {
        return cur != null && cur.valid();
    }

    @Override
    public void seekToFirst() {
        open(0);
        if (cur != null) cur.seekToFirst();
        skipEmpty();
    }

    @Override
    public void seek(byte[] target) {
        int lo = 0, hi = files.size();
        while (lo < hi) { // first file whose largest >= target
            int mid = (lo + hi) >>> 1;
            if (InternalKey.COMPARATOR.compare(files.get(mid).largest(), target) < 0) lo = mid + 1;
            else hi = mid;
        }
        open(lo);
        if (cur != null) cur.seek(target);
        skipEmpty();
    }

    @Override
    public void next() {
        cur.next();
        skipEmpty();
    }

    @Override
    public byte[] key() {
        return cur.key();
    }

    @Override
    public byte[] value() {
        return cur.value();
    }
}
