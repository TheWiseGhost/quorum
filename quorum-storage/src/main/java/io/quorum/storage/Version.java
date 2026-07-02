package io.quorum.storage;

import io.quorum.common.CorruptionException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable set of tables per level. L0 tables may overlap and are ordered newest first; every
 * deeper level is sorted by key with no overlap, and each user key lives in at most one table
 * per level (compaction never splits a user key's versions across tables).
 */
final class Version {
    final List<List<FileMeta>> levels;
    int refs;

    private Version(List<List<FileMeta>> levels) {
        this.levels = levels;
    }

    static Version empty() {
        List<List<FileMeta>> levels = new ArrayList<>();
        for (int i = 0; i < Options.NUM_LEVELS; i++) levels.add(List.of());
        return new Version(levels);
    }

    Version apply(VersionEdit edit) {
        List<List<FileMeta>> next = new ArrayList<>();
        for (int level = 0; level < Options.NUM_LEVELS; level++) {
            Set<Long> removed = new HashSet<>();
            for (VersionEdit.Deleted d : edit.deleted) if (d.level() == level) removed.add(d.number());
            List<FileMeta> files = new ArrayList<>();
            for (FileMeta f : levels.get(level)) if (!removed.contains(f.number())) files.add(f);
            for (VersionEdit.Added a : edit.added) if (a.level() == level) files.add(a.file());
            if (level == 0) files.sort(Comparator.comparingLong(FileMeta::number).reversed());
            else files.sort((x, y) -> InternalKey.COMPARATOR.compare(x.smallest(), y.smallest()));
            next.add(List.copyOf(files));
        }
        return new Version(next);
    }

    void checkInvariants() {
        for (int level = 1; level < Options.NUM_LEVELS; level++) {
            List<FileMeta> files = levels.get(level);
            for (int i = 1; i < files.size(); i++) {
                if (InternalKey.compareUserKeys(files.get(i - 1).largest(), files.get(i).smallest()) >= 0) {
                    throw new CorruptionException("overlapping tables in L" + level + ": "
                            + files.get(i - 1).number() + " and " + files.get(i).number());
                }
            }
        }
    }

    Lookup get(byte[] userKey, long seq, TableCache tables) {
        for (FileMeta f : levels.get(0)) {
            if (!f.containsUserKey(userKey)) continue;
            Lookup r = tables.get(f.number()).get(userKey, seq);
            if (r != null) return r;
        }
        for (int level = 1; level < Options.NUM_LEVELS; level++) {
            FileMeta f = findFile(levels.get(level), userKey);
            if (f == null) continue;
            Lookup r = tables.get(f.number()).get(userKey, seq);
            if (r != null) return r;
        }
        return null;
    }

    private static FileMeta findFile(List<FileMeta> files, byte[] userKey) {
        int lo = 0, hi = files.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (InternalKey.compareUserKey(files.get(mid).largest(), userKey) < 0) lo = mid + 1;
            else hi = mid;
        }
        if (lo < files.size() && InternalKey.compareUserKey(files.get(lo).smallest(), userKey) <= 0) return files.get(lo);
        return null;
    }

    List<FileMeta> overlapping(int level, byte[] loUser, byte[] hiUser) {
        List<FileMeta> out = new ArrayList<>();
        for (FileMeta f : levels.get(level)) if (f.overlaps(loUser, hiUser)) out.add(f);
        return out;
    }

    /** True if some table at {@code level} or deeper may contain {@code userKey}. */
    boolean keyMayExistAtOrBelow(int level, byte[] userKey) {
        for (int l = level; l < Options.NUM_LEVELS; l++) {
            for (FileMeta f : levels.get(l)) if (f.containsUserKey(userKey)) return true;
        }
        return false;
    }

    int files(int level) {
        return levels.get(level).size();
    }

    long bytes(int level) {
        long total = 0;
        for (FileMeta f : levels.get(level)) total += f.size();
        return total;
    }

    void addIterators(List<InternalIterator> out, TableCache tables) {
        for (FileMeta f : levels.get(0)) out.add(tables.get(f.number()).iterator());
        for (int level = 1; level < Options.NUM_LEVELS; level++) {
            if (!levels.get(level).isEmpty()) out.add(new LevelIterator(levels.get(level), tables));
        }
    }
}
