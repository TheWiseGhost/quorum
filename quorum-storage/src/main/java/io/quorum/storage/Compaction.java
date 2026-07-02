package io.quorum.storage;

import java.util.List;

/** Inputs for merging {@code level} into {@code level + 1}. */
record Compaction(int level, List<FileMeta> inputs, List<FileMeta> overlapping, Version version) {
    /** A single table with nothing beneath it can simply be re-labelled one level down. */
    boolean isTrivialMove() {
        return level > 0 && inputs.size() == 1 && overlapping.isEmpty();
    }
}
