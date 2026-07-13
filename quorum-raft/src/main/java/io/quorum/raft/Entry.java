package io.quorum.raft;

import java.util.Arrays;

/** One Raft log entry. {@code CONF_CHANGE} entries carry an encoded {@link ConfChange}. */
public record Entry(long term, long index, Type type, byte[] data) {
    public enum Type { NORMAL, CONF_CHANGE }

    public static Entry normal(long term, long index, byte[] data) {
        return new Entry(term, index, Type.NORMAL, data);
    }

    public int sizeBytes() {
        return data.length + 24;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Entry e && e.term == term && e.index == index && e.type == type && Arrays.equals(e.data, data);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(term) * 31 + Long.hashCode(index) * 17 + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Entry{t" + term + " i" + index + (type == Type.CONF_CHANGE ? " conf" : "") + " " + data.length + "B}";
    }
}
