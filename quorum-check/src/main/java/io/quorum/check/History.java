package io.quorum.check;

import java.util.ArrayList;
import java.util.List;

/** Thread-safe recorder of client operations as invoke/complete pairs. */
public final class History {
    private final List<Op> completed = new ArrayList<>();
    private int nextId;

    public final class Pending {
        private final int id, client;
        private final Op.Kind kind;
        private final String key;
        private final Long arg, expected;
        private final long invoke;

        private Pending(int id, int client, Op.Kind kind, String key, Long arg, Long expected, long invoke) {
            this.id = id;
            this.client = client;
            this.kind = kind;
            this.key = key;
            this.arg = arg;
            this.expected = expected;
            this.invoke = invoke;
        }

        public void ok(long time, Long output, boolean casOk) {
            add(new Op(id, client, kind, key, arg, expected, Op.Status.OK, output, casOk, invoke, time));
        }

        /** Outcome unknown (timed out): may or may not have taken effect. */
        public void info(long time) {
            if (kind == Op.Kind.GET) return; // a read with unknown outcome constrains nothing
            add(new Op(id, client, kind, key, arg, expected, Op.Status.INFO, null, false, invoke, time));
        }

        /** Definitely did not take effect: dropped from the history. */
        public void fail() {}
    }

    public synchronized Pending invoke(int client, Op.Kind kind, String key, Long arg, Long expected, long time) {
        return new Pending(nextId++, client, kind, key, arg, expected, time);
    }

    private synchronized void add(Op op) {
        completed.add(op);
    }

    public synchronized List<Op> ops() {
        return List.copyOf(completed);
    }

    public synchronized int size() {
        return completed.size();
    }
}
