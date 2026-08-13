package io.quorum.check;

import java.util.Objects;

/**
 * One client operation on a single key of a register-per-key store. Values are longs; null
 * means absent. An op that timed out has {@code status == INFO}: it may or may not have taken
 * effect, so it is checked with an unknown outcome and an infinite completion time.
 *
 * @param arg      value written by PUT, or the new value of a CAS (null deletes)
 * @param expected expected value of a CAS
 * @param output   value returned by GET (or the current value after a failed CAS)
 * @param casOk    whether a CAS succeeded
 */
public record Op(int id, int client, Kind kind, String key, Long arg, Long expected, Status status, Long output,
                 boolean casOk, long invoke, long complete) {
    public enum Kind { GET, PUT, DELETE, CAS }

    public enum Status { OK, INFO }

    public long completeOrInfinity() {
        return status == Status.INFO ? Long.MAX_VALUE : complete;
    }

    public Op withId(int newId) {
        return new Op(newId, client, kind, key, arg, expected, status, output, casOk, invoke, complete);
    }

    public String describe() {
        String s = switch (kind) {
            case GET -> "get → " + v(output);
            case PUT -> "put " + v(arg);
            case DELETE -> "delete";
            case CAS -> "cas " + v(expected) + "→" + v(arg) + (status == Status.INFO ? " ?" : casOk ? " ✓" : " ✗");
        };
        return status == Status.INFO ? s + " (timed out)" : s;
    }

    private static String v(Long x) {
        return x == null ? "∅" : x.toString();
    }

    /** Sequential specification of a register. Returns null if {@code op} cannot happen in {@code state}. */
    static Object[] step(Long state, Op op) {
        return switch (op.kind) {
            case GET -> Objects.equals(state, op.output) ? new Object[]{state} : null;
            case PUT -> new Object[]{op.arg};
            case DELETE -> new Object[]{null};
            case CAS -> {
                boolean matches = Objects.equals(state, op.expected);
                if (op.status == Status.INFO) yield new Object[]{matches ? op.arg : state};
                if (op.casOk) yield matches ? new Object[]{op.arg} : null;
                yield matches ? null : new Object[]{state};
            }
        };
    }
}
