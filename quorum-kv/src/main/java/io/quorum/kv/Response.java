package io.quorum.kv;

import java.util.List;

/**
 * @param code       OK, NOT_LEADER (retry at {@code leaderHint}), RETRY (outcome unknown or
 *                   leadership changed: safe to resend the same command), or ERROR
 * @param ok         command outcome (false for a failed CAS)
 * @param value      read value or current value after a failed CAS
 * @param rows       scan results
 */
public record Response(Code code, long leaderHint, boolean ok, byte[] value, List<KvStateMachine.Row> rows,
                       String message) {
    public enum Code { OK, NOT_LEADER, RETRY, ERROR }

    public static Response ok(KvResult r) {
        return new Response(Code.OK, 0, r.ok(), r.value(), List.of(), "");
    }

    public static Response value(byte[] v) {
        return new Response(Code.OK, 0, true, v, List.of(), "");
    }

    public static Response rows(List<KvStateMachine.Row> rows) {
        return new Response(Code.OK, 0, true, null, rows, "");
    }

    public static Response notLeader(long hint) {
        return new Response(Code.NOT_LEADER, hint, false, null, List.of(), "not leader");
    }

    public static Response retry(String why) {
        return new Response(Code.RETRY, 0, false, null, List.of(), why);
    }

    public static Response error(String why) {
        return new Response(Code.ERROR, 0, false, null, List.of(), why);
    }

    public static Response message(String text) {
        return new Response(Code.OK, 0, true, null, List.of(), text);
    }
}
