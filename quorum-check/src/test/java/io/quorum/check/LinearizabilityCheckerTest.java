package io.quorum.check;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.quorum.check.LinearizabilityChecker.Verdict.*;
import static org.junit.jupiter.api.Assertions.*;

class LinearizabilityCheckerTest {
    private int id;

    private Op put(int client, long v, long start, long end) {
        return new Op(id++, client, Op.Kind.PUT, "k", v, null, Op.Status.OK, null, false, start, end);
    }

    private Op get(int client, Long v, long start, long end) {
        return new Op(id++, client, Op.Kind.GET, "k", null, null, Op.Status.OK, v, false, start, end);
    }

    private Op cas(int client, Long expect, Long v, boolean ok, long start, long end) {
        return new Op(id++, client, Op.Kind.CAS, "k", v, expect, Op.Status.OK, null, ok, start, end);
    }

    private LinearizabilityChecker.Verdict check(Op... ops) {
        return new LinearizabilityChecker().check(List.of(ops)).verdict();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        assertEquals(OK, check(put(1, 1, 0, 1), get(2, 1L, 2, 3), put(1, 2, 4, 5), get(2, 2L, 6, 7)));
    }

    @Test
    void staleReadAfterCompletedWriteIsIllegal() {
        // put(2) completes at t=5; a read starting at t=6 must not see 1.
        assertEquals(ILLEGAL, check(put(1, 1, 0, 1), put(1, 2, 2, 5), get(2, 1L, 6, 7)));
    }

    @Test
    void concurrentReadMaySeeEitherValue() {
        assertEquals(OK, check(put(1, 1, 0, 1), put(1, 2, 2, 10), get(2, 1L, 3, 4), get(3, 2L, 5, 6)));
        // But once a read saw the new value, a later read cannot see the old one.
        assertEquals(ILLEGAL, check(put(1, 1, 0, 1), put(1, 2, 2, 10), get(2, 2L, 3, 4), get(3, 1L, 5, 6)));
    }

    @Test
    void doubleCasSuccessIsIllegal() {
        // Two CASes from the same expected value cannot both succeed (the classic retry-without-dedup bug).
        assertEquals(ILLEGAL, check(put(1, 0, 0, 1), cas(1, 0L, 1L, true, 2, 4), cas(2, 0L, 2L, true, 3, 5)));
        assertEquals(OK, check(put(1, 0, 0, 1), cas(1, 0L, 1L, true, 2, 4), cas(2, 0L, 2L, false, 3, 5)));
    }

    @Test
    void timedOutWriteMayOrMayNotHaveHappened() {
        Op info = new Op(id++, 1, Op.Kind.PUT, "k", 9L, null, Op.Status.INFO, null, false, 2, 3);
        assertEquals(OK, check(put(1, 1, 0, 1), info, get(2, 9L, 10, 11)));
        Op info2 = new Op(id++, 1, Op.Kind.PUT, "k", 9L, null, Op.Status.INFO, null, false, 2, 3);
        assertEquals(OK, check(put(1, 1, 0, 1), info2, get(2, 1L, 10, 11)));
    }

    @Test
    void keysAreCheckedIndependently() {
        Op a = new Op(id++, 1, Op.Kind.PUT, "a", 1L, null, Op.Status.OK, null, false, 0, 1);
        Op b = new Op(id++, 2, Op.Kind.GET, "b", null, null, Op.Status.OK, 1L, false, 2, 3);
        LinearizabilityChecker.Result r = new LinearizabilityChecker().check(List.of(a, b));
        assertEquals(ILLEGAL, r.verdict());
        assertEquals("b", r.firstFailure().key());
        String html = HistoryVisualizer.render("demo", r.firstFailure());
        assertTrue(html.contains("<svg"));
    }

    @Test
    void randomSequentialRegisterHistoriesAreAlwaysLinearizable() {
        Random rnd = new Random(42);
        for (int trial = 0; trial < 50; trial++) {
            List<Op> ops = new ArrayList<>();
            Long reg = null;
            long t = 0;
            for (int i = 0; i < 300; i++) {
                int client = rnd.nextInt(6);
                // Overlapping intervals around a sequential execution point keep it linearizable.
                long s = t - rnd.nextInt(3), e = t + 1 + rnd.nextInt(3);
                switch (rnd.nextInt(3)) {
                    case 0 -> { reg = (long) i; ops.add(new Op(i, client, Op.Kind.PUT, "k", reg, null, Op.Status.OK, null, false, s, e)); }
                    case 1 -> ops.add(new Op(i, client, Op.Kind.GET, "k", null, null, Op.Status.OK, reg, false, s, e));
                    default -> {
                        Long expect = rnd.nextBoolean() ? reg : Long.valueOf(-1L);
                        boolean ok = java.util.Objects.equals(expect, reg);
                        Long nv = (long) i;
                        ops.add(new Op(i, client, Op.Kind.CAS, "k", nv, expect, Op.Status.OK, null, ok, s, e));
                        if (ok) reg = nv;
                    }
                }
                t += 10;
            }
            assertEquals(OK, new LinearizabilityChecker().check(ops).verdict(), "trial " + trial);
        }
    }
}
