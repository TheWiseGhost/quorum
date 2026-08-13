package io.quorum.check;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Linearizability checking with the Wing &amp; Gong search and Lowe's memoization (as used by
 * Knossos and Porcupine). The history is first partitioned by key: operations on different
 * registers commute, so each sub-history can be checked independently (P-compositionality),
 * turning one exponential search into many small ones.
 */
public final class LinearizabilityChecker {
    public enum Verdict { OK, ILLEGAL, UNKNOWN }

    /** @param bestPartial ids of the longest partial linearization found (for visualizing failures) */
    public record KeyResult(String key, Verdict verdict, List<Op> ops, Set<Integer> bestPartial, long statesExplored) {}

    public record Result(Verdict verdict, List<KeyResult> perKey) {
        public KeyResult firstFailure() {
            return perKey.stream().filter(k -> k.verdict() == Verdict.ILLEGAL).findFirst().orElse(null);
        }
    }

    private final long maxStatesPerKey;

    public LinearizabilityChecker(long maxStatesPerKey) {
        this.maxStatesPerKey = maxStatesPerKey;
    }

    public LinearizabilityChecker() {
        this(2_000_000);
    }

    public Result check(List<Op> history) {
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        for (Op op : history) byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        List<KeyResult> results = new ArrayList<>();
        Verdict overall = Verdict.OK;
        for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
            KeyResult r = checkKey(e.getKey(), e.getValue());
            results.add(r);
            if (r.verdict() == Verdict.ILLEGAL) overall = Verdict.ILLEGAL;
            else if (r.verdict() == Verdict.UNKNOWN && overall == Verdict.OK) overall = Verdict.UNKNOWN;
        }
        return new Result(overall, results);
    }

    private static final class Node {
        final Op op;      // null for the head sentinel
        final boolean call;
        Node match;       // call ↔ return
        Node prev, next;

        Node(Op op, boolean call) {
            this.op = op;
            this.call = call;
        }
    }

    private record CacheKey(BitSet linearized, Long state) {}

    private record Frame(Node call, Long state) {}

    private KeyResult checkKey(String key, List<Op> rawOps) {
        // Renumber ids densely for the bitset.
        List<Op> ops = new ArrayList<>(rawOps.size());
        for (int i = 0; i < rawOps.size(); i++) ops.add(rawOps.get(i).withId(i));

        record Event(long time, boolean call, Op op) {}
        List<Event> events = new ArrayList<>();
        for (Op op : ops) {
            events.add(new Event(op.invoke(), true, op));
            events.add(new Event(op.completeOrInfinity(), false, op));
        }
        // At equal times, calls sort before returns: treating touching ops as concurrent is conservative.
        events.sort(Comparator.comparingLong(Event::time).thenComparing(e -> e.call() ? 0 : 1));

        Node head = new Node(null, false);
        Node tail = head;
        Map<Integer, Node> calls = new LinkedHashMap<>();
        for (Event ev : events) {
            Node n = new Node(ev.op(), ev.call());
            if (ev.call()) calls.put(ev.op().id(), n);
            else {
                Node c = calls.get(ev.op().id());
                c.match = n;
                n.match = c;
            }
            tail.next = n;
            n.prev = tail;
            tail = n;
        }

        BitSet linearized = new BitSet(ops.size());
        Set<CacheKey> cache = new HashSet<>();
        ArrayDeque<Frame> stack = new ArrayDeque<>();
        Long state = null;
        Node entry = head.next;
        BitSet best = new BitSet();
        long explored = 0;

        while (head.next != null) {
            if (++explored > maxStatesPerKey) return new KeyResult(key, Verdict.UNKNOWN, ops, toSet(best), explored);
            if (entry.call) {
                Object[] next = Op.step(state, entry.op);
                if (next != null) {
                    Long newState = (Long) next[0];
                    BitSet candidate = (BitSet) linearized.clone();
                    candidate.set(entry.op.id());
                    if (cache.add(new CacheKey(candidate, newState))) {
                        stack.push(new Frame(entry, state));
                        state = newState;
                        linearized = candidate;
                        if (linearized.cardinality() > best.cardinality()) best = (BitSet) linearized.clone();
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // Reached a return whose call is not yet linearized: backtrack.
                if (stack.isEmpty()) return new KeyResult(key, Verdict.ILLEGAL, ops, toSet(best), explored);
                Frame f = stack.pop();
                state = f.state();
                linearized = (BitSet) linearized.clone();
                linearized.clear(f.call().op.id());
                unlift(f.call());
                entry = f.call().next;
            }
        }
        return new KeyResult(key, Verdict.OK, ops, toSet(best), explored);
    }

    private static Set<Integer> toSet(BitSet b) {
        Set<Integer> s = new HashSet<>();
        b.stream().forEach(s::add);
        return s;
    }

    private static void lift(Node call) {
        call.prev.next = call.next;
        if (call.next != null) call.next.prev = call.prev;
        Node ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) ret.next.prev = ret.prev;
    }

    private static void unlift(Node call) {
        Node ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) ret.next.prev = ret;
        call.prev.next = call;
        if (call.next != null) call.next.prev = call;
    }

    static boolean same(Long a, Long b) {
        return Objects.equals(a, b);
    }
}
