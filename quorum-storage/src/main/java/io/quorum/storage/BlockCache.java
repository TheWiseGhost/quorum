package io.quorum.storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Byte-bounded CLOCK cache (second-chance LRU approximation). A hit only sets a reference bit,
 * so lookups never reorder a list; the hand clears bits and evicts unreferenced blocks.
 */
final class BlockCache {
    private record Key(long file, long offset) {}

    private static final class Node {
        final Key key;
        final Block block;
        boolean referenced;
        Node prev, next;

        Node(Key key, Block block) {
            this.key = key;
            this.block = block;
        }
    }

    private final long capacity;
    private final Map<Key, Node> map = new HashMap<>();
    private Node hand; // circular doubly-linked ring
    private long used;
    private long hits, misses;

    BlockCache(long capacityBytes) {
        this.capacity = capacityBytes;
    }

    synchronized Block get(long file, long offset) {
        Node n = map.get(new Key(file, offset));
        if (n == null) {
            misses++;
            return null;
        }
        hits++;
        n.referenced = true;
        return n.block;
    }

    synchronized void put(long file, long offset, Block block) {
        if (capacity <= 0 || block.sizeInBytes() > capacity) return;
        Key key = new Key(file, offset);
        if (map.containsKey(key)) return;
        while (used + block.sizeInBytes() > capacity && hand != null) evictOne();
        Node n = new Node(key, block);
        if (hand == null) {
            n.prev = n.next = n;
            hand = n;
        } else { // insert just behind the hand so it is examined last
            n.next = hand;
            n.prev = hand.prev;
            hand.prev.next = n;
            hand.prev = n;
        }
        map.put(key, n);
        used += block.sizeInBytes();
    }

    private void evictOne() {
        while (hand.referenced) {
            hand.referenced = false;
            hand = hand.next;
        }
        Node victim = hand;
        hand = victim.next == victim ? null : victim.next;
        unlink(victim);
    }

    private void unlink(Node n) {
        n.prev.next = n.next;
        n.next.prev = n.prev;
        if (hand == n) hand = n.next == n ? null : n.next;
        map.remove(n.key);
        used -= n.block.sizeInBytes();
    }

    synchronized void evictFile(long file) {
        map.values().removeIf(n -> {
            if (n.key.file() != file) return false;
            n.prev.next = n.next;
            n.next.prev = n.prev;
            if (hand == n) hand = n.next == n ? null : n.next;
            used -= n.block.sizeInBytes();
            return true;
        });
    }

    synchronized long hits() {
        return hits;
    }

    synchronized long misses() {
        return misses;
    }

    synchronized long usedBytes() {
        return used;
    }
}
