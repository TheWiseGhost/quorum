package io.quorum.storage;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** K-way merge of sorted child iterators using a min-heap. Ties go to the earlier child. */
final class MergingIterator implements InternalIterator {
    private final List<InternalIterator> children;
    private final PriorityQueue<Integer> heap;
    private int current = -1;

    MergingIterator(List<InternalIterator> children) {
        this.children = children;
        Comparator<Integer> byKey = (a, b) -> {
            int c = InternalKey.COMPARATOR.compare(children.get(a).key(), children.get(b).key());
            return c != 0 ? c : Integer.compare(a, b);
        };
        this.heap = new PriorityQueue<>(Math.max(1, children.size()), byKey);
    }

    private void rebuild() {
        heap.clear();
        for (int i = 0; i < children.size(); i++) if (children.get(i).valid()) heap.add(i);
        current = heap.isEmpty() ? -1 : heap.peek();
    }

    @Override
    public boolean valid() {
        return current >= 0;
    }

    @Override
    public void seekToFirst() {
        children.forEach(InternalIterator::seekToFirst);
        rebuild();
    }

    @Override
    public void seek(byte[] target) {
        for (InternalIterator c : children) c.seek(target);
        rebuild();
    }

    @Override
    public void next() {
        int i = heap.poll();
        children.get(i).next();
        if (children.get(i).valid()) heap.add(i);
        current = heap.isEmpty() ? -1 : heap.peek();
    }

    @Override
    public byte[] key() {
        return children.get(current).key();
    }

    @Override
    public byte[] value() {
        return children.get(current).value();
    }

    @Override
    public void close() {
        children.forEach(InternalIterator::close);
    }
}
