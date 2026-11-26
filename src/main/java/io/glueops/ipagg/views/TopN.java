package io.glueops.ipagg.views;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Keeps the n greatest elements under {@code order} in a min-heap; O(log n) per offer. */
public final class TopN<T> {

    private final int n;
    private final Comparator<T> order;
    private final PriorityQueue<T> heap;

    public TopN(int n, Comparator<T> order) {
        this.n = n;
        this.order = order;
        this.heap = new PriorityQueue<>(Math.max(1, n), order);
    }

    public void offer(T item) {
        if (n <= 0) {
            return;
        }
        if (heap.size() < n) {
            heap.add(item);
        } else if (order.compare(item, heap.peek()) > 0) {
            heap.poll();
            heap.add(item);
        }
    }

    /** Greatest first. */
    public List<T> descending() {
        List<T> list = new ArrayList<>(heap);
        list.sort(order.reversed());
        return list;
    }
}
