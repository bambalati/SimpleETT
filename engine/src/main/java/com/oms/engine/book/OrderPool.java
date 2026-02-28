package com.oms.engine.book;

/**
 * Fixed-capacity pool of {@link Order} objects.
 * Pre-allocates all orders at construction; no allocation in hot path.
 */
public final class OrderPool {

    private final Order[] pool;
    private int top;

    public OrderPool(int capacity) {
        pool = new Order[capacity];
        for (int i = 0; i < capacity; i++) {
            pool[i] = new Order();
        }
        top = capacity;
    }

    /** Borrow an order from the pool. Returns null if pool is exhausted. */
    public Order borrow() {
        if (top == 0) return null;
        Order o = pool[--top];
        o.reset();
        return o;
    }

    /** Return an order to the pool. */
    public void release(Order o) {
        o.reset();
        pool[top++] = o;
    }

    public int available() { return top; }
}
