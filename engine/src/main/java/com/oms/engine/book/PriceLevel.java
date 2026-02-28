package com.oms.engine.book;

/**
 * Doubly-linked list of orders at a single price.
 * Head = oldest (first to match). Tail = newest.
 * Uses intrusive links on {@link Order} to avoid allocations.
 */
public final class PriceLevel {

    public long price;
    public long totalQty;
    public Order head;
    public Order tail;

    // Intrusive tree links for use in the price tree
    public PriceLevel left, right, parent;
    public int height; // for AVL balancing

    public void reset() {
        price = 0;
        totalQty = 0;
        head = null;
        tail = null;
        left = null;
        right = null;
        parent = null;
        height = 1;
    }

    public void addOrder(Order o) {
        o.level = this;
        o.prev = tail;
        o.next = null;
        if (tail != null) tail.next = o;
        tail = o;
        if (head == null) head = o;
        totalQty += o.qty;
    }

    public void removeOrder(Order o) {
        if (o.prev != null) o.prev.next = o.next;
        else head = o.next;
        if (o.next != null) o.next.prev = o.prev;
        else tail = o.prev;
        totalQty -= o.qty;
        o.prev = null;
        o.next = null;
        o.level = null;
    }

    public boolean isEmpty() { return head == null; }
}
