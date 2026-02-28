package com.oms.engine.book;

public final class PriceLevelPool {
    private final PriceLevel[] pool;
    private int top;

    public PriceLevelPool(int capacity) {
        pool = new PriceLevel[capacity];
        for (int i = 0; i < capacity; i++) pool[i] = new PriceLevel();
        top = capacity;
    }

    public PriceLevel borrow(long price) {
        if (top == 0) throw new IllegalStateException("PriceLevelPool exhausted");
        PriceLevel pl = pool[--top];
        pl.reset();
        pl.price = price;
        return pl;
    }

    public void release(PriceLevel pl) {
        pl.reset();
        pool[top++] = pl;
    }
}
