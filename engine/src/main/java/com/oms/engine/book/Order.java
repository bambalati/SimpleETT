package com.oms.engine.book;

import com.oms.protocol.Side;
import com.oms.protocol.TimeInForce;

/**
 * Represents a resting limit order. Instances are obtained from {@link OrderPool}.
 * Fields are read/written directly — no getters/setters to keep hot paths clean.
 */
public final class Order {

    public long    internalOrderId;
    public int     sessionId;
    public long    clientSeqNo;
    public int     instrumentId;
    public Side    side;
    public TimeInForce tif;
    public long    price;        // scaled (price * PRICE_SCALE)
    public long    qty;          // remaining quantity
    public long    origQty;
    public long    recvTsNanos;

    // Intrusive doubly-linked list within a PriceLevel
    public Order prev;
    public Order next;

    // Back-pointer to the level this order belongs to (for fast cancel)
    public PriceLevel level;

    public void reset() {
        internalOrderId = 0;
        sessionId = 0;
        clientSeqNo = 0;
        instrumentId = 0;
        side = null;
        tif = null;
        price = 0;
        qty = 0;
        origQty = 0;
        recvTsNanos = 0;
        prev = null;
        next = null;
        level = null;
    }
}
