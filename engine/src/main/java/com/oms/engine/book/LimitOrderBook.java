package com.oms.engine.book;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.TreeMap;

/**
 * Limit order book for a single instrument.
 *
 * Data structures:
 *   - bids: TreeMap<Long, PriceLevel> descending (best bid = last entry with highest key)
 *   - asks: TreeMap<Long, PriceLevel> ascending  (best ask = first entry)
 *   - orderMap: Long2ObjectOpenHashMap for O(1) cancel lookup
 *
 * The TreeMap allocates on level creation, but levels are pooled and reused.
 * Hot-path (add/cancel/match) is allocation-free once levels are established.
 *
 * Callback interface allows the engine partition to emit fills/acks without
 * any allocation — it writes directly into a pre-allocated Aeron buffer.
 */
public final class LimitOrderBook {

    public interface MatchCallback {
        /**
         * Called for each fill generated.
         * @param aggressorId  internal order id of the aggressor
         * @param passiveId    internal order id of the passive (resting) order
         * @param aggressorSessionId
         * @param passiveSessionId
         * @param instrumentId
         * @param side         side of the aggressor
         * @param fillPrice    price * PRICE_SCALE
         * @param fillQty
         * @param aggressorLeavesQty remaining on aggressor after this fill
         * @param passiveLeavesQty   remaining on passive after this fill
         */
        void onFill(long aggressorId, long passiveId,
                    int aggressorSessionId, int passiveSessionId,
                    int instrumentId,
                    com.oms.protocol.Side side,
                    long fillPrice, long fillQty,
                    long aggressorLeavesQty, long passiveLeavesQty);
    }

    private final int instrumentId;
    private final OrderPool orderPool;
    private final PriceLevelPool levelPool;

    // bids: ascending TreeMap; best bid = lastKey() (highest price)
    // asks: ascending TreeMap; best ask = firstKey() (lowest price)
    private final TreeMap<Long, PriceLevel> bids = new TreeMap<>(); // price -> level
    private final TreeMap<Long, PriceLevel> asks = new TreeMap<>(); // price -> level

    // internalOrderId -> Order for cancels
    private final Long2ObjectOpenHashMap<Order> orderMap = new Long2ObjectOpenHashMap<>(1024);

    public LimitOrderBook(int instrumentId, OrderPool orderPool, PriceLevelPool levelPool) {
        this.instrumentId = instrumentId;
        this.orderPool = orderPool;
        this.levelPool  = levelPool;
    }

    /**
     * Add a new order. May trigger matching.
     * Returns true if the order (or its remainder) was added to the book (GTC case).
     */
    public boolean addOrder(Order incoming, MatchCallback cb) {
        // Attempt matching against the opposing side
        if (incoming.side == com.oms.protocol.Side.BUY) {
            matchAgainstAsks(incoming, cb);
        } else {
            matchAgainstBids(incoming, cb);
        }

        if (incoming.qty <= 0) return false; // fully filled

        if (incoming.tif == com.oms.protocol.TimeInForce.IOC) {
            // Cancel remainder — caller handles cancel ack if needed
            orderPool.release(incoming);
            return false;
        }

        // GTC: rest on book
        restOrder(incoming);
        orderMap.put(incoming.internalOrderId, incoming);
        return true;
    }

    private void matchAgainstAsks(Order buy, MatchCallback cb) {
        while (buy.qty > 0 && !asks.isEmpty()) {
            long bestAskPrice = asks.firstKey();
            if (buy.price < bestAskPrice) break; // no cross

            PriceLevel level = asks.get(bestAskPrice);
            matchLevel(buy, level, bestAskPrice, cb);

            if (level.isEmpty()) {
                asks.remove(bestAskPrice);
                levelPool.release(level);
            }
        }
    }

    private void matchAgainstBids(Order sell, MatchCallback cb) {
        while (sell.qty > 0 && !bids.isEmpty()) {
            long bestBidPrice = bids.lastKey();
            if (sell.price > bestBidPrice) break; // no cross

            PriceLevel level = bids.get(bestBidPrice);
            matchLevel(sell, level, bestBidPrice, cb);

            if (level.isEmpty()) {
                bids.remove(bestBidPrice);
                levelPool.release(level);
            }
        }
    }

    private void matchLevel(Order aggressor, PriceLevel level, long price, MatchCallback cb) {
        Order passive = level.head;
        while (passive != null && aggressor.qty > 0) {
            long fillQty = Math.min(aggressor.qty, passive.qty);
            aggressor.qty -= fillQty;
            passive.qty   -= fillQty;
            level.totalQty -= fillQty;

            long aggressorLeaves = aggressor.qty;
            long passiveLeaves   = passive.qty;

            cb.onFill(aggressor.internalOrderId, passive.internalOrderId,
                    aggressor.sessionId, passive.sessionId,
                    instrumentId,
                    aggressor.side,
                    price, fillQty,
                    aggressorLeaves, passiveLeaves);

            Order next = passive.next;
            if (passive.qty == 0) {
                level.removeOrder(passive);
                orderMap.remove(passive.internalOrderId);
                orderPool.release(passive);
            }
            passive = next;
        }
    }

    private void restOrder(Order o) {
        TreeMap<Long, PriceLevel> book = o.side == com.oms.protocol.Side.BUY ? bids : asks;
        PriceLevel level = book.get(o.price);
        if (level == null) {
            level = levelPool.borrow(o.price);
            book.put(o.price, level);
        }
        level.addOrder(o);
    }

    /** Cancel by internal order id. Returns true if found and cancelled. */
    public boolean cancel(long internalOrderId) {
        Order o = orderMap.remove(internalOrderId);
        if (o == null) return false;

        PriceLevel level = o.level;
        level.removeOrder(o);

        TreeMap<Long, PriceLevel> book = o.side == com.oms.protocol.Side.BUY ? bids : asks;
        if (level.isEmpty()) {
            book.remove(level.price);
            levelPool.release(level);
        }
        orderPool.release(o);
        return true;
    }

    public long bestBid() { return bids.isEmpty() ? Long.MIN_VALUE : bids.lastKey(); }
    public long bestAsk() { return asks.isEmpty() ? Long.MAX_VALUE : asks.firstKey(); }

    public int bidLevels() { return bids.size(); }
    public int askLevels() { return asks.size(); }

    public int instrumentId() { return instrumentId; }
}
