package com.oms.engine.book;

import com.oms.protocol.Side;
import com.oms.protocol.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LimitOrderBookTest {

    private static final int INSTR = 1;
    private static final long PRICE_SCALE = 1_000_000L;

    private OrderPool orderPool;
    private PriceLevelPool levelPool;
    private LimitOrderBook book;

    // Fill records captured during matching
    record FillEvent(long aggressorId, long passiveId, long price, long qty,
                     long aggressorLeaves, long passiveLeaves) {}

    private final List<FillEvent> fills = new ArrayList<>();

    private final LimitOrderBook.MatchCallback cb = (aggressorId, passiveId,
                                                      aggressorSessId, passiveSessId, instrId,
                                                      side, fillPrice, fillQty,
                                                      aggressorLeaves, passiveLeaves) ->
            fills.add(new FillEvent(aggressorId, passiveId, fillPrice, fillQty, aggressorLeaves, passiveLeaves));

    @BeforeEach
    void setUp() {
        orderPool = new OrderPool(10_000);
        levelPool = new PriceLevelPool(1_000);
        book      = new LimitOrderBook(INSTR, orderPool, levelPool);
        fills.clear();
    }

    // -----------------------------------------------------------------------
    // Test 1: Two crossing GTC orders → full fill
    // -----------------------------------------------------------------------
    @Test
    void testFullCross_buyAggressor() {
        // Rest a sell limit at 100
        Order sell = makeOrder(1, Side.SELL, TimeInForce.GTC, 100 * PRICE_SCALE, 50);
        book.addOrder(sell, cb);
        assertTrue(fills.isEmpty(), "No fills yet — passive side");

        // Aggressive buy at 100 (crosses)
        Order buy = makeOrder(2, Side.BUY, TimeInForce.GTC, 100 * PRICE_SCALE, 50);
        book.addOrder(buy, cb);

        assertEquals(1, fills.size(), "Exactly one fill event");
        FillEvent f = fills.get(0);
        assertEquals(2,              f.aggressorId());
        assertEquals(1,              f.passiveId());
        assertEquals(100 * PRICE_SCALE, f.price());
        assertEquals(50,             f.qty());
        assertEquals(0,              f.aggressorLeaves());
        assertEquals(0,              f.passiveLeaves());

        // Book should be empty
        assertEquals(Long.MIN_VALUE, book.bestBid());
        assertEquals(Long.MAX_VALUE, book.bestAsk());
    }

    // -----------------------------------------------------------------------
    // Test 2: Partial fill, remainder rests
    // -----------------------------------------------------------------------
    @Test
    void testPartialFill_restRemainder() {
        // Rest sell 30 @ 100
        Order sell = makeOrder(1, Side.SELL, TimeInForce.GTC, 100 * PRICE_SCALE, 30);
        book.addOrder(sell, cb);

        // Buy 100 @ 100 — partially matches, 70 rests
        Order buy = makeOrder(2, Side.BUY, TimeInForce.GTC, 100 * PRICE_SCALE, 100);
        book.addOrder(buy, cb);

        assertEquals(1, fills.size());
        FillEvent f = fills.get(0);
        assertEquals(30, f.qty());
        assertEquals(70, f.aggressorLeaves(), "Buy should have 70 remaining");
        assertEquals(0,  f.passiveLeaves(),   "Sell fully filled");

        // Bid side should have resting buy @ 100 qty=70
        assertEquals(100 * PRICE_SCALE, book.bestBid());
        assertEquals(Long.MAX_VALUE, book.bestAsk(), "Ask side empty");
    }

    // -----------------------------------------------------------------------
    // Test 3: FIFO — two resting sells at same price, aggressor buy fills in order
    // -----------------------------------------------------------------------
    @Test
    void testFifoWithinPriceLevel() {
        // Two sells at 100: order 1 arrives first, then order 2
        Order sell1 = makeOrder(1, Side.SELL, TimeInForce.GTC, 100 * PRICE_SCALE, 20);
        Order sell2 = makeOrder(2, Side.SELL, TimeInForce.GTC, 100 * PRICE_SCALE, 20);
        book.addOrder(sell1, cb);
        book.addOrder(sell2, cb);

        // Buy enough to fill both
        Order buy = makeOrder(3, Side.BUY, TimeInForce.GTC, 100 * PRICE_SCALE, 40);
        book.addOrder(buy, cb);

        assertEquals(2, fills.size(), "Should generate 2 fill events");
        // First fill should be against order 1 (oldest)
        assertEquals(1, fills.get(0).passiveId(), "First passive should be sell1");
        assertEquals(2, fills.get(1).passiveId(), "Second passive should be sell2");

        assertEquals(Long.MAX_VALUE, book.bestAsk(), "Ask side should be empty");
    }

    // -----------------------------------------------------------------------
    // Test 4: IOC order — unfilled remainder cancelled, not rested
    // -----------------------------------------------------------------------
    @Test
    void testIocCancelsRemainder() {
        // Resting sell 30 @ 100
        Order sell = makeOrder(1, Side.SELL, TimeInForce.GTC, 100 * PRICE_SCALE, 30);
        book.addOrder(sell, cb);

        // IOC buy 100 @ 100 — matches 30, rest cancelled
        Order iocBuy = makeOrder(2, Side.BUY, TimeInForce.IOC, 100 * PRICE_SCALE, 100);
        book.addOrder(iocBuy, cb);

        assertEquals(1, fills.size(), "One fill for the matched qty");
        assertEquals(30, fills.get(0).qty());

        // No resting bid (IOC remainder cancelled)
        assertEquals(Long.MIN_VALUE, book.bestBid(), "IOC remainder must not rest");
    }

    // -----------------------------------------------------------------------
    // Test 5: Cancel removes order from book
    // -----------------------------------------------------------------------
    @Test
    void testCancelOrder() {
        Order buy = makeOrder(1, Side.BUY, TimeInForce.GTC, 100 * PRICE_SCALE, 50);
        book.addOrder(buy, cb);
        assertEquals(100 * PRICE_SCALE, book.bestBid());

        assertTrue(book.cancel(1), "Cancel should succeed");
        assertEquals(Long.MIN_VALUE, book.bestBid(), "Book should be empty after cancel");
        assertFalse(book.cancel(1), "Second cancel should return false");
    }

    // -----------------------------------------------------------------------
    // Test 6: Price-priority — aggressive buy fills cheapest ask first
    // -----------------------------------------------------------------------
    @Test
    void testPricePriority() {
        // Two asks: 99 and 101
        Order sell99  = makeOrder(1, Side.SELL, TimeInForce.GTC, 99  * PRICE_SCALE, 10);
        Order sell101 = makeOrder(2, Side.SELL, TimeInForce.GTC, 101 * PRICE_SCALE, 10);
        book.addOrder(sell99,  cb);
        book.addOrder(sell101, cb);

        // Buy 10 @ 105 — should match the cheaper 99 ask
        Order buy = makeOrder(3, Side.BUY, TimeInForce.GTC, 105 * PRICE_SCALE, 10);
        book.addOrder(buy, cb);

        assertEquals(1, fills.size());
        assertEquals(99 * PRICE_SCALE, fills.get(0).price(), "Should fill at 99, not 101");
        assertEquals(1,                fills.get(0).passiveId());
        assertEquals(101 * PRICE_SCALE, book.bestAsk(), "101 ask should still rest");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private long nextOrderId = 100;

    private Order makeOrder(long id, Side side, TimeInForce tif, long price, long qty) {
        Order o = orderPool.borrow();
        assertNotNull(o, "Pool exhausted");
        o.internalOrderId = id;
        o.sessionId       = 1;
        o.clientSeqNo     = id;
        o.instrumentId    = INSTR;
        o.side            = side;
        o.tif             = tif;
        o.price           = price;
        o.qty             = qty;
        o.origQty         = qty;
        o.recvTsNanos     = System.nanoTime();
        return o;
    }
}
