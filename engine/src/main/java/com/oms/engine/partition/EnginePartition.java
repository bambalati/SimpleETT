package com.oms.engine.partition;

import com.oms.common.OmsConfig;
import com.oms.common.PartitionUtil;
import com.oms.engine.book.LimitOrderBook;
import com.oms.engine.book.Order;
import com.oms.engine.book.OrderPool;
import com.oms.engine.book.PriceLevelPool;
import com.oms.protocol.*;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-threaded engine partition. Owns a subset of instruments determined by
 *   partition = instrumentId % numPartitions
 *
 * Subscribes to inbound Aeron stream (1000 + partition).
 * Publishes outbound events to Aeron stream (2000 + partition).
 *
 * Run loop: poll inbound -> match -> publish outbound.
 * No locks; single thread per partition.
 */
public final class EnginePartition implements Runnable, FragmentHandler {

    private static final Logger log = LoggerFactory.getLogger(EnginePartition.class);

    private static final int ORDER_POOL_SIZE = 100_000;
    private static final int LEVEL_POOL_SIZE = 50_000;

    private final int partitionId;
    private final OmsConfig cfg;
    private final Aeron aeron;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Lazy-created books per instrument
    private final Int2ObjectOpenHashMap<LimitOrderBook> books = new Int2ObjectOpenHashMap<>(512);
    private final OrderPool orderPool = new OrderPool(ORDER_POOL_SIZE);
    private final PriceLevelPool levelPool = new PriceLevelPool(LEVEL_POOL_SIZE);

    // Pre-allocated output buffer (no allocation in hot path)
    private final UnsafeBuffer outBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

    private Subscription inboundSub;
    private Publication  outboundPub;

    // Monotonic internal order id (partition-local; unique combined with partitionId)
    private long nextOrderId = 1;

    public EnginePartition(int partitionId, OmsConfig cfg, Aeron aeron) {
        this.partitionId = partitionId;
        this.cfg = cfg;
        this.aeron = aeron;
    }

    public void start() {
        int inStream  = PartitionUtil.inboundStream(partitionId, cfg);
        int outStream = PartitionUtil.outboundStream(partitionId, cfg);

        inboundSub  = aeron.addSubscription(cfg.aeronChannel, inStream);
        outboundPub = aeron.addPublication(cfg.aeronChannel, outStream);

        log.info("Partition {} started: sub stream={} pub stream={}", partitionId, inStream, outStream);

        Thread t = new Thread(this, "engine-partition-" + partitionId);
        t.setDaemon(false);
        t.start();
    }

    public void stop() { running.set(false); }

    @Override
    public void run() {
        while (running.get()) {
            int fragments = inboundSub.poll(this, 256);
            if (fragments == 0) {
                Thread.yield();
            }
        }
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (length < 1) return;
        // First byte is the MsgType code (no TCP frame header in Aeron)
        byte typeCode = buffer.getByte(offset);
        MsgType type;
        try {
            type = MsgType.fromCode(typeCode);
        } catch (IllegalArgumentException e) {
            log.warn("Partition {}: unknown msg type {}", partitionId, typeCode);
            return;
        }

        switch (type) {
            case NEW_ORDER      -> handleNewOrder(buffer, offset + 1);
            case CANCEL_REQUEST -> handleCancel(buffer, offset + 1);
            default -> log.warn("Partition {}: unexpected msg type {}", partitionId, type);
        }
    }

    private void handleNewOrder(DirectBuffer buf, int offset) {
        long internalOrderId = buf.getLong(offset + Messages.INOS_INTERNAL_ID_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN);
        int  sessionId       = buf.getInt(offset + Messages.INOS_SESSION_ID_OFFSET,   java.nio.ByteOrder.LITTLE_ENDIAN);
        long clientSeqNo     = buf.getLong(offset + Messages.INOS_CLIENT_SEQNO_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN);
        int  instrumentId    = buf.getInt(offset + Messages.INOS_INSTRUMENT_ID_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN);
        Side side            = Side.fromCode(buf.getByte(offset + Messages.INOS_SIDE_OFFSET));
        TimeInForce tif      = TimeInForce.fromCode(buf.getByte(offset + Messages.INOS_TIF_OFFSET));
        long price           = buf.getLong(offset + Messages.INOS_PRICE_OFFSET,  java.nio.ByteOrder.LITTLE_ENDIAN);
        long qty             = buf.getLong(offset + Messages.INOS_QTY_OFFSET,    java.nio.ByteOrder.LITTLE_ENDIAN);
        long recvTs          = buf.getLong(offset + Messages.INOS_RECV_TS_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN);

        Order order = orderPool.borrow();
        if (order == null) {
            publishReject(sessionId, clientSeqNo, RejectReason.SYSTEM_BUSY);
            return;
        }

        order.internalOrderId = internalOrderId;
        order.sessionId       = sessionId;
        order.clientSeqNo     = clientSeqNo;
        order.instrumentId    = instrumentId;
        order.side            = side;
        order.tif             = tif;
        order.price           = price;
        order.qty             = qty;
        order.origQty         = qty;
        order.recvTsNanos     = recvTs;

        // Publish ack first
        publishAck(internalOrderId, clientSeqNo, sessionId, instrumentId);

        LimitOrderBook book = getOrCreateBook(instrumentId);
        book.addOrder(order, fillCallback);
    }

    private void handleCancel(DirectBuffer buf, int offset) {
        int  sessionId      = buf.getInt(offset + Messages.CANCEL_SESSION_ID_OFFSET,   java.nio.ByteOrder.LITTLE_ENDIAN);
        long clientSeqNo    = buf.getLong(offset + Messages.CANCEL_CLIENT_SEQNO_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN);
        long internalOrderId = buf.getLong(offset + Messages.CANCEL_INTERNAL_ID_OFFSET,  java.nio.ByteOrder.LITTLE_ENDIAN);

        // Find which instrument this order belongs to - need to check all books in this partition
        // In practice, order map should include instrumentId. For simplicity scan isn't needed
        // because we store the instrument on the Order via orderMap in each book.
        // We'll broadcast cancel to all books; only one will have it.
        boolean found = false;
        for (LimitOrderBook book : books.values()) {
            if (book.cancel(internalOrderId)) {
                found = true;
                publishCancelAck(internalOrderId, sessionId);
                break;
            }
        }
        if (!found) {
            publishReject(sessionId, clientSeqNo, RejectReason.ORDER_NOT_FOUND);
        }
    }

    private final LimitOrderBook.MatchCallback fillCallback = (aggressorId, passiveId,
                                                                aggressorSessionId, passiveSessionId,
                                                                instrumentId, side,
                                                                fillPrice, fillQty,
                                                                aggressorLeaves, passiveLeaves) -> {
        publishFill(aggressorId, aggressorSessionId, instrumentId, side, fillPrice, fillQty, aggressorLeaves);
        publishFill(passiveId,   passiveSessionId,   instrumentId, side.equals(Side.BUY) ? Side.SELL : Side.BUY,
                fillPrice, fillQty, passiveLeaves);
    };

    // ---- Publish helpers ----

    private void publishAck(long internalOrderId, long clientSeqNo, int sessionId, int instrumentId) {
        int len = Messages.encodeAeronAck(outBuf, 0, internalOrderId, clientSeqNo, sessionId, instrumentId, System.nanoTime());
        offer(len);
    }

    private void publishFill(long internalOrderId, int sessionId, int instrumentId,
                              Side side, long fillPrice, long fillQty, long leavesQty) {
        int len = Messages.encodeAeronFill(outBuf, 0, internalOrderId, sessionId, instrumentId,
                side, fillPrice, fillQty, leavesQty, System.nanoTime());
        offer(len);
    }

    private void publishReject(int sessionId, long clientSeqNo, RejectReason reason) {
        int len = Messages.encodeAeronReject(outBuf, 0, sessionId, clientSeqNo, reason);
        offer(len);
    }

    private void publishCancelAck(long internalOrderId, int sessionId) {
        int len = Messages.encodeAeronCancelAck(outBuf, 0, internalOrderId, sessionId);
        offer(len);
    }

    private void offer(int len) {
        long result;
        int attempts = 0;
        do {
            result = outboundPub.offer(outBuf, 0, len);
            if (result > 0) return;
            attempts++;
        } while ((result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) && attempts < 3);
        if (result < 0) {
            log.warn("Partition {} failed to publish outbound: {}", partitionId, result);
        }
    }

    private LimitOrderBook getOrCreateBook(int instrumentId) {
        LimitOrderBook book = books.get(instrumentId);
        if (book == null) {
            book = new LimitOrderBook(instrumentId, orderPool, levelPool);
            books.put(instrumentId, book);
        }
        return book;
    }
}
