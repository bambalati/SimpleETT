package com.oms.gateway.aeron;

import com.oms.common.OmsConfig;
import com.oms.common.PartitionUtil;
import com.oms.protocol.*;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * One Publication per partition (lazily created). Thread-safe per publication
 * because each Netty I/O thread will target a different partition.
 * For simplicity we synchronize per-publication; at scale use thread-local buffers.
 */
public final class AeronPublisher {

    private static final Logger log = LoggerFactory.getLogger(AeronPublisher.class);

    private final OmsConfig cfg;
    private final Aeron aeron;
    private final Publication[] pubs;

    // Per-thread scratch buffer — keeps hot path allocation-free.
    // Each call writes into this buffer before offering; safe because offer is sync.
    private final UnsafeBuffer scratchBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    public AeronPublisher(OmsConfig cfg, Aeron aeron) {
        this.cfg  = cfg;
        this.aeron = aeron;
        this.pubs  = new Publication[cfg.partitions];
    }

    public boolean publishNewOrder(long internalOrderId,
                                   int sessionId, long clientId, long clientSeqNo,
                                   int instrumentId, Side side, TimeInForce tif,
                                   long price, long qty, long recvTsNanos) {
        int partition = PartitionUtil.partition(instrumentId, cfg.partitions);
        Publication pub = getOrCreatePub(partition);

        // Layout: [1 byte type][INOS_PAYLOAD_SIZE bytes]
        scratchBuf.putByte(0, MsgType.NEW_ORDER.code);
        Messages.encodeInternalNewOrder(scratchBuf, 1,
                internalOrderId, sessionId, clientId, clientSeqNo,
                instrumentId, side, tif, price, qty, recvTsNanos);

        return offer(pub, 1 + Messages.INOS_PAYLOAD_SIZE);
    }

    public boolean publishCancel(int sessionId, long clientSeqNo, long internalOrderId, int instrumentId) {
        int partition = PartitionUtil.partition(instrumentId, cfg.partitions);
        Publication pub = getOrCreatePub(partition);

        scratchBuf.putByte(0, MsgType.CANCEL_REQUEST.code);
        Messages.encodeInternalCancel(scratchBuf, 1, sessionId, clientSeqNo, internalOrderId, instrumentId);

        return offer(pub, 1 + Messages.CANCEL_PAYLOAD_SIZE);
    }

    private synchronized Publication getOrCreatePub(int partition) {
        if (pubs[partition] == null) {
            int stream = PartitionUtil.inboundStream(partition, cfg);
            pubs[partition] = aeron.addPublication(cfg.aeronChannel, stream);
            log.info("Created publication for partition {} stream {}", partition, stream);
        }
        return pubs[partition];
    }

    private boolean offer(Publication pub, int len) {
        long result = pub.offer(scratchBuf, 0, len);
        if (result > 0) return true;
        if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
            log.warn("Aeron back-pressure on offer: {}", result);
            return false;
        }
        log.error("Aeron offer failed: {}", result);
        return false;
    }

    public void close() {
        for (Publication p : pubs) {
            if (p != null) p.close();
        }
    }
}
