package com.oms.gateway.aeron;

import com.oms.common.LatencyStats;
import com.oms.common.OmsConfig;
import com.oms.common.PartitionUtil;
import com.oms.gateway.session.ClientSession;
import com.oms.gateway.session.SessionRegistry;
import com.oms.protocol.Messages;
import com.oms.protocol.MsgType;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Subscribes to all outbound engine streams and forwards messages to the
 * appropriate client channel. Runs on a dedicated daemon thread.
 */
public final class AeronSubscriber implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AeronSubscriber.class);

    private final OmsConfig cfg;
    private final Aeron aeron;
    private final SessionRegistry sessions;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LatencyStats ackLatency = new LatencyStats("ack-latency");
    private final byte[]       fwdBuf    = new byte[4096];

    public AeronSubscriber(OmsConfig cfg, Aeron aeron, SessionRegistry sessions) {
        this.cfg      = cfg;
        this.aeron    = aeron;
        this.sessions = sessions;
    }

    public void start() {
        Thread t = new Thread(this, "gateway-aeron-subscriber");
        t.setDaemon(true);
        t.start();
        log.info("AeronSubscriber started, polling {} partitions", cfg.partitions);
    }

    public void stop() { running.set(false); }

    @Override
    public void run() {
        Subscription[] subs = new Subscription[cfg.partitions];
        for (int i = 0; i < cfg.partitions; i++) {
            int stream = PartitionUtil.outboundStream(i, cfg);
            subs[i] = aeron.addSubscription(cfg.aeronChannel, stream);
        }

        FragmentHandler handler = this::onFragment;

        while (running.get()) {
            int fragments = 0;
            for (Subscription sub : subs) {
                fragments += sub.poll(handler, 64);
            }
            if (fragments == 0) Thread.yield();
        }

        for (Subscription s : subs) s.close();
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (length < 1) return;
        byte typeCode = buffer.getByte(offset);
        MsgType type;
        try {
            type = MsgType.fromCode(typeCode);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown outbound msg type: {}", typeCode);
            return;
        }

        // For all outbound msgs, sessionId is at a known offset after the type byte.
        // Extract sessionId and route to correct client channel.
        int payloadOffset = offset + 1;

        switch (type) {
            case ACK -> {
                int sessionId = buffer.getInt(payloadOffset + Messages.ACK_SESSION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
                long recvTs   = buffer.getLong(payloadOffset + Messages.ACK_TS_OFFSET, ByteOrder.LITTLE_ENDIAN);
                ackLatency.record(System.nanoTime() - recvTs);
                forwardToClient(sessionId, buffer, payloadOffset - 1, length);
            }
            case FILL -> {
                int sessionId = buffer.getInt(payloadOffset + Messages.FILL_SESSION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
                forwardToClient(sessionId, buffer, payloadOffset - 1, length);
            }
            case REJECT -> {
                int sessionId = buffer.getInt(payloadOffset + Messages.REJECT_SESSION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
                forwardToClient(sessionId, buffer, payloadOffset - 1, length);
            }
            case CANCEL_ACK -> {
                int sessionId = buffer.getInt(payloadOffset + Messages.CACK_SESSION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
                forwardToClient(sessionId, buffer, payloadOffset - 1, length);
            }
            default -> log.warn("Unhandled outbound type: {}", type);
        }
    }

    /**
     * Wraps the Aeron fragment in a TCP frame and writes it to the client channel.
     * Aeron fragment: [type(1)][payload(N)] = length bytes total
     * TCP frame: [len(2)][type(1)][payload(N)] where len = 1 + N = length
     */
    private void forwardToClient(int sessionId, DirectBuffer buf, int offset, int length) {
        ClientSession session = sessions.get(sessionId);
        if (session == null) return;
        Channel ch = session.channel;
        if (ch == null || !ch.isActive()) return;

        ByteBuf out = ch.alloc().buffer(2 + length);
        out.writeShortLE(length); // len = bytes after this field = type(1) + payload(N)
        buf.getBytes(offset, fwdBuf, 0, length);
        out.writeBytes(fwdBuf, 0, length);
        ch.writeAndFlush(out);
    }
}
