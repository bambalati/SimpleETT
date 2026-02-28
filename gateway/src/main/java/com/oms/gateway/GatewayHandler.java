package com.oms.gateway;

import com.oms.common.OmsConfig;
import com.oms.gateway.aeron.AeronPublisher;
import com.oms.gateway.session.ClientSession;
import com.oms.gateway.session.SessionRegistry;
import com.oms.protocol.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-channel Netty handler.
 *
 * Framing: [2-byte LE length][1-byte type][payload]
 * The handler reassembles frames using Netty's ReplayingDecoder via a length-field-based
 * accumulation pattern (we use a simple state machine here).
 */
public final class GatewayHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(GatewayHandler.class);

    private final OmsConfig cfg;
    private final SessionRegistry sessions;
    private final AeronPublisher publisher;

    // Monotonic internal order id counter (global across gateway)
    private final AtomicLong orderIdGen;

    // Per-channel scratch buffer (one handler instance per channel — not @Sharable)
    private final UnsafeBuffer scratch = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

    private static final io.netty.util.AttributeKey<ClientSession> SESSION_KEY =
            io.netty.util.AttributeKey.valueOf("session");

    public GatewayHandler(OmsConfig cfg, SessionRegistry sessions, AeronPublisher publisher,
                          AtomicLong orderIdGen) {
        this.cfg        = cfg;
        this.sessions   = sessions;
        this.publisher  = publisher;
        this.orderIdGen = orderIdGen;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            while (buf.readableBytes() >= Messages.FRAME_HEADER_SIZE) {
                buf.markReaderIndex();
                // frameLen = number of bytes following the 2-byte length field = type(1) + payload(N)
                int frameLen  = buf.readUnsignedShortLE();
                if (buf.readableBytes() < frameLen) {
                    buf.resetReaderIndex();
                    break;
                }
                byte typeCode = buf.readByte();
                int payloadLen = frameLen - 1; // subtract type byte
                MsgType type;
                try {
                    type = MsgType.fromCode(typeCode);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown msg type from {}: {}", ctx.channel().remoteAddress(), typeCode);
                    buf.skipBytes(payloadLen);
                    continue;
                }
                handleMessage(ctx, type, buf, payloadLen);
            }
        } finally {
            buf.release();
        }
    }

    private void handleMessage(ChannelHandlerContext ctx, MsgType type, ByteBuf payload, int payloadLen) {
        switch (type) {
            case LOGON          -> handleLogon(ctx, payload);
            case NEW_ORDER      -> handleNewOrder(ctx, payload);
            case CANCEL_REQUEST -> handleCancel(ctx, payload);
            default -> {
                log.warn("Unhandled msg type: {}", type);
                payload.skipBytes(payloadLen);
            }
        }
    }

    private void handleLogon(ChannelHandlerContext ctx, ByteBuf payload) {
        int  sessionId = payload.readIntLE();
        long clientId  = payload.readLongLE();

        ClientSession existing = ctx.channel().attr(SESSION_KEY).get();
        if (existing != null) {
            log.warn("Duplicate logon from session {}", sessionId);
            return;
        }

        ClientSession session = sessions.register(clientId, ctx.channel());
        ctx.channel().attr(SESSION_KEY).set(session);
        log.info("Client logged on: sessionId={} clientId={} assigned sessionId={}", sessionId, clientId, session.sessionId);

        // Send logon ack
        int len = Messages.encodeLogonAck(scratch, 0, session.sessionId);
        ByteBuf resp = ctx.alloc().buffer(len);
        for (int i = 0; i < len; i++) resp.writeByte(scratch.getByte(i));
        ctx.writeAndFlush(resp);
    }

    private void handleNewOrder(ChannelHandlerContext ctx, ByteBuf payload) {
        ClientSession session = ctx.channel().attr(SESSION_KEY).get();
        if (session == null) {
            rejectNoSession(ctx, 0, 0);
            return;
        }

        // Read payload fields (sessId and clientId are echo fields sent by client; skip them)
        payload.skipBytes(12);
        long clientSeqNo = payload.readLongLE();
        int  instrId     = payload.readIntLE();
        byte sideCode    = payload.readByte();
        byte tifCode     = payload.readByte();
        long price       = payload.readLongLE();
        long qty         = payload.readLongLE();
        long recvTs      = payload.readLongLE();

        // Sequence number check
        ClientSession.SeqNoResult seqResult = session.validateAndAdvance(clientSeqNo);
        if (seqResult == ClientSession.SeqNoResult.DUPLICATE) {
            sendReject(ctx, session.sessionId, clientSeqNo, RejectReason.DUPLICATE_SEQNO);
            return;
        }
        if (seqResult == ClientSession.SeqNoResult.GAP) {
            sendReject(ctx, session.sessionId, clientSeqNo, RejectReason.SEQNO_GAP);
            return;
        }

        Side side = Side.fromCode(sideCode);
        TimeInForce tif = TimeInForce.fromCode(tifCode);
        long internalId = orderIdGen.getAndIncrement();
        long now = System.nanoTime();

        boolean published = publisher.publishNewOrder(internalId, session.sessionId, session.clientId,
                clientSeqNo, instrId, side, tif, price, qty, now);

        if (!published) {
            sendReject(ctx, session.sessionId, clientSeqNo, RejectReason.SYSTEM_BUSY);
        }
    }

    private void handleCancel(ChannelHandlerContext ctx, ByteBuf payload) {
        ClientSession session = ctx.channel().attr(SESSION_KEY).get();
        if (session == null) {
            payload.skipBytes(Messages.CANCEL_PAYLOAD_SIZE);
            return;
        }

        payload.skipBytes(4); // sessId echo
        long clientSeqNo     = payload.readLongLE();
        long internalOrderId = payload.readLongLE();
        int  instrumentId    = payload.readIntLE();

        publisher.publishCancel(session.sessionId, clientSeqNo, internalOrderId, instrumentId);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ClientSession session = ctx.channel().attr(SESSION_KEY).get();
        if (session != null) {
            sessions.remove(session.sessionId);
            log.info("Session {} disconnected", session.sessionId);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel error", cause);
        ctx.close();
    }

    private void sendReject(ChannelHandlerContext ctx, int sessionId, long clientSeqNo, RejectReason reason) {
        int len = Messages.encodeReject(scratch, 0, sessionId, clientSeqNo, reason);
        ByteBuf buf = ctx.alloc().buffer(len);
        for (int i = 0; i < len; i++) buf.writeByte(scratch.getByte(i));
        ctx.writeAndFlush(buf);
    }

    private void rejectNoSession(ChannelHandlerContext ctx, int sessionId, long clientSeqNo) {
        sendReject(ctx, sessionId, clientSeqNo, RejectReason.SESSION_NOT_LOGGED_ON);
    }
}
