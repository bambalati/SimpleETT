package com.oms.bridge;

import com.oms.protocol.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;

/**
 * Netty handler on the TCP client side (bridge ↔ gateway).
 *
 * Reads length-prefixed binary frames from the gateway, parses them,
 * converts to JSON, and writes TextWebSocketFrames back to the browser.
 *
 * Frame format received from gateway:
 *   [2 bytes LE: length][1 byte: MsgType][N bytes: payload]
 *   where length = 1 + N  (bytes after the length field)
 *
 * The LengthFieldBasedFrameDecoder upstream strips the 2-byte length field,
 * so this handler receives [type(1)][payload(N)] as a ByteBuf.
 */
public final class GatewayResponseHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(GatewayResponseHandler.class);

    private final BridgeSession session;

    public GatewayResponseHandler(BridgeSession session) {
        this.session = session;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            if (buf.readableBytes() < 1) return;
            byte typeCode = buf.readByte();

            MsgType type;
            try {
                type = MsgType.fromCode(typeCode);
            } catch (IllegalArgumentException e) {
                log.warn("Session {}: unknown gateway response type 0x{}", session.clientId, Integer.toHexString(typeCode & 0xFF));
                return;
            }

            String json = switch (type) {
                case LOGON_ACK   -> handleLogonAck(buf);
                case ACK         -> handleAck(buf);
                case FILL        -> handleFill(buf);
                case REJECT      -> handleReject(buf);
                case CANCEL_ACK  -> handleCancelAck(buf);
                default -> {
                    log.warn("Session {}: unhandled response type {}", session.clientId, type);
                    yield null;
                }
            };

            if (json != null && session.wsChannel.isActive()) {
                session.wsChannel.writeAndFlush(new TextWebSocketFrame(json));
            }
        } finally {
            buf.release();
        }
    }

    private String handleLogonAck(ByteBuf buf) {
        int sessionId = buf.readIntLE();
        session.gatewaySessionId = sessionId;
        log.info("Client {} logged on — gateway sessionId={}", session.clientId, sessionId);
        return JsonMessages.logonAck(sessionId);
    }

    private String handleAck(ByteBuf buf) {
        // ACK payload: internalId(8) clientSeqNo(8) sessionId(4) instrumentId(4) ts(8)
        long internalOrderId = buf.readLongLE();
        long clientSeqNo     = buf.readLongLE();
        int  sessionId       = buf.readIntLE();
        int  instrumentId    = buf.readIntLE();
        long tsNanos         = buf.readLongLE();
        return JsonMessages.ack(internalOrderId, clientSeqNo, instrumentId, tsNanos);
    }

    private String handleFill(ByteBuf buf) {
        // FILL payload: internalId(8) sessionId(4) instrumentId(4) side(1) fillPrice(8) fillQty(8) leavesQty(8) ts(8)
        long internalOrderId = buf.readLongLE();
        int  sessionId       = buf.readIntLE();
        int  instrumentId    = buf.readIntLE();
        byte sideCode        = buf.readByte();
        long fillPrice       = buf.readLongLE();
        long fillQty         = buf.readLongLE();
        long leavesQty       = buf.readLongLE();
        long tsNanos         = buf.readLongLE();
        String side = sideCode == Side.BUY.code ? "BUY" : "SELL";
        return JsonMessages.fill(internalOrderId, sessionId, instrumentId, side, fillPrice, fillQty, leavesQty, tsNanos);
    }

    private String handleReject(ByteBuf buf) {
        // REJECT payload: sessionId(4) clientSeqNo(8) reason(1)
        int  sessionId   = buf.readIntLE();
        long clientSeqNo = buf.readLongLE();
        byte reasonCode  = buf.readByte();
        String reason = RejectReason.fromCode(reasonCode).name();
        return JsonMessages.reject(sessionId, clientSeqNo, reason);
    }

    private String handleCancelAck(ByteBuf buf) {
        // CANCEL_ACK payload: internalId(8) sessionId(4)
        long internalOrderId = buf.readLongLE();
        int  sessionId       = buf.readIntLE();
        return JsonMessages.cancelAck(internalOrderId, sessionId);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("Gateway TCP connection closed for client {}", session.clientId);
        if (session.wsChannel.isActive()) {
            session.wsChannel.writeAndFlush(new TextWebSocketFrame(
                    JsonMessages.error("Gateway connection lost")));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Gateway TCP error for client {}: {}", session.clientId, cause.getMessage());
        ctx.close();
    }
}
