package com.oms.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.oms.common.OmsConfig;
import com.oms.protocol.Side;
import com.oms.protocol.Messages;
import com.oms.protocol.TimeInForce;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance per browser WebSocket connection.
 *
 * Lifecycle:
 *   channelActive   → create BridgeSession + GatewayTcpClient → connect to gateway → LOGON
 *   channelRead     → parse JSON from browser → call GatewayTcpClient
 *   channelInactive → close TCP client
 *
 * JSON inbound message types (from browser):
 *   NEW_ORDER:      { type, instrumentId, symbol, side, price, qty, tif, clientSeqNo }
 *   CANCEL_REQUEST: { type, instrumentId, internalOrderId, clientSeqNo }
 *
 * JSON outbound (to browser) is produced by GatewayResponseHandler via BridgeSession.
 */
public final class WsBridgeHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(WsBridgeHandler.class);

    private final OmsConfig       cfg;
    private final NioEventLoopGroup workerGroup;

    private BridgeSession     session;
    private GatewayTcpClient  tcpClient;

    public WsBridgeHandler(OmsConfig cfg, NioEventLoopGroup workerGroup) {
        this.cfg         = cfg;
        this.workerGroup = workerGroup;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        session   = new BridgeSession(ctx.channel());
        tcpClient = new GatewayTcpClient(session, workerGroup);

        log.info("WS client connected — clientId={}, connecting to gateway", session.clientId);
        tcpClient.connect("localhost", cfg.gatewayPort);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("WS client disconnected — clientId={}", session != null ? session.clientId : "?");
        if (tcpClient != null) tcpClient.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("WS error for clientId={}: {}", session != null ? session.clientId : "?", cause.getMessage());
        ctx.close();
    }

    // ── Message handling ──────────────────────────────────────────────────────

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (!(frame instanceof TextWebSocketFrame)) return;
        String text = ((TextWebSocketFrame) frame).text();

        try {
            JsonNode msg = JsonMessages.parse(text);
            String type  = JsonMessages.msgType(msg);

            switch (type) {
                case "NEW_ORDER"      -> handleNewOrder(msg);
                case "CANCEL_REQUEST" -> handleCancel(msg);
                default -> log.warn("Unknown message type from browser: {}", type);
            }
        } catch (Exception e) {
            log.error("Failed to handle WS message: {} — {}", text, e.getMessage());
            ctx.writeAndFlush(new TextWebSocketFrame(JsonMessages.error("Bad message: " + e.getMessage())));
        }
    }

    private void handleNewOrder(JsonNode msg) {
        if (!session.isReady()) {
            log.warn("Client {} not logged on, dropping NEW_ORDER", session.clientId);
            return;
        }

        int         instrumentId = msg.get("instrumentId").asInt();
        Side        side         = Side.valueOf(msg.get("side").asText());
        TimeInForce tif          = TimeInForce.valueOf(msg.get("tif").asText());
        double      price        = msg.get("price").asDouble();
        long        qty          = msg.get("qty").asLong();

        // Use bridge's monotonic seqNo, or accept one from the browser
        long clientSeqNo = msg.has("clientSeqNo")
                ? msg.get("clientSeqNo").asLong()
                : session.nextSeqNo();

        long priceScaled = Math.round(price * Messages.PRICE_SCALE);

        log.debug("Client {} NEW_ORDER: instr={} {} {}@{} {}", session.clientId,
                instrumentId, side, qty, price, tif);

        tcpClient.sendNewOrder(clientSeqNo, instrumentId, side, tif, priceScaled, qty);
    }

    private void handleCancel(JsonNode msg) {
        if (!session.isReady()) return;

        long clientSeqNo    = msg.has("clientSeqNo") ? msg.get("clientSeqNo").asLong() : session.nextSeqNo();
        long internalOrderId = msg.get("internalOrderId").asLong();
        int  instrumentId    = msg.get("instrumentId").asInt();

        tcpClient.sendCancel(clientSeqNo, internalOrderId, instrumentId);
    }
}
