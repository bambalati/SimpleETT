package com.oms.bridge;

import com.oms.protocol.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Maintains a single TCP connection from the bridge to the OMS gateway.
 * One instance per browser WebSocket connection.
 *
 * Pipeline (reading from gateway):
 *   LengthFieldBasedFrameDecoder → GatewayResponseHandler
 *
 * The decoder strips the 2-byte length prefix, delivering [type][payload] to the handler.
 *
 * Writing: encode binary frames into a direct ByteBuffer and flush.
 */
public final class GatewayTcpClient {

    private static final Logger log = LoggerFactory.getLogger(GatewayTcpClient.class);

    private final BridgeSession       session;
    private final NioEventLoopGroup   workerGroup;
    private Channel                   channel;

    // Pre-allocated scratch buffer for encoding outbound messages (bridge thread only)
    private final UnsafeBuffer scratch  = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
    private final byte[]       writeBuf = new byte[512];

    public GatewayTcpClient(BridgeSession session, NioEventLoopGroup workerGroup) {
        this.session     = session;
        this.workerGroup = workerGroup;
    }

    /**
     * Connect to the gateway TCP server and send LOGON.
     * Calls back on the Netty I/O thread once ready.
     */
    public ChannelFuture connect(String host, int port) {
        Bootstrap b = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // Decode gateway responses: strip the 2-byte length prefix.
                                // length field value = 1 (type) + N (payload bytes)
                                // initialBytesToStrip=2 → handler receives [type(1)][payload(N)]
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        65535,  // maxFrameLength
                                        0,      // lengthFieldOffset
                                        2,      // lengthFieldLength
                                        0,      // lengthAdjustment
                                        2))     // initialBytesToStrip (remove length header)
                                .addLast(new GatewayResponseHandler(session));
                    }
                });

        ChannelFuture future = b.connect(host, port);
        future.addListener(f -> {
            if (f.isSuccess()) {
                channel = future.channel();
                session.tcpChannel = channel;
                log.info("Client {} connected to gateway {}:{}", session.clientId, host, port);
                sendLogon();
            } else {
                log.error("Client {} failed to connect to gateway: {}", session.clientId, f.cause().getMessage());
            }
        });
        return future;
    }

    // ── Outbound message writers ───────────────────────────────────────────

    private void sendLogon() {
        int len = Messages.encodeLogon(scratch, 0, 0 /*sessId placeholder*/, session.clientId);
        write(len);
        log.debug("Client {} sent LOGON", session.clientId);
    }

    public void sendNewOrder(long clientSeqNo,
                              int instrumentId, Side side, TimeInForce tif,
                              long priceScaled, long qty) {
        if (!session.isReady()) {
            log.warn("Client {} not ready, dropping NEW_ORDER", session.clientId);
            return;
        }
        int len = Messages.encodeNewOrder(scratch, 0,
                session.gatewaySessionId,
                session.clientId,
                clientSeqNo,
                instrumentId, side, tif,
                priceScaled, qty,
                System.nanoTime());
        write(len);
    }

    public void sendCancel(long clientSeqNo, long internalOrderId, int instrumentId) {
        if (!session.isReady()) return;
        // Cancel payload: sessionId(4) clientSeqNo(8) internalOrderId(8)
        scratch.putShort(0, (short)(1 + Messages.CANCEL_PAYLOAD_SIZE), ByteOrder.LITTLE_ENDIAN);
        scratch.putByte(2, MsgType.CANCEL_REQUEST.code);
        Messages.encodeInternalCancel(scratch, 3, session.gatewaySessionId, clientSeqNo, internalOrderId);
        write(Messages.FRAME_HEADER_SIZE + Messages.CANCEL_PAYLOAD_SIZE);
    }

    private void write(int len) {
        if (channel == null || !channel.isActive()) {
            log.warn("Client {} TCP channel not active, dropping message", session.clientId);
            return;
        }
        scratch.getBytes(0, writeBuf, 0, len);
        ByteBuf buf = channel.alloc().buffer(len);
        buf.writeBytes(writeBuf, 0, len);
        channel.writeAndFlush(buf);
    }

    public void close() {
        if (channel != null) channel.close();
    }
}
