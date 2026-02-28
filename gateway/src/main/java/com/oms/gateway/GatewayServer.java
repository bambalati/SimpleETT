package com.oms.gateway;

import com.oms.common.OmsConfig;
import com.oms.gateway.aeron.AeronPublisher;
import com.oms.gateway.session.SessionRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Netty TCP server accepting OMS client connections.
 *
 * Pipeline per channel:
 *   LengthFieldBasedFrameDecoder → GatewayHandler
 *
 * LengthField: 2 bytes LE, value = total frame size including header.
 * The decoder strips the length field and passes the remainder (type + payload) to GatewayHandler.
 */
public final class GatewayServer {

    private static final Logger log = LoggerFactory.getLogger(GatewayServer.class);

    private final OmsConfig cfg;
    private final SessionRegistry sessions;
    private final AeronPublisher publisher;
    private final AtomicLong orderIdGen;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public GatewayServer(OmsConfig cfg, SessionRegistry sessions, AeronPublisher publisher,
                         AtomicLong orderIdGen) {
        this.cfg        = cfg;
        this.sessions   = sessions;
        this.publisher  = publisher;
        this.orderIdGen = orderIdGen;
    }

    public void start() throws InterruptedException {
        bossGroup   = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(2);

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // Decode TCP stream into frames.
                                // Length field = first 2 bytes (LE), value = total frame length.
                                // We want to pass full frames (incl header) to GatewayHandler,
                                // so initialBytesToStrip=0.
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        ByteOrder.LITTLE_ENDIAN,
                                        /* maxFrameLen */ 65535,
                                        /* lengthOffset */ 0,
                                        /* lengthFieldLen */ 2,
                                        /* adjustment */ 0,
                                        /* strip */ 0,
                                        /* failFast */ true))
                                // One handler instance per channel (not @Sharable) so each
                                // channel has its own scratch buffer.
                                .addLast(new GatewayHandler(cfg, sessions, publisher, orderIdGen));
                    }
                });

        ChannelFuture future = bootstrap.bind(cfg.gatewayPort).sync();
        serverChannel = future.channel();
        log.info("Gateway TCP server listening on port {}", cfg.gatewayPort);
    }

    public void stop() throws InterruptedException {
        if (serverChannel != null) serverChannel.close().sync();
        if (bossGroup   != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("Gateway TCP server stopped.");
    }
}
