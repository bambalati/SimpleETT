package com.oms.bridge;

import com.oms.common.OmsConfig;
import io.netty.channel.nio.NioEventLoopGroup;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket ↔ OMS Gateway bridge.
 *
 * Browser (JSON/WS) ──► BridgeMain ──► Gateway TCP (binary) ──► Aeron ──► Engine
 *
 * Usage:
 *   java -jar ui-bridge-fat.jar [oms-config-path] [ws-port]
 *
 * Defaults: connects to gateway at localhost:7001, WS on ws://localhost:8080/ws
 */
public final class BridgeMain {

    private static final Logger log = LoggerFactory.getLogger(BridgeMain.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        int    wsPort     = args.length > 1 ? Integer.parseInt(args[1]) : 8080;

        OmsConfig cfg = OmsConfig.load(configPath);

        log.info("Starting OMS UI Bridge: ws://localhost:{}/ws → gateway localhost:{}", wsPort, cfg.gatewayPort);

        // Shared Netty thread pools for both WS server and TCP client connections
        NioEventLoopGroup bossGroup   = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(4);

        WebSocketServer wsServer = new WebSocketServer(cfg, wsPort, bossGroup, workerGroup);
        wsServer.start();

        log.info("Bridge UP — open the React UI and connect to ws://localhost:{}/ws", wsPort);
        new ShutdownSignalBarrier().await();

        wsServer.stop();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        log.info("Bridge stopped.");
    }
}
