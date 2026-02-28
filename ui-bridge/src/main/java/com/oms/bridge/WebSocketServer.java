package com.oms.bridge;

import com.oms.common.OmsConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty HTTP server that upgrades GET /ws to WebSocket.
 *
 * Pipeline per connection:
 *   HttpServerCodec
 *   → HttpObjectAggregator          (reassemble HTTP requests)
 *   → WebSocketServerCompressionHandler
 *   → WebSocketServerProtocolHandler (handles upgrade + ping/pong)
 *   → WsBridgeHandler               (our application logic)
 */
public final class WebSocketServer {

    private static final Logger log = LoggerFactory.getLogger(WebSocketServer.class);
    private static final String WS_PATH = "/ws";

    private final OmsConfig        cfg;
    private final int              port;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;

    private Channel serverChannel;

    public WebSocketServer(OmsConfig cfg, int port,
                            NioEventLoopGroup bossGroup, NioEventLoopGroup workerGroup) {
        this.cfg         = cfg;
        this.port        = port;
        this.bossGroup   = bossGroup;
        this.workerGroup = workerGroup;
    }

    public void start() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(65536))
                                .addLast(new WebSocketServerCompressionHandler())
                                // Handles the HTTP→WS upgrade handshake and WS control frames
                                .addLast(new WebSocketServerProtocolHandler(WS_PATH, null, true))
                                // CORS — allow any origin (dev convenience; restrict in prod)
                                .addLast(new CorsHandler())
                                // Our logic: one per connection, NOT @Sharable
                                .addLast(new WsBridgeHandler(cfg, workerGroup));
                    }
                });

        serverChannel = b.bind(port).sync().channel();
        log.info("WebSocket server listening on ws://localhost:{}{}", port, WS_PATH);
    }

    public void stop() throws InterruptedException {
        if (serverChannel != null) serverChannel.close().sync();
    }

    /**
     * Minimal CORS handler — adds Access-Control-Allow-Origin: * to HTTP responses
     * so the browser can connect from any origin during development.
     */
    @ChannelHandler.Sharable
    private static final class CorsHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpRequest req) {
                if (req.method() == HttpMethod.OPTIONS) {
                    FullHttpResponse resp = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                    resp.headers()
                            .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                            .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "*");
                    ctx.writeAndFlush(resp);
                    return;
                }
                // Add CORS header to upgrade response (Netty WS handler will carry it)
                req.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            }
            super.channelRead(ctx, msg);
        }
    }
}
