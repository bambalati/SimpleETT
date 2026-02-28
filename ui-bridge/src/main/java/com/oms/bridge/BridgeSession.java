package com.oms.bridge;

import io.netty.channel.Channel;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds state for one active WS ↔ TCP session pair.
 *
 * One BridgeSession is created per browser WebSocket connection.
 * It owns:
 *   - the browser-facing WebSocket channel
 *   - the gateway-facing TCP channel (set after connect+logon)
 *   - a monotonic clientSeqNo for idempotency
 *   - the gateway-assigned sessionId (set after LOGON_ACK)
 */
public final class BridgeSession {

    /** Incrementing counter across all bridge sessions — used as clientId. */
    private static final AtomicLong CLIENT_ID_GEN = new AtomicLong(1);

    public final long    clientId;
    public final Channel wsChannel;

    // Set after gateway sends LOGON_ACK
    public volatile int  gatewaySessionId = -1;

    // Gateway TCP channel — set once connected and ready
    public volatile Channel tcpChannel;

    // Monotonic sequence number sent in every order to the gateway
    private final AtomicLong seqNo = new AtomicLong(1);

    public BridgeSession(Channel wsChannel) {
        this.wsChannel = wsChannel;
        this.clientId  = CLIENT_ID_GEN.getAndIncrement();
    }

    public long nextSeqNo() { return seqNo.getAndIncrement(); }

    public boolean isReady() {
        return gatewaySessionId >= 0 && tcpChannel != null && tcpChannel.isActive();
    }
}
