package com.oms.gateway.session;

import io.netty.channel.Channel;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Gateway-side session state for a connected client.
 *
 * Thread safety: sessionId and clientId are immutable after logon.
 * lastSeqNo and channel are volatile for visibility between the Netty I/O thread
 * and the Aeron poller thread.
 */
public final class ClientSession {

    public final int    sessionId;
    public final long   clientId;
    public volatile Channel channel;

    // Last processed client sequence number; monotonically increasing.
    // Volatile: read by Aeron reply thread, written by Netty I/O thread.
    private volatile long lastSeqNo = 0;

    public ClientSession(int sessionId, long clientId, Channel channel) {
        this.sessionId = sessionId;
        this.clientId  = clientId;
        this.channel   = channel;
    }

    /**
     * Validate and advance sequence number.
     * @return ACCEPT, DUPLICATE, or GAP
     */
    public SeqNoResult validateAndAdvance(long seqNo) {
        long expected = lastSeqNo + 1;
        if (seqNo < expected) return SeqNoResult.DUPLICATE;
        if (seqNo > expected) return SeqNoResult.GAP;
        lastSeqNo = seqNo;
        return SeqNoResult.ACCEPT;
    }

    public long lastSeqNo() { return lastSeqNo; }

    public enum SeqNoResult { ACCEPT, DUPLICATE, GAP }
}
