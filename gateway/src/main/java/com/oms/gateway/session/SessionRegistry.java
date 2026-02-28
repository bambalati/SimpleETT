package com.oms.gateway.session;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry of active client sessions.
 * Accessed from both Netty I/O threads and Aeron reply thread;
 * protected by simple synchronized blocks (low contention: connects/disconnects are rare).
 */
public final class SessionRegistry {

    private final Int2ObjectOpenHashMap<ClientSession> sessions = new Int2ObjectOpenHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);

    public synchronized ClientSession register(long clientId, io.netty.channel.Channel channel) {
        int id = nextId.getAndIncrement();
        ClientSession session = new ClientSession(id, clientId, channel);
        sessions.put(id, session);
        return session;
    }

    public synchronized ClientSession get(int sessionId) {
        return sessions.get(sessionId);
    }

    public synchronized void remove(int sessionId) {
        sessions.remove(sessionId);
    }
}
