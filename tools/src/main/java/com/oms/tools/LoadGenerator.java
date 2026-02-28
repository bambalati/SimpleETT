package com.oms.tools;

import com.oms.common.OmsConfig;
import com.oms.protocol.*;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP load generator. Sends NewOrderSingle messages to the gateway and
 * measures round-trip latency (send → ACK/FILL received).
 *
 * Usage:
 *   java -jar loadgen-fat.jar [config-path] [orders-per-second] [duration-secs]
 */
public final class LoadGenerator {

    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);

    // Configurable
    private static int TARGET_OPS = 10_000;   // orders per second
    private static int DURATION_SECS = 10;

    private final String host;
    private final int port;

    private final AtomicLong ackCount   = new AtomicLong();
    private final AtomicLong fillCount  = new AtomicLong();
    private final Histogram  rttHist    = new Histogram(10_000_000_000L, 3);

    // Map seqno -> send timestamp for RTT measurement
    private final long[] sendTimestamps = new long[1 << 20]; // 1M slots, circular

    public LoadGenerator(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        if (args.length > 1) TARGET_OPS     = Integer.parseInt(args[1]);
        if (args.length > 2) DURATION_SECS  = Integer.parseInt(args[2]);

        OmsConfig cfg = OmsConfig.load(configPath);
        new LoadGenerator("localhost", cfg.gatewayPort).run();
    }

    public void run() throws Exception {
        log.info("Connecting to gateway {}:{}", host, port);
        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);
            OutputStream out = socket.getOutputStream();
            InputStream  in  = socket.getInputStream();

            // Logon
            sendLogon(out, 1, 42L);
            byte[] logonAck = readFrame(in);
            log.info("Logon ack received, starting load: {} ops/sec for {} secs",
                    TARGET_OPS, DURATION_SECS);

            // Start reader thread
            CountDownLatch done = new CountDownLatch(1);
            Thread reader = new Thread(() -> readResponses(in, done), "loadgen-reader");
            reader.setDaemon(true);
            reader.start();

            // Send loop with token-bucket rate limiting
            long intervalNs = 1_000_000_000L / TARGET_OPS;
            long totalOrders = (long) TARGET_OPS * DURATION_SECS;
            long seqNo = 1;
            long nextSendAt = System.nanoTime();

            UnsafeBuffer sendBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

            for (long i = 0; i < totalOrders; i++) {
                // Busy-wait to hit target rate
                while (System.nanoTime() < nextSendAt) Thread.onSpinWait();
                nextSendAt += intervalNs;

                long price = 100_000_000L + (i % 100) * 1_000_000L; // 100.000 - 100.100
                long qty   = 100;
                int  instrId = (int)(i % 100) + 1; // instruments 1..100

                sendTimestamps[(int)(seqNo & (sendTimestamps.length - 1))] = System.nanoTime();

                int len = Messages.encodeNewOrder(sendBuf, 0,
                        1, 42L, seqNo, instrId,
                        (i % 2 == 0) ? Side.BUY : Side.SELL,
                        TimeInForce.GTC, price, qty, System.nanoTime());

                byte[] frame = new byte[len];
                sendBuf.getBytes(0, frame);
                out.write(frame);
                seqNo++;
            }
            out.flush();

            log.info("All {} orders sent. Waiting for responses...", totalOrders);
            Thread.sleep(3000);
            done.countDown();
            reader.interrupt();

            printStats(totalOrders);
        }
    }

    private void readResponses(InputStream in, CountDownLatch done) {
        try {
            while (!Thread.interrupted()) {
                byte[] frame = readFrame(in);
                if (frame == null) break;
                processResponse(frame);
            }
        } catch (IOException e) {
            if (!Thread.interrupted()) log.warn("Reader error: {}", e.getMessage());
        }
    }

    private void processResponse(byte[] frame) {
        if (frame.length < 1) return;
        byte typeCode = frame[0];
        MsgType type;
        try { type = MsgType.fromCode(typeCode); }
        catch (IllegalArgumentException e) { return; }

        UnsafeBuffer buf = new UnsafeBuffer(frame);
        int p = 1; // payload offset (type byte stripped)

        switch (type) {
            case ACK -> {
                long clientSeqNo = buf.getLong(p + Messages.ACK_CLIENT_SEQNO_OFFSET, ByteOrder.LITTLE_ENDIAN);
                long sendTs = sendTimestamps[(int)(clientSeqNo & (sendTimestamps.length - 1))];
                if (sendTs > 0) rttHist.recordValue(System.nanoTime() - sendTs);
                ackCount.incrementAndGet();
            }
            case FILL  -> fillCount.incrementAndGet();
            case REJECT -> log.warn("Reject received");
        }
    }

    private void printStats(long sent) {
        long acks = ackCount.get();
        long fills = fillCount.get();
        System.out.printf("%n=== Load Generator Results ===%n");
        System.out.printf("Orders sent: %d%n", sent);
        System.out.printf("Acks:        %d (%.1f%%)%n", acks, 100.0 * acks / sent);
        System.out.printf("Fills:       %d%n", fills);
        System.out.printf("RTT p50:     %.1f µs%n", rttHist.getValueAtPercentile(50) / 1_000.0);
        System.out.printf("RTT p99:     %.1f µs%n", rttHist.getValueAtPercentile(99) / 1_000.0);
        System.out.printf("RTT p999:    %.1f µs%n", rttHist.getValueAtPercentile(99.9) / 1_000.0);
        System.out.printf("RTT max:     %.1f µs%n", rttHist.getMaxValue() / 1_000.0);
    }

    private void sendLogon(OutputStream out, int sessionId, long clientId) throws IOException {
        UnsafeBuffer buf = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        int len = Messages.encodeLogon(buf, 0, sessionId, clientId);
        byte[] bytes = new byte[len];
        buf.getBytes(0, bytes);
        out.write(bytes);
        out.flush();
    }

    /** Read one TCP frame (length-prefixed). Returns type+payload bytes (strips length field). */
    private byte[] readFrame(InputStream in) throws IOException {
        byte[] lenBytes = new byte[2];
        if (!readFully(in, lenBytes)) return null;
        int frameLen = ((lenBytes[1] & 0xFF) << 8) | (lenBytes[0] & 0xFF); // LE
        // frameLen includes the header; payload = frameLen - 2 (length field not counted separately here)
        // Our frame: [2 len][1 type][payload]; frameLen = 1+payloadLen when we strip the 2-byte len field
        byte[] body = new byte[frameLen];
        if (!readFully(in, body)) return null;
        return body;
    }

    private boolean readFully(InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int n = in.read(buf, offset, buf.length - offset);
            if (n < 0) return false;
            offset += n;
        }
        return true;
    }
}
