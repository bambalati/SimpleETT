package com.oms.gateway;

import com.oms.common.OmsConfig;
import com.oms.gateway.aeron.AeronPublisher;
import com.oms.gateway.aeron.AeronSubscriber;
import com.oms.gateway.session.SessionRegistry;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Gateway process entry point.
 *
 * When runEngine=true (default dev config), also launches embedded engine partitions
 * so the whole system runs in-process on localhost.
 */
public final class GatewayMain {

    private static final Logger log = LoggerFactory.getLogger(GatewayMain.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        OmsConfig cfg = OmsConfig.load(configPath);

        log.info("Starting OMS Gateway: port={} partitions={}", cfg.gatewayPort, cfg.partitions);

        // Start embedded MediaDriver (shared when engine is in same process)
        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(cfg.aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launch(driverCtx)) {
            Aeron.Context aeronCtx = new Aeron.Context().aeronDirectoryName(cfg.aeronDir);

            try (Aeron aeron = Aeron.connect(aeronCtx)) {
                SessionRegistry sessions = new SessionRegistry();
                AeronPublisher publisher = new AeronPublisher(cfg, aeron);
                AtomicLong orderIdGen = new AtomicLong(1);

                // Start engine partitions if configured to run co-located
                if (cfg.runEngine) {
                    log.info("Starting co-located engine ({} partitions)", cfg.partitions);
                    for (int i = 0; i < cfg.partitions; i++) {
                        com.oms.engine.partition.EnginePartition ep =
                                new com.oms.engine.partition.EnginePartition(i, cfg, aeron);
                        ep.start();
                    }
                }

                // Start Aeron subscriber (outbound engine -> gateway)
                AeronSubscriber subscriber = new AeronSubscriber(cfg, aeron, sessions);
                subscriber.start();

                // Start TCP gateway
                GatewayServer server = new GatewayServer(cfg, sessions, publisher, orderIdGen);
                server.start();

                log.info("OMS is UP. Ctrl-C to stop.");
                new ShutdownSignalBarrier().await();

                server.stop();
                subscriber.stop();
                publisher.close();
            }
        }
    }
}
