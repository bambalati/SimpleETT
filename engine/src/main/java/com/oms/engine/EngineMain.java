package com.oms.engine;

import com.oms.common.OmsConfig;
import com.oms.engine.partition.EnginePartition;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for the matching engine process.
 *
 * Usage:
 *   java -jar engine-fat.jar [config-path]
 *
 * Starts an embedded Aeron MediaDriver (shared with gateway when running locally)
 * and one EnginePartition thread per configured partition.
 */
public final class EngineMain {

    private static final Logger log = LoggerFactory.getLogger(EngineMain.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : null;
        OmsConfig cfg = OmsConfig.load(configPath);

        log.info("Starting OMS Engine: partitions={} channel={}", cfg.partitions, cfg.aeronChannel);

        // Embedded Aeron driver — suitable for local dev.
        // For multi-host: start a separate aeron-driver process and use external context.
        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(cfg.aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launch(driverCtx)) {
            Aeron.Context aeronCtx = new Aeron.Context()
                    .aeronDirectoryName(cfg.aeronDir);

            try (Aeron aeron = Aeron.connect(aeronCtx)) {
                List<EnginePartition> partitions = new ArrayList<>();
                for (int i = 0; i < cfg.partitions; i++) {
                    EnginePartition p = new EnginePartition(i, cfg, aeron);
                    partitions.add(p);
                    p.start();
                }

                log.info("All {} partitions started. Waiting for shutdown signal.", cfg.partitions);
                new ShutdownSignalBarrier().await();

                partitions.forEach(EnginePartition::stop);
                log.info("Engine shutdown complete.");
            }
        }
    }
}
