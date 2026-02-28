package com.oms.common;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Central configuration loaded from oms.yml (or classpath default).
 * All fields have sensible defaults for localhost development.
 */
public final class OmsConfig {

    // Aeron
    public String aeronDir = "/dev/shm/aeron-oms";
    public String aeronChannel = "aeron:udp?endpoint=localhost:20121";
    public int partitions = 32;
    public int inboundStreamBase = 1000;   // stream = base + partition
    public int outboundStreamBase = 2000;

    // Gateway
    public int gatewayPort = 7001;
    public int gatewayBackpressureQueueLimit = 4096;

    // Engine
    public boolean runGateway = true;
    public boolean runEngine = true;

    // Metrics
    public int metricsIntervalSecs = 5;

    @SuppressWarnings("unchecked")
    public static OmsConfig load(String path) {
        OmsConfig cfg = new OmsConfig();
        try {
            InputStream is = path != null && Files.exists(Paths.get(path))
                    ? Files.newInputStream(Paths.get(path))
                    : OmsConfig.class.getResourceAsStream("/oms.yml");
            if (is == null) return cfg;
            Map<String, Object> map = new Yaml().load(is);
            if (map == null) return cfg;
            applyMap(cfg, map);
        } catch (Exception e) {
            // log and proceed with defaults
            System.err.println("[config] failed to load config, using defaults: " + e.getMessage());
        }
        return cfg;
    }

    @SuppressWarnings("unchecked")
    private static void applyMap(OmsConfig cfg, Map<String, Object> map) {
        if (map.containsKey("aeronDir")) cfg.aeronDir = (String) map.get("aeronDir");
        if (map.containsKey("aeronChannel")) cfg.aeronChannel = (String) map.get("aeronChannel");
        if (map.containsKey("partitions")) cfg.partitions = (int) map.get("partitions");
        if (map.containsKey("inboundStreamBase")) cfg.inboundStreamBase = (int) map.get("inboundStreamBase");
        if (map.containsKey("outboundStreamBase")) cfg.outboundStreamBase = (int) map.get("outboundStreamBase");
        if (map.containsKey("gatewayPort")) cfg.gatewayPort = (int) map.get("gatewayPort");
        if (map.containsKey("gatewayBackpressureQueueLimit")) cfg.gatewayBackpressureQueueLimit = (int) map.get("gatewayBackpressureQueueLimit");
        if (map.containsKey("runGateway")) cfg.runGateway = (boolean) map.get("runGateway");
        if (map.containsKey("runEngine")) cfg.runEngine = (boolean) map.get("runEngine");
        if (map.containsKey("metricsIntervalSecs")) cfg.metricsIntervalSecs = (int) map.get("metricsIntervalSecs");
    }
}
