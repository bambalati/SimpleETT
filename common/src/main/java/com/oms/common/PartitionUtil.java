package com.oms.common;

public final class PartitionUtil {
    private PartitionUtil() {}

    public static int partition(int instrumentId, int numPartitions) {
        return Math.abs(instrumentId % numPartitions);
    }

    public static int inboundStream(int partition, OmsConfig cfg) {
        return cfg.inboundStreamBase + partition;
    }

    public static int outboundStream(int partition, OmsConfig cfg) {
        return cfg.outboundStreamBase + partition;
    }
}
