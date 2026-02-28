package com.oms.common;

import org.HdrHistogram.Histogram;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe latency tracking using HdrHistogram.
 * Record nanos; report percentiles periodically.
 */
public final class LatencyStats {

    private final Histogram histogram;
    private final LongAdder count = new LongAdder();
    private final String name;

    public LatencyStats(String name) {
        this.name = name;
        // max 10 seconds, 3 sig figs
        this.histogram = new Histogram(10_000_000_000L, 3);
    }

    public void record(long latencyNanos) {
        histogram.recordValue(latencyNanos);
        count.increment();
    }

    public void logAndReset() {
        long total = count.sumThenReset();
        if (total == 0) return;
        System.out.printf("[metrics] %s count=%d p50=%.1fµs p99=%.1fµs p999=%.1fµs max=%.1fµs%n",
                name, total,
                histogram.getValueAtPercentile(50) / 1_000.0,
                histogram.getValueAtPercentile(99) / 1_000.0,
                histogram.getValueAtPercentile(99.9) / 1_000.0,
                histogram.getMaxValue() / 1_000.0);
        histogram.reset();
    }
}
