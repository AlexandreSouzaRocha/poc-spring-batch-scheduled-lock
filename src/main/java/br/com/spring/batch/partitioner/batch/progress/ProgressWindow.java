package br.com.spring.batch.partitioner.batch.progress;

import java.util.concurrent.atomic.AtomicLong;

public final class ProgressWindow {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final long intervalNanos;
    private final AtomicLong nextReportNanos;

    private ProgressWindow(long intervalNanos, long startNanos) {
        this.intervalNanos = intervalNanos;
        this.nextReportNanos = new AtomicLong(startNanos + intervalNanos);
    }

    public static ProgressWindow ofSeconds(int seconds) {
        return new ProgressWindow(seconds * NANOS_PER_SECOND, System.nanoTime());
    }

    public boolean isDisabled() {
        return intervalNanos <= 0;
    }

    public boolean isDue(long nowNanos) {
        long scheduled = nextReportNanos.get();
        if (isDisabled() || nowNanos < scheduled) {
            return false;
        }
        return nextReportNanos.compareAndSet(scheduled, nowNanos + intervalNanos);
    }
}
