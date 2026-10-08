package br.com.spring.batch.partitioner.batch.progress;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class ProgressCounter {

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final AtomicLong copiedBytes = new AtomicLong();
    private final AtomicLong totalBytes;
    private final long startNanos = System.nanoTime();
    private final long reportIntervalNanos;
    private final AtomicLong nextReportNanos;
    private final Consumer<ProgressSnapshot> reporter;
    private final ProgressCounter parent;

    ProgressCounter(long totalBytes, int reportIntervalSeconds, Consumer<ProgressSnapshot> reporter,
                    ProgressCounter parent) {
        this.totalBytes = new AtomicLong(totalBytes);
        this.reportIntervalNanos = reportIntervalSeconds * NANOS_PER_SECOND;
        this.nextReportNanos = new AtomicLong(startNanos + reportIntervalNanos);
        this.reporter = reporter;
        this.parent = parent;
    }

    public void advance(long bytes) {
        copiedBytes.addAndGet(bytes);
        if (parent != null) {
            parent.advance(bytes);
        }
        if (isReportDue()) {
            reporter.accept(snapshot());
        }
    }

    void addTotal(long bytes) {
        totalBytes.addAndGet(bytes);
    }

    ProgressSnapshot snapshot() {
        return new ProgressSnapshot(copiedBytes.get(), totalBytes.get(),
                (System.nanoTime() - startNanos) / NANOS_PER_MILLI);
    }

    private boolean isReportDue() {
        long now = System.nanoTime();
        long scheduled = nextReportNanos.get();
        return reportIntervalNanos > 0 && now >= scheduled
                && nextReportNanos.compareAndSet(scheduled, now + reportIntervalNanos);
    }
}
