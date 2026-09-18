package br.com.spring.batch.partitioner.batch.progress;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public final class ProgressCounter {

    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final AtomicLong copiedBytes = new AtomicLong();
    private final AtomicLong totalBytes;
    private final long startNanos = System.nanoTime();
    private final ProgressWindow window;
    private final Consumer<ProgressSnapshot> listener;
    private final LongConsumer downstream;

    ProgressCounter(long totalBytes, ProgressWindow window, Consumer<ProgressSnapshot> listener,
            LongConsumer downstream) {
        this.totalBytes = new AtomicLong(totalBytes);
        this.window = window;
        this.listener = listener;
        this.downstream = downstream;
    }

    public void advance(long bytes) {
        copiedBytes.addAndGet(bytes);
        downstream.accept(bytes);
        reportIfDue();
    }

    void addTotal(long bytes) {
        totalBytes.addAndGet(bytes);
    }

    ProgressSnapshot snapshot() {
        return new ProgressSnapshot(copiedBytes.get(), totalBytes.get(),
                (System.nanoTime() - startNanos) / NANOS_PER_MILLI);
    }

    private void reportIfDue() {
        if (!window.isDue(System.nanoTime())) {
            return;
        }
        listener.accept(snapshot());
    }
}
