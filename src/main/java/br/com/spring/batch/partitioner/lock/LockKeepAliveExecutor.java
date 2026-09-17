package br.com.spring.batch.partitioner.lock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LockKeepAliveExecutor implements AutoCloseable {

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("shedlock-keep-alive").factory());

    public ScheduledExecutorService executor() {
        return executor;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
