package br.com.spring.batch.partitioner.service.dispatch;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;

public class TypeLockGuard implements GroupGuard {

    private final ProcessingLock processingLock;
    private final SchedulerProperties scheduler;

    public TypeLockGuard(ProcessingLock processingLock, SchedulerProperties scheduler) {
        this.processingLock = processingLock;
        this.scheduler = scheduler;
    }

    @Override
    public void run(String movementGroup, Runnable action) {
        processingLock.tryRun(scheduler.lockNameFor(movementGroup), action);
    }
}
