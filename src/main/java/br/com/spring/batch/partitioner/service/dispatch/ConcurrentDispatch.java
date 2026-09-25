package br.com.spring.batch.partitioner.service.dispatch;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.service.InboxFiles;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class ConcurrentDispatch implements InboxDispatch {

    private final FilePartitionLauncher launcher;
    private final ProcessingLock processingLock;
    private final SchedulerProperties scheduler;
    private final int maxConcurrentTypes;

    public ConcurrentDispatch(FilePartitionLauncher launcher, ProcessingLock processingLock,
            SchedulerProperties scheduler, int maxConcurrentTypes) {
        this.launcher = launcher;
        this.processingLock = processingLock;
        this.scheduler = scheduler;
        this.maxConcurrentTypes = maxConcurrentTypes;
    }

    @Override
    public void dispatch(InboxFiles inbox) {
        Semaphore permits = new Semaphore(maxConcurrentTypes);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            inbox.byMovementGroup().forEach((group, files) ->
                    executor.submit(RequestContext.propagate(() -> launchGroup(group, files, permits))));
        }
    }

    private void launchGroup(String group, List<BlobFile> files, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            processingLock.tryRun(scheduler.lockNameFor(group), () -> files.forEach(launcher::launch));
        } finally {
            permits.release();
        }
    }
}
