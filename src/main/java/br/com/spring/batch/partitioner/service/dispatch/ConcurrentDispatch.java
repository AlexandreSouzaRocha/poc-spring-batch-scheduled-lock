package br.com.spring.batch.partitioner.service.dispatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class ConcurrentDispatch implements QueueDispatch {

    private final FilePartitionLauncher launcher;
    private final ProcessingLock processingLock;
    private final SchedulerProperties scheduler;
    private final PartitionSettings settings;

    public ConcurrentDispatch(FilePartitionLauncher launcher, ProcessingLock processingLock,
            SchedulerProperties scheduler, PartitionSettings settings) {
        this.launcher = launcher;
        this.processingLock = processingLock;
        this.scheduler = scheduler;
        this.settings = settings;
    }

    @Override
    public void dispatch(ProcessingQueue queue) {
        Map<String, List<ReceivedFileDocument>> groups = queue.byMovementGroup();
        Semaphore permits = new Semaphore(settings.maxConcurrentTypes());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            groups.forEach((group, files) ->
                    executor.submit(RequestContext.propagate(() -> launchGroup(group, files, permits))));
        }
    }

    private void launchGroup(String group, List<ReceivedFileDocument> files, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            processingLock.tryRun(scheduler.lockNameFor(group), () -> files.forEach(this::launch));
        } finally {
            permits.release();
        }
    }

    private void launch(ReceivedFileDocument file) {
        RequestContext.run(RequestContext.childRequestId(file.id()), () -> launcher.launch(file));
    }
}
