package br.com.spring.batch.partitioner.service.dispatch;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class SequentialDispatch implements QueueDispatch {

    private final FilePartitionLauncher launcher;
    private final ProcessingLock processingLock;
    private final SchedulerProperties scheduler;

    public SequentialDispatch(FilePartitionLauncher launcher, ProcessingLock processingLock,
            SchedulerProperties scheduler) {
        this.launcher = launcher;
        this.processingLock = processingLock;
        this.scheduler = scheduler;
    }

    @Override
    public void dispatch(ProcessingQueue queue) {
        processingLock.tryRun(scheduler.lockName(), () -> queue.files().forEach(this::launch));
    }

    private void launch(ReceivedFileDocument file) {
        RequestContext.run(RequestContext.childRequestId(file.id()), () -> launcher.launch(file));
    }
}
