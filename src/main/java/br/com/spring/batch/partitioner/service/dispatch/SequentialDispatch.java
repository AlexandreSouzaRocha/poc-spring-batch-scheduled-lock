package br.com.spring.batch.partitioner.service.dispatch;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.service.InboxFiles;

public class SequentialDispatch implements InboxDispatch {

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
    public void dispatch(InboxFiles inbox) {
        processingLock.tryRun(scheduler.lockName(), () -> inbox.files().forEach(launcher::launch));
    }
}
