package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.service.BlobPollingService;
import br.com.spring.batch.partitioner.service.PartitionCycleService;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class FileProcessingScheduler {

    private static final String SCHEDULER = "file-processing";

    private final CycleRunner cycleRunner;
    private final BlobPollingService pollingService;
    private final PartitionCycleService cycleService;
    private final ProcessingLock processingLock;
    private final SchedulerProperties properties;

    public FileProcessingScheduler(CycleRunner cycleRunner, BlobPollingService pollingService,
                                   PartitionCycleService cycleService, ProcessingLock processingLock,
                                   SchedulerProperties properties) {
        this.cycleRunner = cycleRunner;
        this.pollingService = pollingService;
        this.cycleService = cycleService;
        this.processingLock = processingLock;
        this.properties = properties;
    }

    @Scheduled(initialDelayString = "${app.scheduler.file-processing.initial-delay}",
            fixedDelayString = "${app.scheduler.file-processing.interval}")
    public void processFiles() {
        cycleRunner.run(SCHEDULER, "cycle", this::pollAndPartition);
    }

    private void pollAndPartition() {
        processingLock.tryRun(properties.pollLockName(), pollingService::poll);
        cycleService.processPendingFiles();
    }
}
