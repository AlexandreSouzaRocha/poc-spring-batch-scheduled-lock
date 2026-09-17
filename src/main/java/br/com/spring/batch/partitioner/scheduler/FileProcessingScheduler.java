package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.service.BlobPollingService;
import br.com.spring.batch.partitioner.service.PartitionCycleService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class FileProcessingScheduler {

    private static final String SCHEDULER = "file-processing";

    private final LockedCycleRunner cycleRunner;
    private final BlobPollingService pollingService;
    private final PartitionCycleService cycleService;

    public FileProcessingScheduler(LockedCycleRunner cycleRunner, BlobPollingService pollingService,
                                   PartitionCycleService cycleService) {
        this.cycleRunner = cycleRunner;
        this.pollingService = pollingService;
        this.cycleService = cycleService;
    }

    @Scheduled(initialDelayString = "${app.scheduler.file-processing.initial-delay}",
            fixedDelayString = "${app.scheduler.file-processing.interval}")
    @SchedulerLock(name = "${app.scheduler.file-processing.lock-name}",
            lockAtMostFor = "${app.scheduler.file-processing.lock-at-most-for}",
            lockAtLeastFor = "${app.scheduler.file-processing.lock-at-least-for}")
    public void processFiles() {
        cycleRunner.run(SCHEDULER, "cycle", this::pollAndPartition);
    }

    private void pollAndPartition() {
        pollingService.poll();
        cycleService.processPendingFiles();
    }
}
