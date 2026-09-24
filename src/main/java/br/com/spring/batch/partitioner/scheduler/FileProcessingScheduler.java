package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.service.PartitionCycleService;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class FileProcessingScheduler {

    private static final String SCHEDULER = "file-processing";

    private final CycleRunner cycleRunner;
    private final PartitionCycleService cycleService;

    public FileProcessingScheduler(CycleRunner cycleRunner, PartitionCycleService cycleService) {
        this.cycleRunner = cycleRunner;
        this.cycleService = cycleService;
    }

    @Scheduled(initialDelayString = "${app.scheduler.file-processing.initial-delay}",
            fixedDelayString = "${app.scheduler.file-processing.interval}")
    public void processFiles() {
        cycleRunner.run(SCHEDULER, "cycle", cycleService::processInbox);
    }
}
