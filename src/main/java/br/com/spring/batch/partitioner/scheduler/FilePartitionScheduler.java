package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.service.PartitionCycleService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class FilePartitionScheduler {

    private final LockedCycleRunner cycleRunner;
    private final PartitionCycleService cycleService;

    public FilePartitionScheduler(LockedCycleRunner cycleRunner, PartitionCycleService cycleService) {
        this.cycleRunner = cycleRunner;
        this.cycleService = cycleService;
    }

    @Scheduled(initialDelayString = "${app.scheduler.partitioning.initial-delay}",
            fixedDelayString = "${app.scheduler.partitioning.interval}")
    @SchedulerLock(name = "${app.scheduler.partitioning.lock-name}",
            lockAtMostFor = "${app.scheduler.partitioning.lock-at-most-for}",
            lockAtLeastFor = "${app.scheduler.partitioning.lock-at-least-for}")
    public void partition() {
        cycleRunner.run("file-partitioning", "part", cycleService::processPendingFiles);
    }
}
