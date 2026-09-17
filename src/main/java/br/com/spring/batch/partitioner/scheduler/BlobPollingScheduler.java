package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.service.BlobPollingService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class BlobPollingScheduler {

    private final LockedCycleRunner cycleRunner;
    private final BlobPollingService pollingService;

    public BlobPollingScheduler(LockedCycleRunner cycleRunner, BlobPollingService pollingService) {
        this.cycleRunner = cycleRunner;
        this.pollingService = pollingService;
    }

    @Scheduled(initialDelayString = "${app.scheduler.blob-polling.initial-delay}",
            fixedDelayString = "${app.scheduler.blob-polling.interval}")
    @SchedulerLock(name = "${app.scheduler.blob-polling.lock-name}",
            lockAtMostFor = "${app.scheduler.blob-polling.lock-at-most-for}",
            lockAtLeastFor = "${app.scheduler.blob-polling.lock-at-least-for}")
    public void poll() {
        cycleRunner.run("blob-polling", "poll", pollingService::poll);
    }
}
