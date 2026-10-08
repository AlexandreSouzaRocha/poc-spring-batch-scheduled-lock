package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.service.PartitionCycleService;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class FileProcessingScheduler {

    private static final StructuredLogger log = StructuredLogger.of(FileProcessingScheduler.class, "scheduler");
    private static final String SCHEDULER = "file-processing";
    private static final String REQUEST_PREFIX = "cycle";

    private final PartitionCycleService cycleService;
    private final String owner;

    public FileProcessingScheduler(PartitionCycleService cycleService, ShedLockProperties shedLock) {
        this.cycleService = cycleService;
        this.owner = shedLock.lockedBy();
    }

    @Scheduled(initialDelayString = "${app.scheduler.file-processing.initial-delay}",
            fixedDelayString = "${app.scheduler.file-processing.interval}")
    public void processFiles() {
        RequestContext.run(RequestContext.newRequestId(REQUEST_PREFIX), this::runCycle);
    }

    private void runCycle() {
        long start = System.currentTimeMillis();
        log.info("cycle.start").field("scheduler", SCHEDULER).field("owner", owner).log("ciclo iniciado");
        try {
            cycleService.processInbox();
        } catch (RuntimeException e) {
            log.error("cycle.run").field("scheduler", SCHEDULER).field("owner", owner).error(e)
                    .log("falha no ciclo agendado");
        } finally {
            log.info("cycle.end").field("scheduler", SCHEDULER).field("owner", owner)
                    .field("durationMs", System.currentTimeMillis() - start).log("ciclo encerrado");
        }
    }
}
