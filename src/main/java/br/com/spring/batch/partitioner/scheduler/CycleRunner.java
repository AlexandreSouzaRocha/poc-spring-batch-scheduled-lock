package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class CycleRunner {

    private static final StructuredLogger log = StructuredLogger.of(CycleRunner.class, "scheduler");

    private final String owner;

    public CycleRunner(ShedLockProperties properties) {
        this.owner = properties.lockedBy();
    }

    public void run(String scheduler, String requestPrefix, Runnable cycle) {
        RequestContext.run(RequestContext.newRequestId(requestPrefix), () -> runCycle(scheduler, cycle));
    }

    private void runCycle(String scheduler, Runnable cycle) {
        long start = System.currentTimeMillis();
        log.info("cycle.start").field("scheduler", scheduler).field("owner", owner).log("ciclo iniciado");
        try {
            cycle.run();
        } catch (RuntimeException e) {
            log.error("cycle.run").field("scheduler", scheduler).field("owner", owner).error(e)
                    .log("falha no ciclo agendado");
        } finally {
            log.info("cycle.end").field("scheduler", scheduler).field("owner", owner)
                    .field("durationMs", System.currentTimeMillis() - start).log("ciclo encerrado");
        }
    }
}
