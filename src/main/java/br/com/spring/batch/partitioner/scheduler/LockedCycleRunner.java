package br.com.spring.batch.partitioner.scheduler;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import net.javacrumbs.shedlock.core.LockAssert;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("partitioner")
public class LockedCycleRunner {

    private static final StructuredLogger log = StructuredLogger.of(LockedCycleRunner.class, "scheduler");

    private final String owner;

    public LockedCycleRunner(ShedLockProperties properties) {
        this.owner = properties.lockedBy();
    }

    public void run(String scheduler, String requestPrefix, Runnable cycle) {
        RequestContext.run(RequestContext.newRequestId(requestPrefix), () -> runLocked(scheduler, cycle));
    }

    private void runLocked(String scheduler, Runnable cycle) {
        LockAssert.assertLocked();
        long start = System.currentTimeMillis();
        log.info("lock.acquired").field("scheduler", scheduler).field("owner", owner)
                .log("lock obtido; executando ciclo");
        try {
            cycle.run();
        } catch (RuntimeException e) {
            log.error("cycle.run").field("scheduler", scheduler).field("owner", owner).error(e)
                    .log("falha no ciclo agendado");
        } finally {
            log.info("cycle.end").field("scheduler", scheduler).field("owner", owner)
                    .field("durationMs", System.currentTimeMillis() - start).log("ciclo encerrado; liberando lock");
        }
    }
}
