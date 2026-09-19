package br.com.spring.batch.partitioner.lock;

import java.time.Instant;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockingTaskExecutor.TaskResult;

import org.springframework.stereotype.Component;

@Component
public class ProcessingLock {

    private static final StructuredLogger log = StructuredLogger.of(ProcessingLock.class, "scheduler");

    private final LockingTaskExecutor executor;
    private final SchedulerProperties scheduler;
    private final String owner;

    public ProcessingLock(LockingTaskExecutor executor, SchedulerProperties scheduler, ShedLockProperties shedLock) {
        this.executor = executor;
        this.scheduler = scheduler;
        this.owner = shedLock.lockedBy();
    }

    public boolean tryRun(String lockName, Runnable action) {
        long start = System.currentTimeMillis();
        try {
            return reportOutcome(lockName,
                    executor.executeWithLock(() -> run(lockName, action), configurationOf(lockName)), start);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("falha ao executar sob o lock " + lockName, e);
        }
    }

    private Boolean run(String lockName, Runnable action) {
        log.info("lock.acquired").field("lock", lockName).field("owner", owner).log("lock obtido");
        action.run();
        return Boolean.TRUE;
    }

    private boolean reportOutcome(String lockName, TaskResult<Boolean> result, long start) {
        if (!result.wasExecuted()) {
            log.info("lock.busy").field("lock", lockName).field("owner", owner)
                    .log("lock em uso por outra instância; pulando");
            return false;
        }
        log.info("lock.released").field("lock", lockName).field("owner", owner)
                .field("durationMs", System.currentTimeMillis() - start).log("lock liberado");
        return true;
    }

    private LockConfiguration configurationOf(String lockName) {
        return new LockConfiguration(Instant.now(), lockName, scheduler.lockAtMostFor(), scheduler.lockAtLeastFor());
    }
}
