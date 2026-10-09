package br.com.spring.batch.partitioner.support.chaos;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("chaos")
public class FailureInjector {

    private static final StructuredLogger log = StructuredLogger.of(FailureInjector.class, "chaos");

    private final AtomicReference<ChaosRule> activeRule = new AtomicReference<>();

    public void activate(ChaosRule rule) {
        activeRule.set(rule);
        log.warn("chaos.activate").field("point", rule.point()).field("action", rule.action())
                .field("onAttempt", rule.onAttempt()).field("partitionIndex", rule.partitionIndex())
                .field("delaySeconds", rule.delaySeconds()).log("regra de falha ativada");
    }

    public void deactivate() {
        activeRule.set(null);
        log.info("chaos.deactivate").log("regra de falha removida");
    }

    public Optional<ChaosRule> activeRule() {
        return Optional.ofNullable(activeRule.get());
    }

    public void check(ChaosPoint point, String fileId, int attempt) {
        apply(point, fileId, attempt, null);
    }

    public void checkPartition(String fileId, int attempt, int partitionIndex) {
        apply(ChaosPoint.PARTITION, fileId, attempt, partitionIndex);
    }

    private void apply(ChaosPoint point, String fileId, int attempt, Integer partitionIndex) {
        ChaosRule rule = activeRule.get();
        if (rule == null || !rule.matches(point, attempt, partitionIndex)) {
            return;
        }
        switch (rule.action()) {
            case FAIL -> {
                log.warn("chaos.fail").field("point", point).field("fileId", fileId).field("attempt", attempt)
                        .field("partitionIndex", partitionIndex).log("falha injetada");
                throw new SimulatedFailureException("falha simulada em " + point + " (fileId=" + fileId
                        + ", attempt=" + attempt + ", partition=" + partitionIndex + ")");
            }
            case DELAY -> {
                log.warn("chaos.delay").field("point", point).field("fileId", fileId).field("attempt", attempt)
                        .field("partitionIndex", partitionIndex).field("delaySeconds", rule.delaySeconds())
                        .log("atraso injetado");
                sleep(Duration.ofSeconds(rule.delaySeconds()));
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
