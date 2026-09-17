package br.com.spring.batch.partitioner.support.chaos;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Component;

@Component
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

    public void check(ChaosTarget target) {
        activeRule()
                .filter(rule -> rule.matches(target))
                .ifPresent(rule -> rule.applyTo(target));
    }
}
