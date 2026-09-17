package br.com.spring.batch.partitioner.support.chaos;

import java.time.Duration;

import br.com.spring.batch.partitioner.support.log.StructuredLogger;

public enum ChaosAction {

    FAIL {
        @Override
        void apply(ChaosRule rule, ChaosTarget target) {
            LOG.warn("chaos.fail").field("point", target.point()).field("fileId", target.fileId())
                    .field("attempt", target.attempt()).field("partitionIndex", target.partitionIndex())
                    .log("falha injetada");
            throw new SimulatedFailureException("falha simulada em " + target.describe());
        }
    },

    DELAY {
        @Override
        void apply(ChaosRule rule, ChaosTarget target) {
            LOG.warn("chaos.delay").field("point", target.point()).field("fileId", target.fileId())
                    .field("attempt", target.attempt()).field("partitionIndex", target.partitionIndex())
                    .field("delaySeconds", rule.delaySeconds()).log("atraso injetado");
            sleep(Duration.ofSeconds(rule.delaySeconds()));
        }
    };

    private static final StructuredLogger LOG = StructuredLogger.of(ChaosAction.class, "chaos");

    abstract void apply(ChaosRule rule, ChaosTarget target);

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
