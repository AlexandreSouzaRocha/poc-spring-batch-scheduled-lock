package br.com.spring.batch.partitioner.support.chaos;

import java.util.Objects;

public record ChaosRule(ChaosPoint point, ChaosAction action, int onAttempt, Integer partitionIndex,
                        int delaySeconds) {

    private static final int ANY_ATTEMPT = 0;

    public boolean matches(ChaosTarget target) {
        return point == target.point() && attemptMatches(target) && partitionMatches(target);
    }

    public void applyTo(ChaosTarget target) {
        action.apply(this, target);
    }

    private boolean attemptMatches(ChaosTarget target) {
        return onAttempt == ANY_ATTEMPT || onAttempt == target.attempt();
    }

    private boolean partitionMatches(ChaosTarget target) {
        return partitionIndex == null || Objects.equals(partitionIndex, target.partitionIndex());
    }
}
