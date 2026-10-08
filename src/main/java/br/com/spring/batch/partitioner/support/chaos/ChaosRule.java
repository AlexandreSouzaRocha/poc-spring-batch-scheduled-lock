package br.com.spring.batch.partitioner.support.chaos;

import java.util.Objects;

public record ChaosRule(ChaosPoint point, ChaosAction action, int onAttempt, Integer partitionIndex,
                        int delaySeconds) {

    private static final int ANY_ATTEMPT = 0;

    public boolean matches(ChaosPoint point, int attempt, Integer partitionIndex) {
        boolean attemptMatches = onAttempt == ANY_ATTEMPT || onAttempt == attempt;
        boolean partitionMatches = this.partitionIndex == null || Objects.equals(this.partitionIndex, partitionIndex);
        return this.point == point && attemptMatches && partitionMatches;
    }
}
