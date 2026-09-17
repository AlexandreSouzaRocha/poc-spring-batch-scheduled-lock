package br.com.spring.batch.partitioner.support;

import br.com.spring.batch.partitioner.support.chaos.ChaosAction;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.chaos.ChaosRule;
import br.com.spring.batch.partitioner.support.chaos.ChaosTarget;
import br.com.spring.batch.partitioner.support.chaos.FailureInjector;
import br.com.spring.batch.partitioner.support.chaos.SimulatedFailureException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChaosRuleTest {

    private final FailureInjector injector = new FailureInjector();

    @Test
    void failsOnlyOnConfiguredAttemptAndPartition() {
        injector.activate(new ChaosRule(ChaosPoint.PARTITION, ChaosAction.FAIL, 1, 3, 0));

        assertThatThrownBy(() -> injector.check(ChaosTarget.partition("file", 1, 3)))
                .isInstanceOf(SimulatedFailureException.class);
        assertThatCode(() -> injector.check(ChaosTarget.partition("file", 2, 3))).doesNotThrowAnyException();
        assertThatCode(() -> injector.check(ChaosTarget.partition("file", 1, 4))).doesNotThrowAnyException();
        assertThatCode(() -> injector.check(ChaosTarget.file(ChaosPoint.PUBLISH, "file", 1)))
                .doesNotThrowAnyException();
    }

    @Test
    void anyAttemptMatchesEveryAttempt() {
        ChaosRule rule = new ChaosRule(ChaosPoint.PUBLISH, ChaosAction.FAIL, 0, null, 0);

        assertThat(rule.matches(ChaosTarget.file(ChaosPoint.PUBLISH, "file", 7))).isTrue();
    }

    @Test
    void deactivatedInjectorDoesNothing() {
        injector.activate(new ChaosRule(ChaosPoint.MOVE, ChaosAction.FAIL, 0, null, 0));
        injector.deactivate();

        assertThatCode(() -> injector.check(ChaosTarget.file(ChaosPoint.MOVE, "file", 1))).doesNotThrowAnyException();
        assertThat(injector.activeRule()).isEmpty();
    }
}
