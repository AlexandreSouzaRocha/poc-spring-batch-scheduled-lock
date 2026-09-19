package br.com.spring.batch.partitioner.model;

import java.util.Arrays;
import java.util.Comparator;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MovementTypeTest {

    @Test
    @DisplayName("define a ordem de processamento FECHADO, ABERTO, ULTIMA e SALDO")
    void ordersTypesForProcessing() {
        assertThat(Arrays.stream(MovementType.values())
                .sorted(Comparator.comparingInt(MovementType::processingOrder))
                .toList())
                .containsExactly(MovementType.FECHADO, MovementType.ABERTO, MovementType.ULTIMA, MovementType.SALDO);
    }

    @Test
    @DisplayName("nao repete ordem entre tipos")
    void keepsOrderUnique() {
        assertThat(Arrays.stream(MovementType.values()).map(MovementType::processingOrder).distinct().count())
                .isEqualTo(MovementType.values().length);
    }
}
