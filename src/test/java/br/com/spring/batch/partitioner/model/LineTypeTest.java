package br.com.spring.batch.partitioner.model;

import br.com.spring.batch.partitioner.model.layout.LineType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LineTypeTest {

    @Test
    void recognizesEachLineByItsIndicator() {
        assertThat(LineType.of((byte) 'H')).contains(LineType.HEADER);
        assertThat(LineType.of((byte) 'D')).contains(LineType.DETAIL);
        assertThat(LineType.of((byte) 'T')).contains(LineType.TRAILER);
        assertThat(LineType.of((byte) 'X')).isEmpty();
    }

    @Test
    void describesKnownAndUnknownIndicators() {
        assertThat(LineType.describe((byte) 'T')).isEqualTo("'T' (trailer)");
        assertThat(LineType.describe((byte) 'X')).isEqualTo("'X' (desconhecido)");
    }
}
