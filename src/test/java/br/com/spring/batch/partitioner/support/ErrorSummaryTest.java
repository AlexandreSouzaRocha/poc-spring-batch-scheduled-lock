package br.com.spring.batch.partitioner.support;

import java.util.Map;

import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorSummaryTest {

    @Test
    void summarizesErrorWithRootCauseAndApplicationOrigin() {
        IllegalStateException error = new IllegalStateException("falha no job",
                new IllegalArgumentException("causa raiz"));

        Map<String, Object> summary = ErrorSummary.of(error);

        assertThat(summary)
                .containsEntry("type", IllegalStateException.class.getName())
                .containsEntry("message", "falha no job")
                .containsEntry("root_cause_type", IllegalArgumentException.class.getName())
                .containsEntry("root_cause_message", "causa raiz")
                .containsKey("at");
        assertThat(summary.get("at").toString()).startsWith(ErrorSummaryTest.class.getName());
        assertThat(ErrorSummary.oneLine(error)).isEqualTo("IllegalArgumentException: causa raiz");
    }
}
