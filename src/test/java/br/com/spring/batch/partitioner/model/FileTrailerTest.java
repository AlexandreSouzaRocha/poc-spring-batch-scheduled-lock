package br.com.spring.batch.partitioner.model;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileTrailerTest {

    private static final FileLayout LAYOUT = FileLayout.standard(LineSeparator.LF);

    @Test
    void formatsQuantityWithFifteenDigitsAndProcessingDate() {
        FileTrailer trailer = FileTrailer.of(1_000_000, LocalDate.of(2026, 10, 9), LAYOUT);

        assertThat(trailer.text()).isEqualTo("T0000000010000002026-10-09");
        assertThat(trailer.lineBytes(LAYOUT)).hasSize(LAYOUT.trailerLineBytes());
    }

    @Test
    void parsesTrailerWithAndWithoutFinalSeparator() {
        FileTrailer expected = FileTrailer.of(42, LocalDate.of(2026, 10, 9), LAYOUT);

        assertThat(FileTrailer.parse(ascii("T0000000000000422026-10-09"), LAYOUT)).isEqualTo(expected);
        assertThat(FileTrailer.parse(ascii("T0000000000000422026-10-09\n"), LAYOUT)).isEqualTo(expected);
    }

    @Test
    void padsLongerTrailerWithSpaces() {
        FileLayout layout = new FileLayout(LineSeparator.LF, 18, 150, 30);

        FileTrailer trailer = FileTrailer.of(7, LocalDate.of(2026, 10, 9), layout);

        assertThat(trailer.text()).isEqualTo("T0000000000000072026-10-09    ");
        assertThat(FileTrailer.parse(trailer.lineBytes(layout), layout)).isEqualTo(trailer);
    }

    @ParameterizedTest
    @ValueSource(strings = { "D0000000000000422026-10-09", "H0000000000000422026-10-09", "T00000000000004A2026-10-09",
            "T0000000000000422026-13-09", "T000000000000042" })
    void rejectsInvalidTrailers(String trailer) {
        assertThatThrownBy(() -> FileTrailer.parse(ascii(trailer), LAYOUT)).isInstanceOf(InvalidFileException.class);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }
}
