package br.com.spring.batch.partitioner.model;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileHeaderTest {

    private static final FileLayout LAYOUT = FileLayout.standard(LineSeparator.LF);

    @ParameterizedTest
    @EnumSource(MovementType.class)
    void parsesEveryMovementTypeWithPadding(MovementType type) {
        FileHeader header = FileHeader.of(LocalDate.of(2026, 9, 16), type, LAYOUT);

        FileHeader parsed = FileHeader.parse(header.lineBytes(LAYOUT), LAYOUT);

        assertThat(parsed).isEqualTo(header);
        assertThat(header.text()).hasSize(LAYOUT.headerLength());
        assertThat(header.lineBytes(LAYOUT)).hasSize(LAYOUT.headerLineBytes());
    }

    @Test
    void formatsHeaderPositionally() {
        FileHeader header = FileHeader.of(LocalDate.of(2026, 1, 2), MovementType.SALDO, LAYOUT);

        assertThat(header.text()).isEqualTo("H2026-01-02SALDO  ");
    }

    @Test
    void keepsFillerOfLongerHeaderAsReceived() {
        FileLayout layout = new FileLayout(LineSeparator.LF, 25, 150, 26);
        byte[] line = "H2026-01-02SALDO  XYZ1234\n".getBytes(StandardCharsets.US_ASCII);

        FileHeader parsed = FileHeader.parse(line, layout);

        assertThat(parsed.movementType()).isEqualTo(MovementType.SALDO);
        assertThat(parsed.text()).isEqualTo("H2026-01-02SALDO  XYZ1234");
    }

    @ParameterizedTest
    @ValueSource(strings = { "D2026-09-16ABERTO ", "T2026-09-16ABERTO ", "H2026-02-30ABERTO ", "H2026-09-16OUTRO  ",
            "H2026-09-16" })
    void rejectsInvalidHeaders(String header) {
        byte[] bytes = header.getBytes(StandardCharsets.US_ASCII);

        assertThatThrownBy(() -> FileHeader.parse(bytes, LAYOUT)).isInstanceOf(InvalidFileException.class);
    }

    @Test
    void rejectsHeaderLongerThanLayout() {
        byte[] bytes = "H2026-09-16ABERTO X\n".getBytes(StandardCharsets.US_ASCII);

        assertThatThrownBy(() -> FileHeader.parse(bytes, LAYOUT)).isInstanceOf(InvalidFileException.class);
    }
}
