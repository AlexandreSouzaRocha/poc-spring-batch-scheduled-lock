package br.com.spring.batch.partitioner.model;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileHeaderTest {

    @ParameterizedTest
    @EnumSource(MovementType.class)
    void parsesEveryMovementTypeWithPadding(MovementType type) {
        FileHeader header = new FileHeader(LocalDate.of(2026, 9, 16), type);

        FileHeader parsed = FileHeader.parse(header.lineBytes());

        assertThat(parsed).isEqualTo(header);
        assertThat(header.text()).hasSize(FileLayout.HEADER_LENGTH);
        assertThat(header.lineBytes()).hasSize(FileLayout.HEADER_LINE_BYTES);
    }

    @Test
    void formatsHeaderPositionally() {
        FileHeader header = new FileHeader(LocalDate.of(2026, 1, 2), MovementType.SALDO);

        assertThat(header.text()).isEqualTo("H2026-01-02SALDO  ");
    }

    @ParameterizedTest
    @ValueSource(strings = { "D2026-09-16ABERTO ", "H2026-02-30ABERTO ", "H2026-09-16OUTRO  ", "H2026-09-16" })
    void rejectsInvalidHeaders(String header) {
        byte[] bytes = header.getBytes(StandardCharsets.US_ASCII);

        assertThatThrownBy(() -> FileHeader.parse(bytes)).isInstanceOf(InvalidFileException.class);
    }

    @Test
    void rejectsHeaderLongerThanLayout() {
        byte[] bytes = "H2026-09-16ABERTO X\n".getBytes(StandardCharsets.US_ASCII);

        assertThatThrownBy(() -> FileHeader.parse(bytes)).isInstanceOf(InvalidFileException.class);
    }
}
