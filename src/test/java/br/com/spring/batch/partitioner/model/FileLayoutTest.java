package br.com.spring.batch.partitioner.model;

import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileLayoutTest {

    private static final FileLayout LF = FileLayout.standard(LineSeparator.LF);
    private static final FileLayout CRLF = FileLayout.standard(LineSeparator.CRLF);

    @Test
    @DisplayName("CRLF acrescenta um byte por linha no header, no detalhe e no trailer")
    void countsTheExtraByteOfCrlf() {
        assertThat(LF.headerLineBytes()).isEqualTo(19);
        assertThat(LF.detailLineBytes()).isEqualTo(151);
        assertThat(LF.trailerLineBytes()).isEqualTo(27);
        assertThat(CRLF.headerLineBytes()).isEqualTo(20);
        assertThat(CRLF.detailLineBytes()).isEqualTo(152);
        assertThat(CRLF.trailerLineBytes()).isEqualTo(28);
    }

    @Test
    @DisplayName("conta as linhas de detalhe descontando header e trailer, com ou sem quebra final")
    void countsDetailLinesWithOptionalFinalSeparator() {
        assertThat(LF.detailLineCount(19 + 151 * 1000 + 26)).isEqualTo(1000);
        assertThat(LF.detailLineCount(19 + 151 * 1000 + 27)).isEqualTo(1000);
        assertThat(CRLF.detailLineCount(20 + 152 * 1000 + 26)).isEqualTo(1000);
        assertThat(CRLF.detailLineCount(20 + 152 * 1000 + 28)).isEqualTo(1000);
    }

    @Test
    @DisplayName("usa os tamanhos configurados de header, detalhe e trailer")
    void usesConfiguredLengths() {
        FileLayout layout = new FileLayout(LineSeparator.LF, 30, 200, 40);

        assertThat(layout.headerRange()).isEqualTo(new ByteRange(0, 31));
        assertThat(layout.detailLineCount(31 + 201 * 5 + 41)).isEqualTo(5);
        assertThat(layout.trailerRange(5)).isEqualTo(new ByteRange(31 + 201 * 5, 31 + 201 * 5 + 40));
    }

    @Test
    @DisplayName("localiza o trailer logo depois da última linha de detalhe")
    void locatesTrailerAfterLastDetailLine() {
        assertThat(LF.trailerRange(1000)).isEqualTo(new ByteRange(19 + 151 * 1000, 19 + 151 * 1000 + 26));
    }

    @Test
    @DisplayName("calcula o offset da linha somando o separador de cada linha anterior")
    void calculatesByteOffset() {
        assertThat(LF.byteOffsetOfLine(0)).isEqualTo(19);
        assertThat(LF.byteOffsetOfLine(10)).isEqualTo(19 + 151 * 10);
        assertThat(CRLF.byteOffsetOfLine(10)).isEqualTo(20 + 152 * 10);
    }

    @Test
    @DisplayName("rejeita arquivo cujo tamanho não fecha com o separador configurado")
    void rejectsSizeFromAnotherSeparator() {
        long crlfFile = 20 + 152 * 1000 + 28;

        assertThatThrownBy(() -> LF.detailLineCount(crlfFile))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("incompatível com o layout LF");
    }

    @Test
    @DisplayName("rejeita arquivo sem linhas de detalhe")
    void rejectsFileWithoutDetailLines() {
        assertThatThrownBy(() -> LF.detailLineCount(19 + 27))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("sem linhas de detalhe");
    }

    @Test
    @DisplayName("não aceita header ou trailer menores que os campos obrigatórios")
    void rejectsLengthsBelowMandatoryFields() {
        assertThatThrownBy(() -> new FileLayout(LineSeparator.LF, 17, 150, 26))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("header");
        assertThatThrownBy(() -> new FileLayout(LineSeparator.LF, 18, 150, 25))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("trailer");
        assertThatThrownBy(() -> new FileLayout(LineSeparator.LF, 18, 0, 26))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("detalhe");
    }
}
