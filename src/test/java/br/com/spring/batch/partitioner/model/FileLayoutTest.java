package br.com.spring.batch.partitioner.model;

import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileLayoutTest {

    private static final FileLayout LF = new FileLayout(LineSeparator.LF);
    private static final FileLayout CRLF = new FileLayout(LineSeparator.CRLF);

    @Test
    @DisplayName("CRLF acrescenta um byte por linha, no header e no detalhe")
    void countsTheExtraByteOfCrlf() {
        assertThat(LF.headerLineBytes()).isEqualTo(19);
        assertThat(LF.recordLineBytes()).isEqualTo(151);
        assertThat(CRLF.headerLineBytes()).isEqualTo(20);
        assertThat(CRLF.recordLineBytes()).isEqualTo(152);
    }

    @Test
    @DisplayName("conta as linhas de detalhe pelo tamanho do arquivo em cada separador")
    void countsDetailLines() {
        assertThat(LF.detailLineCount(19 + 151 * 1000)).isEqualTo(1000);
        assertThat(CRLF.detailLineCount(20 + 152 * 1000)).isEqualTo(1000);
    }

    @Test
    @DisplayName("calcula o offset da linha somando o separador de cada linha anterior")
    void calculatesByteOffset() {
        assertThat(LF.byteOffsetOfLine(0)).isEqualTo(19);
        assertThat(LF.byteOffsetOfLine(10)).isEqualTo(19 + 151 * 10);
        assertThat(CRLF.byteOffsetOfLine(0)).isEqualTo(20);
        assertThat(CRLF.byteOffsetOfLine(10)).isEqualTo(20 + 152 * 10);
    }

    @Test
    @DisplayName("rejeita arquivo cujo tamanho nao fecha com o separador configurado")
    void rejectsSizeFromAnotherSeparator() {
        long crlfFile = 20 + 152 * 1000;

        assertThatThrownBy(() -> LF.detailLineCount(crlfFile))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("incompatível com o layout LF");
    }

    @Test
    @DisplayName("rejeita arquivo truncado ou sem linhas de detalhe")
    void rejectsTruncatedFile() {
        assertThatThrownBy(() -> CRLF.detailLineCount(25))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("sem linhas de detalhe");
    }
}
