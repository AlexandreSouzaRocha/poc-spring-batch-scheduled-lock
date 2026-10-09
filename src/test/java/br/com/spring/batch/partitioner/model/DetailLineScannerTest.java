package br.com.spring.batch.partitioner.model;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import br.com.spring.batch.partitioner.model.layout.DetailLineScanner;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DetailLineScannerTest {

    private static final FileLayout LF = new FileLayout(LineSeparator.LF, 18, 4, 26);
    private static final FileLayout CRLF = new FileLayout(LineSeparator.CRLF, 18, 4, 26);
    private static final long FIRST_DETAIL = LF.headerLineBytes();

    @Test
    @DisplayName("conta as linhas de detalhe que começam no bloco")
    void countsDetailLinesStartingInBlock() {
        byte[] block = ascii("D123\nD456\nD789\n");

        assertThat(new DetailLineScanner(LF).scan(block, block.length, FIRST_DETAIL)).isEqualTo(3);
    }

    @Test
    @DisplayName("confere linhas que atravessam blocos sem contar a mesma linha duas vezes")
    void handlesLinesCrossingBlocks() {
        DetailLineScanner scanner = new DetailLineScanner(LF);
        byte[] lines = ascii("D123\nD456\nD789\n");

        long first = scanner.scan(lines, 7, FIRST_DETAIL);
        long second = scanner.scan(Arrays.copyOfRange(lines, 7, 15), 8, FIRST_DETAIL + 7);

        assertThat(first + second).isEqualTo(3);
    }

    @Test
    @DisplayName("reprova header ou trailer no meio do arquivo")
    void rejectsHeaderOrTrailerInTheMiddle() {
        byte[] block = ascii("D123\nT456\n");

        assertThatThrownBy(() -> new DetailLineScanner(LF).scan(block, block.length, FIRST_DETAIL))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("linha 3")
                .hasMessageContaining("'T' (trailer)");
    }

    @Test
    @DisplayName("reprova indicador desconhecido")
    void rejectsUnknownIndicator() {
        byte[] block = ascii("X123\n");

        assertThatThrownBy(() -> new DetailLineScanner(LF).scan(block, block.length, FIRST_DETAIL))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("'X' (desconhecido)");
    }

    @Test
    @DisplayName("reprova linha sem a quebra na posição do layout")
    void rejectsLineWithoutSeparatorAtExpectedPosition() {
        byte[] block = ascii("D123XD456\n");

        assertThatThrownBy(() -> new DetailLineScanner(LF).scan(block, block.length, FIRST_DETAIL))
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("quebra de linha fora da posição");
    }

    @Test
    @DisplayName("confere os dois bytes da quebra CRLF")
    void checksBothBytesOfCrlf() {
        DetailLineScanner scanner = new DetailLineScanner(CRLF);
        long firstDetail = CRLF.headerLineBytes();

        assertThat(scanner.scan(ascii("D123\r\nD456\r\n"), 12, firstDetail)).isEqualTo(2);
        assertThatThrownBy(() -> scanner.scan(ascii("D123\n\nD456\r\n"), 12, firstDetail))
                .isInstanceOf(InvalidFileException.class);
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }
}
