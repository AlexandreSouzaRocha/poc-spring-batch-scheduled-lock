package br.com.spring.batch.partitioner.model;

import br.com.spring.batch.partitioner.model.layout.DetailRecordBuilder;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileLayoutTest {

    @Test
    void countsDetailLinesFromFileSize() {
        long size = FileLayout.HEADER_LINE_BYTES + 1_000L * FileLayout.RECORD_LINE_BYTES;

        assertThat(FileLayout.detailLineCount(size)).isEqualTo(1_000L);
    }

    @Test
    void rejectsFileWithoutDetailLines() {
        assertThatThrownBy(() -> FileLayout.detailLineCount(FileLayout.HEADER_LINE_BYTES))
                .isInstanceOf(InvalidFileException.class);
    }

    @Test
    void rejectsFileWithPartialLine() {
        long size = FileLayout.HEADER_LINE_BYTES + 10L * FileLayout.RECORD_LINE_BYTES + 3;

        assertThatThrownBy(() -> FileLayout.detailLineCount(size)).isInstanceOf(InvalidFileException.class);
    }

    @Test
    void buildsFixedLengthDetailRecords() {
        byte[] line = new DetailRecordBuilder("2026-09-16").next(42);

        assertThat(line).hasSize(FileLayout.RECORD_LINE_BYTES);
        assertThat(line[0]).isEqualTo(FileLayout.DETAIL_INDICATOR);
        assertThat(line[FileLayout.RECORD_LENGTH]).isEqualTo(FileLayout.LINE_SEPARATOR);
        assertThat(new String(line, 15, 10)).isEqualTo("2026-09-16");
        assertThat(new String(line, 41, 12)).isEqualTo("000000000042");
    }
}
