package br.com.spring.batch.partitioner.model;

import java.util.List;

import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionPlanTest {

    private static final FileLayout LAYOUT = new FileLayout(LineSeparator.LF);

    @Test
    void splitsLinesIntoContiguousRanges() {
        PartitionPlan plan = PartitionPlan.split(1_000_003, 10, LAYOUT);
        List<PartitionRange> ranges = plan.ranges();

        assertThat(plan.size()).isEqualTo(10);
        assertThat(plan.totalLines()).isEqualTo(1_000_003);
        assertThat(ranges).extracting(PartitionRange::lineCount).startsWith(100_001L, 100_001L, 100_001L, 100_000L);
        assertThat(ranges.getFirst().bytes().start()).isEqualTo(LAYOUT.headerLineBytes());
        assertThat(ranges.getLast().bytes().end())
                .isEqualTo(LAYOUT.headerLineBytes() + 1_000_003L * LAYOUT.recordLineBytes());
        for (int position = 1; position < ranges.size(); position++) {
            assertThat(ranges.get(position).bytes().start()).isEqualTo(ranges.get(position - 1).bytes().end());
        }
    }

    @Test
    void neverCreatesMorePartitionsThanLines() {
        PartitionPlan plan = PartitionPlan.split(3, 10, LAYOUT);

        assertThat(plan.size()).isEqualTo(3);
        assertThat(plan.ranges()).extracting(PartitionRange::lineCount).containsOnly(1L);
    }

    @Test
    @DisplayName("as faixas de bytes acompanham o separador configurado")
    void rangesFollowConfiguredSeparator() {
        FileLayout crlf = new FileLayout(LineSeparator.CRLF);

        PartitionRange primeira = PartitionPlan.split(1000, 2, crlf).ranges().getFirst();
        PartitionRange segunda = PartitionPlan.split(1000, 2, crlf).ranges().getLast();

        assertThat(primeira.bytes().start()).isEqualTo(crlf.headerLineBytes());
        assertThat(primeira.bytes().end()).isEqualTo(segunda.bytes().start());
        assertThat(segunda.bytes().end()).isEqualTo(crlf.headerLineBytes() + 1000L * crlf.recordLineBytes());
        assertThat(primeira.bytes().length()).isEqualTo(500L * crlf.recordLineBytes());
    }

    @Test
    void roundTripsThroughExecutionContext() {
        PartitionRange range = PartitionPlan.split(500, 4, LAYOUT).ranges().get(2);

        assertThat(PartitionRange.from(range.toExecutionContext())).isEqualTo(range);
        assertThat(range.fileSizeBytes(LAYOUT))
                .isEqualTo(LAYOUT.headerLineBytes() + range.lineCount() * LAYOUT.recordLineBytes());
        assertThat(range.stepName()).isEqualTo("partition0003");
    }

    @Test
    void rejectsEmptyFiles() {
        assertThatThrownBy(() -> PartitionPlan.split(0, 10, LAYOUT)).isInstanceOf(IllegalArgumentException.class);
    }
}
