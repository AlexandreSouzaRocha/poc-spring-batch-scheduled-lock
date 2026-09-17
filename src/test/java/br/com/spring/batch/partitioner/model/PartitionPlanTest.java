package br.com.spring.batch.partitioner.model;

import java.util.List;

import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionPlanTest {

    @Test
    void splitsLinesIntoContiguousRanges() {
        PartitionPlan plan = PartitionPlan.split(1_000_003, 10);
        List<PartitionRange> ranges = plan.ranges();

        assertThat(plan.size()).isEqualTo(10);
        assertThat(plan.totalLines()).isEqualTo(1_000_003);
        assertThat(ranges).extracting(PartitionRange::lineCount).startsWith(100_001L, 100_001L, 100_001L, 100_000L);
        assertThat(ranges.getFirst().bytes().start()).isEqualTo(FileLayout.HEADER_LINE_BYTES);
        assertThat(ranges.getLast().bytes().end())
                .isEqualTo(FileLayout.HEADER_LINE_BYTES + 1_000_003L * FileLayout.RECORD_LINE_BYTES);
        for (int position = 1; position < ranges.size(); position++) {
            assertThat(ranges.get(position).bytes().start()).isEqualTo(ranges.get(position - 1).bytes().end());
        }
    }

    @Test
    void neverCreatesMorePartitionsThanLines() {
        PartitionPlan plan = PartitionPlan.split(3, 10);

        assertThat(plan.size()).isEqualTo(3);
        assertThat(plan.ranges()).extracting(PartitionRange::lineCount).containsOnly(1L);
    }

    @Test
    void roundTripsThroughExecutionContext() {
        PartitionRange range = PartitionPlan.split(500, 4).ranges().get(2);

        assertThat(PartitionRange.from(range.toExecutionContext())).isEqualTo(range);
        assertThat(range.stepName()).isEqualTo("partition0003");
    }

    @Test
    void rejectsEmptyFiles() {
        assertThatThrownBy(() -> PartitionPlan.split(0, 10)).isInstanceOf(IllegalArgumentException.class);
    }
}
