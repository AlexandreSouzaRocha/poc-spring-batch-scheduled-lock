package br.com.spring.batch.partitioner.model.partition;

import java.util.List;
import java.util.stream.IntStream;

public final class PartitionPlan {

    private final List<PartitionRange> ranges;

    private PartitionPlan(List<PartitionRange> ranges) {
        this.ranges = List.copyOf(ranges);
    }

    public static PartitionPlan split(long totalLines, int requestedPartitions) {
        requirePositive(totalLines, "totalLines");
        requirePositive(requestedPartitions, "requestedPartitions");
        int partitions = effectivePartitionCount(totalLines, requestedPartitions);
        long baseLines = totalLines / partitions;
        long remainder = totalLines % partitions;
        return new PartitionPlan(IntStream.range(0, partitions)
                .mapToObj(position -> PartitionRange.of(position + 1,
                        firstLineOf(position, baseLines, remainder),
                        baseLines + extraLine(position, remainder)))
                .toList());
    }

    public static int effectivePartitionCount(long totalLines, int requestedPartitions) {
        return (int) Math.min(requestedPartitions, totalLines);
    }

    public List<PartitionRange> ranges() {
        return ranges;
    }

    public int size() {
        return ranges.size();
    }

    public long totalLines() {
        return ranges.stream().mapToLong(PartitionRange::lineCount).sum();
    }

    private static long firstLineOf(int position, long baseLines, long remainder) {
        return position * baseLines + Math.min(position, remainder);
    }

    private static long extraLine(int position, long remainder) {
        return position < remainder ? 1 : 0;
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " deve ser > 0");
        }
    }
}
