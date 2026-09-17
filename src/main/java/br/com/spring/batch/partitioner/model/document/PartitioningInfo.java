package br.com.spring.batch.partitioner.model.document;

import br.com.spring.batch.partitioner.model.partition.PartitionRange;

import org.springframework.data.mongodb.core.mapping.Field;

public record PartitioningInfo(
        @Field(ReceivedFileFields.INDEX) Integer index,
        @Field(ReceivedFileFields.COUNT) int count,
        @Field(ReceivedFileFields.LINE_COUNT) long lineCount,
        @Field(ReceivedFileFields.BYTE_START) Long byteStart,
        @Field(ReceivedFileFields.BYTE_END) Long byteEnd) {

    public static PartitioningInfo ofOriginal(long lineCount, int partitionCount) {
        return new PartitioningInfo(null, partitionCount, lineCount, null, null);
    }

    public static PartitioningInfo ofPartition(PartitionRange range, int partitionCount) {
        return new PartitioningInfo(range.index(), partitionCount, range.lineCount(), range.bytes().start(),
                range.bytes().end());
    }
}
