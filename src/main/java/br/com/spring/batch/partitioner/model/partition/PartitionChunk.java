package br.com.spring.batch.partitioner.model.partition;

public record PartitionChunk(ByteRange bytes, int firstBlockIndex) {
}
