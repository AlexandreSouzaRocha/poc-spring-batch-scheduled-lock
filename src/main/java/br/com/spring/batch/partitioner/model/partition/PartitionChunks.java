package br.com.spring.batch.partitioner.model.partition;

import java.util.ArrayList;
import java.util.List;

public record PartitionChunks(List<PartitionChunk> chunks, int blockCount) {

    public static PartitionChunks of(ByteRange range, int chunkCount, int blockSizeBytes, int firstBlockIndex) {
        int totalBlocks = blockCountOf(range.length(), blockSizeBytes);
        int blocksPerChunk = divideRoundingUp(totalBlocks, chunkCount);
        List<PartitionChunk> chunks = new ArrayList<>();
        for (int block = 0; block < totalBlocks; block += blocksPerChunk) {
            chunks.add(chunkAt(range, block, Math.min(blocksPerChunk, totalBlocks - block),
                    blockSizeBytes, firstBlockIndex));
        }
        return new PartitionChunks(List.copyOf(chunks), totalBlocks);
    }

    public int size() {
        return chunks.size();
    }

    private static PartitionChunk chunkAt(ByteRange range, int firstBlock, int blocks, int blockSizeBytes,
            int firstBlockIndex) {
        long start = range.start() + (long) firstBlock * blockSizeBytes;
        long end = Math.min(range.end(), start + (long) blocks * blockSizeBytes);
        return new PartitionChunk(new ByteRange(start, end), firstBlockIndex + firstBlock);
    }

    private static int blockCountOf(long length, int blockSizeBytes) {
        return (int) divideRoundingUp(length, blockSizeBytes);
    }

    private static int divideRoundingUp(int value, int divisor) {
        return (int) divideRoundingUp((long) value, divisor);
    }

    private static long divideRoundingUp(long value, int divisor) {
        return (value + divisor - 1) / divisor;
    }
}
