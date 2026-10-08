package br.com.spring.batch.partitioner.batch.partition;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks.Chunk;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockList;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class ServerSidePartitionCopy implements PartitionCopy {

    private static final int HEADER_BLOCK_INDEX = 0;
    private static final int FIRST_DATA_BLOCK_INDEX = 1;

    private final BlobStorage storage;
    private final int blockSizeBytes;
    private final int threads;
    private final Duration sourceUrlValidity;

    public ServerSidePartitionCopy(BlobStorage storage, int blockSizeBytes, int threads, Duration sourceUrlValidity) {
        this.storage = storage;
        this.blockSizeBytes = blockSizeBytes;
        this.threads = threads;
        this.sourceUrlValidity = sourceUrlValidity;
    }

    @Override
    public long copy(String sourcePath, ByteRange range, String targetPath, byte[] header, ProgressCounter progress) {
        String sourceUrl = storage.readableUrl(sourcePath, sourceUrlValidity);
        PartitionChunks chunks = PartitionChunks.of(range, threads, blockSizeBytes, FIRST_DATA_BLOCK_INDEX);
        BlockList blocks = storage.openBlockList(targetPath);
        blocks.stage(HEADER_BLOCK_INDEX, header, header.length);
        long copiedBytes = stageChunks(sourceUrl, chunks.chunks(), blocks, progress);
        blocks.commit(chunks.blockCount() + FIRST_DATA_BLOCK_INDEX);
        return copiedBytes;
    }

    private long stageChunks(String sourceUrl, List<Chunk> chunks, BlockList blocks,
            ProgressCounter progress) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            return chunks.stream()
                    .map(chunk -> executor.submit(() -> RequestContext.call(requestId,
                            () -> stageChunk(sourceUrl, chunk, blocks, progress))))
                    .toList()
                    .stream()
                    .mapToLong(ServerSidePartitionCopy::await)
                    .sum();
        }
    }

    private long stageChunk(String sourceUrl, Chunk chunk, BlockList blocks, ProgressCounter progress) {
        long copiedBytes = 0;
        int blockIndex = chunk.firstBlockIndex();
        for (ByteRange block : blocksOf(chunk.bytes())) {
            blocks.stageFromUrl(blockIndex, sourceUrl, block);
            progress.advance(block.length());
            copiedBytes += block.length();
            blockIndex++;
        }
        return copiedBytes;
    }

    private List<ByteRange> blocksOf(ByteRange range) {
        return LongStream.iterate(range.start(), start -> start < range.end(),
                        start -> start + blockSizeBytes)
                .mapToObj(start -> new ByteRange(start, Math.min(range.end(), start + blockSizeBytes)))
                .toList();
    }

    private static long await(Future<Long> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("cópia server-side de partição interrompida", e);
        } catch (Exception e) {
            throw new IllegalStateException("falha ao copiar trecho da partição no servidor", e);
        }
    }
}
