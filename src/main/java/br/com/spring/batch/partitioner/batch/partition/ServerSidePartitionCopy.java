package br.com.spring.batch.partitioner.batch.partition;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionChunk;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks;
import br.com.spring.batch.partitioner.storage.BlobUrls;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.storage.BlockUpload;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class ServerSidePartitionCopy implements PartitionCopy {

    private static final int HEADER_BLOCK_INDEX = 0;
    private static final int FIRST_DATA_BLOCK_INDEX = 1;

    private final BlobUrls urls;
    private final BlobWriter writer;
    private final int blockSizeBytes;
    private final int threads;
    private final Duration sourceUrlValidity;

    public ServerSidePartitionCopy(BlobUrls urls, BlobWriter writer, int blockSizeBytes, int threads,
            Duration sourceUrlValidity) {
        this.urls = urls;
        this.writer = writer;
        this.blockSizeBytes = blockSizeBytes;
        this.threads = threads;
        this.sourceUrlValidity = sourceUrlValidity;
    }

    @Override
    public long copy(String sourcePath, ByteRange range, String targetPath, byte[] header, ProgressCounter progress) {
        String sourceUrl = urls.readableUrl(sourcePath, sourceUrlValidity);
        PartitionChunks chunks = PartitionChunks.of(range, threads, blockSizeBytes, FIRST_DATA_BLOCK_INDEX);
        try (BlockUpload upload = writer.openBlocks(targetPath)) {
            upload.stage(HEADER_BLOCK_INDEX, header, header.length);
            long copiedBytes = stageChunks(sourceUrl, chunks.chunks(), upload, progress);
            upload.commit(chunks.blockCount() + FIRST_DATA_BLOCK_INDEX);
            return copiedBytes;
        }
    }

    private long stageChunks(String sourceUrl, List<PartitionChunk> chunks, BlockUpload upload,
            ProgressCounter progress) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            return chunks.stream()
                    .map(chunk -> executor.submit(() -> RequestContext.call(requestId,
                            () -> stageChunk(sourceUrl, chunk, upload, progress))))
                    .toList()
                    .stream()
                    .mapToLong(ServerSidePartitionCopy::await)
                    .sum();
        }
    }

    private long stageChunk(String sourceUrl, PartitionChunk chunk, BlockUpload upload, ProgressCounter progress) {
        long copiedBytes = 0;
        int blockIndex = chunk.firstBlockIndex();
        for (ByteRange block : blocksOf(chunk.bytes())) {
            upload.stageFromUrl(blockIndex, sourceUrl, block);
            progress.advance(block.length());
            copiedBytes += block.length();
            blockIndex++;
        }
        return copiedBytes;
    }

    private List<ByteRange> blocksOf(ByteRange range) {
        return java.util.stream.LongStream.iterate(range.start(), start -> start < range.end(),
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
