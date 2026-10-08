package br.com.spring.batch.partitioner.batch.partition;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks.Chunk;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockList;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class StreamingPartitionCopy implements PartitionCopy {

    private static final int HEADER_BLOCK_INDEX = 0;
    private static final int FIRST_DATA_BLOCK_INDEX = 1;

    private final BlobStorage storage;
    private final int blockSizeBytes;
    private final int threads;

    public StreamingPartitionCopy(BlobStorage storage, int blockSizeBytes, int threads) {
        this.storage = storage;
        this.blockSizeBytes = blockSizeBytes;
        this.threads = threads;
    }

    @Override
    public long copy(String sourcePath, ByteRange range, String targetPath, byte[] header, ProgressCounter progress) {
        PartitionChunks chunks = PartitionChunks.of(range, threads, blockSizeBytes, FIRST_DATA_BLOCK_INDEX);
        BlockList blocks = storage.openBlockList(targetPath);
        blocks.stage(HEADER_BLOCK_INDEX, header, header.length);
        long copiedBytes = copyChunks(sourcePath, chunks.chunks(), blocks, progress);
        blocks.commit(chunks.blockCount() + FIRST_DATA_BLOCK_INDEX);
        return copiedBytes;
    }

    private long copyChunks(String sourcePath, List<Chunk> chunks, BlockList blocks,
            ProgressCounter progress) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            return chunks.stream()
                    .map(chunk -> executor.submit(() -> RequestContext.call(requestId,
                            () -> copyChunk(sourcePath, chunk, blocks, progress))))
                    .toList()
                    .stream()
                    .mapToLong(StreamingPartitionCopy::await)
                    .sum();
        }
    }

    private long copyChunk(String sourcePath, Chunk chunk, BlockList blocks, ProgressCounter progress) {
        byte[] buffer = new byte[blockSizeBytes];
        int blockIndex = chunk.firstBlockIndex();
        long copiedBytes = 0;
        try (InputStream input = storage.openStream(sourcePath, chunk.bytes())) {
            int read = input.readNBytes(buffer, 0, buffer.length);
            while (read > 0) {
                blocks.stage(blockIndex, buffer, read);
                progress.advance(read);
                copiedBytes += read;
                blockIndex++;
                read = input.readNBytes(buffer, 0, buffer.length);
            }
            return copiedBytes;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long await(Future<Long> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("cópia de partição interrompida", e);
        } catch (Exception e) {
            throw new IllegalStateException("falha ao copiar trecho da partição", e);
        }
    }
}
