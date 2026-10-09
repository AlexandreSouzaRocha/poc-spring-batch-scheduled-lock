package br.com.spring.batch.partitioner.batch.partition;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.layout.DetailLineScanner;
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
    private final DetailLineScanner scanner;
    private final int blockSizeBytes;
    private final int threads;

    public StreamingPartitionCopy(BlobStorage storage, DetailLineScanner scanner, int blockSizeBytes, int threads) {
        this.storage = storage;
        this.scanner = scanner;
        this.blockSizeBytes = blockSizeBytes;
        this.threads = threads;
    }

    @Override
    public CopyResult copy(String sourcePath, ByteRange range, PartitionTarget target, ProgressCounter progress) {
        PartitionChunks chunks = PartitionChunks.of(range, threads, blockSizeBytes, FIRST_DATA_BLOCK_INDEX);
        BlockList blocks = storage.openBlockList(target.path());
        blocks.stage(HEADER_BLOCK_INDEX, target.header(), target.header().length);
        ChunkCopy copied = copyChunks(sourcePath, chunks.chunks(), blocks, progress);
        int blockCount = chunks.blockCount() + FIRST_DATA_BLOCK_INDEX;
        if (target.hasTrailer()) {
            blocks.stage(blockCount, target.trailer(), target.trailer().length);
            blockCount++;
        }
        blocks.commit(blockCount);
        return new CopyResult(copied.bytes(), OptionalLong.of(copied.lines()));
    }

    private ChunkCopy copyChunks(String sourcePath, List<Chunk> chunks, BlockList blocks, ProgressCounter progress) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            return chunks.stream()
                    .map(chunk -> executor.submit(() -> RequestContext.call(requestId,
                            () -> copyChunk(sourcePath, chunk, blocks, progress))))
                    .toList()
                    .stream()
                    .map(StreamingPartitionCopy::await)
                    .reduce(new ChunkCopy(0, 0), ChunkCopy::plus);
        }
    }

    private ChunkCopy copyChunk(String sourcePath, Chunk chunk, BlockList blocks, ProgressCounter progress) {
        byte[] buffer = new byte[blockSizeBytes];
        int blockIndex = chunk.firstBlockIndex();
        long copiedBytes = 0;
        long lines = 0;
        try (InputStream input = storage.openStream(sourcePath, chunk.bytes())) {
            int read = input.readNBytes(buffer, 0, buffer.length);
            while (read > 0) {
                lines += scanner.scan(buffer, read, chunk.bytes().start() + copiedBytes);
                blocks.stage(blockIndex, buffer, read);
                progress.advance(read);
                copiedBytes += read;
                blockIndex++;
                read = input.readNBytes(buffer, 0, buffer.length);
            }
            return new ChunkCopy(copiedBytes, lines);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ChunkCopy await(Future<ChunkCopy> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("cópia de partição interrompida", e);
        } catch (Exception e) {
            throw new IllegalStateException("falha ao copiar trecho da partição", e);
        }
    }

    private record ChunkCopy(long bytes, long lines) {

        ChunkCopy plus(ChunkCopy other) {
            return new ChunkCopy(bytes + other.bytes, lines + other.lines);
        }
    }
}
