package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Component;

@Component
public class PartitionBlobWriter {

    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final PartitionCopy transfer;
    private final BlobPaths paths;

    public PartitionBlobWriter(PartitionCopy transfer, BlobPaths paths) {
        this.transfer = transfer;
        this.paths = paths;
    }

    public UploadedPartition upload(ReceivedFileDocument original, PartitionRange range, ProgressCounter progress) {
        String target = paths.partitionPath(original, range.index());
        long startNanos = System.nanoTime();
        long copiedBytes = transfer.copy(original.currentPath(), range.bytes(), target,
                original.headerLineBytes(), progress);
        requireComplete(range, copiedBytes);
        return new UploadedPartition(target, range.fileSizeBytes(), (System.nanoTime() - startNanos) / NANOS_PER_MILLI);
    }

    private static void requireComplete(PartitionRange range, long copiedBytes) {
        if (range.bytes().isFullyCopied(copiedBytes)) {
            return;
        }
        throw new IllegalStateException("partição " + range.index() + " copiou " + copiedBytes
                + " bytes; esperado " + range.bytes().length());
    }

    public record UploadedPartition(String path, long sizeBytes, long durationMs) {
    }
}
