package br.com.spring.batch.partitioner.batch.partition;

import java.io.IOException;
import java.io.UncheckedIOException;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobReader;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobWriter;

import org.springframework.stereotype.Component;

@Component
public class PartitionBlobWriter {

    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final BlobTransfer transfer;
    private final BlobPaths paths;

    public PartitionBlobWriter(BlobReader reader, BlobWriter writer, BlobPaths paths) {
        this.transfer = new BlobTransfer(reader, writer);
        this.paths = paths;
    }

    public UploadedPartition upload(ReceivedFileDocument original, PartitionRange range) {
        String target = paths.partitionPath(original, range.index());
        long startNanos = System.nanoTime();
        transfer.copy(original.currentPath(), range, target, original.headerLineBytes());
        return new UploadedPartition(target, range.fileSizeBytes(), (System.nanoTime() - startNanos) / NANOS_PER_MILLI);
    }

    public record UploadedPartition(String path, long sizeBytes, long durationMs) {
    }

    private record BlobTransfer(BlobReader reader, BlobWriter writer) {

        void copy(String sourcePath, PartitionRange range, String targetPath, byte[] header) {
            try (BlobUpload upload = writer.open(targetPath)) {
                upload.output().write(header);
                requireComplete(range, reader.copy(sourcePath, range.bytes(), upload.output()));
                upload.commit();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static void requireComplete(PartitionRange range, long copiedBytes) {
            if (!range.bytes().isFullyCopied(copiedBytes)) {
                throw new IllegalStateException("partição " + range.index() + " copiou " + copiedBytes
                        + " bytes; esperado " + range.bytes().length());
            }
        }
    }
}
