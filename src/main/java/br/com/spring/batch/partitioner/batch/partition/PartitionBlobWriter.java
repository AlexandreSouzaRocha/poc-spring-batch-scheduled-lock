package br.com.spring.batch.partitioner.batch.partition;

import java.io.IOException;
import java.io.UncheckedIOException;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument.WrittenBlob;
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

    public WrittenBlob write(ReceivedFileDocument original, PartitionRange range) {
        String target = paths.partitionPath(original, range.index());
        long startNanos = System.nanoTime();
        byte[] header = original.headerLineBytes();
        long copiedBytes = transfer.copy(original.currentPath(), range, target, header);
        long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
        return new WrittenBlob(paths.partitionFileName(original, range.index()), target, header.length + copiedBytes,
                durationMs);
    }

    private static void requireComplete(PartitionRange range, long copiedBytes) {
        if (!range.bytes().isFullyCopied(copiedBytes)) {
            throw new IllegalStateException("partição " + range.index() + " copiou " + copiedBytes
                    + " bytes; esperado " + range.bytes().length());
        }
    }

    private record BlobTransfer(BlobReader reader, BlobWriter writer) {

        long copy(String sourcePath, PartitionRange range, String targetPath, byte[] header) {
            try (BlobUpload upload = writer.open(targetPath)) {
                upload.output().write(header);
                long copiedBytes = reader.copy(sourcePath, range.bytes(), upload.output());
                requireComplete(range, copiedBytes);
                upload.commit();
                return copiedBytes;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
