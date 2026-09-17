package br.com.spring.batch.partitioner.storage.azure;

import java.time.Duration;

import br.com.spring.batch.partitioner.storage.BlobMover;
import br.com.spring.batch.partitioner.storage.BlobReader;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.CopyStatusType;

public class AzureBlobMover implements BlobMover {

    private static final StructuredLogger log = StructuredLogger.of(AzureBlobMover.class, "blob-storage");
    private static final Duration COPY_POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration COPY_TIMEOUT = Duration.ofMinutes(30);

    private final BlobContainerClient container;
    private final StreamCopy streamCopy;

    public AzureBlobMover(BlobContainerClient container, BlobReader reader, BlobWriter writer) {
        this.container = container;
        this.streamCopy = new StreamCopy(reader, writer);
    }

    @Override
    public void move(String sourcePath, String targetPath) {
        BlobClient source = container.getBlobClient(sourcePath);
        if (!exists(source)) {
            requireAlreadyMoved(sourcePath, targetPath);
            return;
        }
        copy(source, targetPath);
        source.deleteIfExists();
    }

    private void requireAlreadyMoved(String sourcePath, String targetPath) {
        if (!exists(container.getBlobClient(targetPath))) {
            throw new IllegalStateException("blob de origem " + sourcePath + " e destino " + targetPath
                    + " não existem");
        }
        log.info("blob.move").field("source", sourcePath).field("target", targetPath)
                .log("move já estava concluído");
    }

    private void copy(BlobClient source, String targetPath) {
        try {
            copyServerSide(source, targetPath);
        } catch (BlobStorageException e) {
            log.warn("blob.move").field("source", source.getBlobName()).field("target", targetPath).error(e)
                    .log("cópia server-side indisponível; copiando por stream");
            streamCopy.copy(source.getBlobName(), targetPath);
        }
    }

    private void copyServerSide(BlobClient source, String targetPath) {
        CopyStatusType status = container.getBlobClient(targetPath)
                .beginCopy(source.getBlobUrl(), COPY_POLL_INTERVAL)
                .waitForCompletion(COPY_TIMEOUT)
                .getValue()
                .getCopyStatus();
        if (status != CopyStatusType.SUCCESS) {
            throw new IllegalStateException("cópia de " + source.getBlobName() + " terminou com status " + status);
        }
    }

    private static boolean exists(BlobClient blob) {
        return Boolean.TRUE.equals(blob.exists());
    }

    private record StreamCopy(BlobReader reader, BlobWriter writer) {

        void copy(String sourcePath, String targetPath) {
            try (BlobUpload upload = writer.open(targetPath)) {
                reader.copyAll(sourcePath, upload.output());
                upload.commit();
            }
        }
    }
}
