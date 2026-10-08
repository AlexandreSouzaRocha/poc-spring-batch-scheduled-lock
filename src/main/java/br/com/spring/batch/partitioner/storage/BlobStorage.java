package br.com.spring.batch.partitioner.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;

import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.CopyStatusType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;

import org.springframework.stereotype.Component;

@Component
public class BlobStorage {

    private static final StructuredLogger log = StructuredLogger.of(BlobStorage.class, "blob-storage");
    private static final Duration COPY_POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration COPY_TIMEOUT = Duration.ofMinutes(30);

    private final BlobContainerClient container;
    private final BlobSettings settings;

    public BlobStorage(BlobContainerClient container, BlobSettings settings) {
        this.container = container;
        this.settings = settings;
    }

    public List<BlobFile> list(String prefix) {
        return container.listBlobs(new ListBlobsOptions().setPrefix(prefix), null).stream()
                .map(BlobStorage::toBlobFile)
                .toList();
    }

    public boolean exists(String path) {
        return exists(container.getBlobClient(path));
    }

    public int deleteByPrefix(String prefix) {
        return (int) list(prefix).stream()
                .filter(file -> container.getBlobClient(file.path()).deleteIfExists())
                .count();
    }

    public byte[] read(String path, ByteRange range) {
        try (InputStream input = openStream(path, range)) {
            return input.readNBytes((int) range.length());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public InputStream openStream(String path, ByteRange range) {
        int blockSize = (int) Math.min(settings.readBlockSizeBytes(), Math.max(range.length(), 1));
        return container.getBlobClient(path).openInputStream(new BlobInputStreamOptions()
                .setRange(new BlobRange(range.start(), range.length()))
                .setBlockSize(blockSize));
    }

    public String readableUrl(String path, Duration validity) {
        BlobClient blob = container.getBlobClient(path);
        BlobServiceSasSignatureValues readPermission = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plus(validity), new BlobSasPermission().setReadPermission(true));
        return blob.getBlobUrl() + "?" + blob.generateSas(readPermission);
    }

    public BlockOutputStream openOutput(String path) {
        return new BlockOutputStream(blockBlob(path), (int) settings.uploadBlockSizeBytes());
    }

    public BlockList openBlockList(String path) {
        return new BlockList(blockBlob(path));
    }

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
        if (!exists(targetPath)) {
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
            copyByStream(source, targetPath);
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

    private void copyByStream(BlobClient source, String targetPath) {
        BlobInputStreamOptions options = new BlobInputStreamOptions().setBlockSize(settings.readBlockSizeBytes());
        try (InputStream input = source.openInputStream(options);
             BlockOutputStream output = openOutput(targetPath)) {
            byte[] buffer = new byte[settings.copyBufferBytes()];
            int read = input.read(buffer);
            while (read != -1) {
                output.write(buffer, 0, read);
                read = input.read(buffer);
            }
            output.commit();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private BlockBlobClient blockBlob(String path) {
        return container.getBlobClient(path).getBlockBlobClient();
    }

    private static boolean exists(BlobClient blob) {
        return Boolean.TRUE.equals(blob.exists());
    }

    private static BlobFile toBlobFile(BlobItem item) {
        return new BlobFile(item.getName(), item.getProperties().getContentLength(), item.getProperties().getETag());
    }
}
