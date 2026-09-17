package br.com.spring.batch.partitioner.storage.azure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.storage.BlobReader;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobInputStreamOptions;

public class AzureBlobReader implements BlobReader {

    private final BlobContainerClient container;
    private final BlobSettings settings;

    public AzureBlobReader(BlobContainerClient container, BlobSettings settings) {
        this.container = container;
        this.settings = settings;
    }

    @Override
    public byte[] read(String path, ByteRange range) {
        try (InputStream input = open(path, range)) {
            return input.readNBytes((int) range.length());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long copy(String path, ByteRange range, OutputStream target) {
        try (InputStream input = open(path, range)) {
            return transfer(input, target);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void copyAll(String path, OutputStream target) {
        BlobInputStreamOptions options = new BlobInputStreamOptions().setBlockSize(settings.readBlockSizeBytes());
        try (InputStream input = container.getBlobClient(path).openInputStream(options)) {
            transfer(input, target);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InputStream open(String path, ByteRange range) {
        int blockSize = (int) Math.min(settings.readBlockSizeBytes(), Math.max(range.length(), 1));
        return container.getBlobClient(path).openInputStream(new BlobInputStreamOptions()
                .setRange(new BlobRange(range.start(), range.length()))
                .setBlockSize(blockSize));
    }

    private long transfer(InputStream input, OutputStream target) throws IOException {
        byte[] buffer = new byte[settings.copyBufferBytes()];
        long copied = 0;
        int read = input.read(buffer);
        while (read != -1) {
            target.write(buffer, 0, read);
            copied += read;
            read = input.read(buffer);
        }
        return copied;
    }
}
