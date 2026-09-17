package br.com.spring.batch.partitioner.storage.azure;

import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import com.azure.storage.blob.BlobContainerClient;

public class AzureBlobWriter implements BlobWriter {

    private final BlobContainerClient container;
    private final BlobSettings settings;

    public AzureBlobWriter(BlobContainerClient container, BlobSettings settings) {
        this.container = container;
        this.settings = settings;
    }

    @Override
    public BlobUpload open(String path) {
        return new AzureBlockUpload(container.getBlobClient(path).getBlockBlobClient(),
                (int) settings.uploadBlockSizeBytes());
    }
}
