package br.com.spring.batch.partitioner.storage.azure;

import java.util.List;

import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobFile;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

public class AzureBlobCatalog implements BlobCatalog {

    private final BlobContainerClient container;

    public AzureBlobCatalog(BlobContainerClient container) {
        this.container = container;
    }

    @Override
    public List<BlobFile> list(String prefix) {
        return container.listBlobs(new ListBlobsOptions().setPrefix(prefix), null).stream()
                .map(AzureBlobCatalog::toBlobFile)
                .toList();
    }

    @Override
    public int deleteByPrefix(String prefix) {
        return (int) list(prefix).stream()
                .filter(file -> container.getBlobClient(file.path()).deleteIfExists())
                .count();
    }

    private static BlobFile toBlobFile(BlobItem item) {
        return new BlobFile(item.getName(), item.getProperties().getContentLength(), item.getProperties().getETag());
    }
}
