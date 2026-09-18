package br.com.spring.batch.partitioner.storage.azure;

import java.time.Duration;
import java.time.OffsetDateTime;

import br.com.spring.batch.partitioner.storage.BlobUrls;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

public class AzureBlobUrls implements BlobUrls {

    private final BlobContainerClient container;

    public AzureBlobUrls(BlobContainerClient container) {
        this.container = container;
    }

    @Override
    public String readableUrl(String path, Duration validity) {
        BlobClient blob = container.getBlobClient(path);
        return blob.getBlobUrl() + "?" + blob.generateSas(readPermission(validity));
    }

    private static BlobServiceSasSignatureValues readPermission(Duration validity) {
        return new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(validity),
                new BlobSasPermission().setReadPermission(true));
    }
}
