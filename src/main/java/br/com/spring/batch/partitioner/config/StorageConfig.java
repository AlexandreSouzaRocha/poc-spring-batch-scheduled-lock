package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobMover;
import br.com.spring.batch.partitioner.storage.BlobReader;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobCatalog;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobMover;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobReader;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobWriter;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfig {

    @Bean
    public BlobContainerClient blobContainerClient(BlobSettings settings) {
        return new BlobServiceClientBuilder()
                .endpoint(settings.endpoint())
                .credential(new StorageSharedKeyCredential(settings.accountName(), settings.accountKey()))
                .buildClient()
                .getBlobContainerClient(settings.container());
    }

    @Bean
    public BlobCatalog blobCatalog(BlobContainerClient container) {
        return new AzureBlobCatalog(container);
    }

    @Bean
    public BlobReader blobReader(BlobContainerClient container, BlobSettings settings) {
        return new AzureBlobReader(container, settings);
    }

    @Bean
    public BlobWriter blobWriter(BlobContainerClient container, BlobSettings settings) {
        return new AzureBlobWriter(container, settings);
    }

    @Bean
    public BlobMover blobMover(BlobContainerClient container, BlobReader blobReader, BlobWriter blobWriter) {
        return new AzureBlobMover(container, blobReader, blobWriter);
    }
}
