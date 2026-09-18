package br.com.spring.batch.partitioner.config;

import java.time.Duration;

import br.com.spring.batch.partitioner.batch.partition.PartitionCopy;
import br.com.spring.batch.partitioner.batch.partition.ServerSidePartitionCopy;
import br.com.spring.batch.partitioner.batch.partition.StreamingPartitionCopy;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobMover;
import br.com.spring.batch.partitioner.storage.BlobReader;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobCatalog;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobMover;
import br.com.spring.batch.partitioner.storage.BlobUrls;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobReader;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobUrls;
import br.com.spring.batch.partitioner.storage.azure.AzureBlobWriter;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfig {

    private static final int SOURCE_URL_VALIDITY_HOURS = 6;

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
    public BlobUrls blobUrls(BlobContainerClient container) {
        return new AzureBlobUrls(container);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "server-side-copy", havingValue = "false",
            matchIfMissing = true)
    public PartitionCopy streamingPartitionCopy(BlobReader blobReader, BlobWriter blobWriter,
            AppProperties properties) {
        return new StreamingPartitionCopy(blobReader, blobWriter, (int) properties.blob().uploadBlockSizeBytes(),
                properties.partition().threadsPerPartition());
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "server-side-copy", havingValue = "true")
    public PartitionCopy serverSidePartitionCopy(BlobUrls blobUrls, BlobWriter blobWriter,
            AppProperties properties) {
        return new ServerSidePartitionCopy(blobUrls, blobWriter, properties.partition().serverSideBlockSizeBytes(),
                properties.partition().threadsPerPartition(), Duration.ofHours(SOURCE_URL_VALIDITY_HOURS));
    }

    @Bean
    public BlobMover blobMover(BlobContainerClient container, BlobReader blobReader, BlobWriter blobWriter) {
        return new AzureBlobMover(container, blobReader, blobWriter);
    }
}
