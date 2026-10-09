package br.com.spring.batch.partitioner.config;

import java.time.Duration;

import br.com.spring.batch.partitioner.batch.partition.PartitionCopy;
import br.com.spring.batch.partitioner.batch.partition.ServerSidePartitionCopy;
import br.com.spring.batch.partitioner.batch.partition.StreamingPartitionCopy;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.model.layout.DetailLineScanner;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

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
                .retryOptions(retryOptionsOf(settings))
                .httpClient(httpClientOf(settings))
                .buildClient()
                .getBlobContainerClient(settings.container());
    }

    private static RequestRetryOptions retryOptionsOf(BlobSettings settings) {
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, settings.maxTries(), settings.tryTimeout(),
                settings.retryDelay(), settings.maxRetryDelay(), null);
    }

    private static HttpClient httpClientOf(BlobSettings settings) {
        return new NettyAsyncHttpClientBuilder()
                .connectTimeout(settings.connectTimeout())
                .responseTimeout(settings.responseTimeout())
                .readTimeout(settings.responseTimeout())
                .writeTimeout(settings.responseTimeout())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "server-side-copy", havingValue = "false",
            matchIfMissing = true)
    public PartitionCopy streamingPartitionCopy(BlobStorage storage, FileLayout layout, AppProperties properties) {
        return new StreamingPartitionCopy(storage, new DetailLineScanner(layout),
                (int) properties.blob().uploadBlockSizeBytes(),
                properties.partition().threadsPerPartition());
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "server-side-copy", havingValue = "true")
    public PartitionCopy serverSidePartitionCopy(BlobStorage storage, AppProperties properties) {
        return new ServerSidePartitionCopy(storage, properties.partition().serverSideBlockSizeBytes(),
                properties.partition().threadsPerPartition(), Duration.ofHours(SOURCE_URL_VALIDITY_HOURS));
    }
}
