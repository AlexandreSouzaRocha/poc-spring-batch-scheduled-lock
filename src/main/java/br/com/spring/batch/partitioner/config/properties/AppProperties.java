package br.com.spring.batch.partitioner.config.properties;

import java.time.Duration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @Valid @NotNull BlobSettings blob,
        @Valid @NotNull FolderSettings folders,
        @Valid @NotNull PartitionSettings partition,
        @Valid @NotNull KafkaSettings kafka) {

    public record BlobSettings(
            @NotBlank String endpoint,
            @NotBlank String accountName,
            @NotBlank String accountKey,
            @NotBlank String container,
            @Min(1) int uploadBlockSizeMb,
            @Min(1) int readBlockSizeMb,
            @Min(1) int copyBufferKb,
            @Min(1) int maxTries,
            @Min(1) int tryTimeoutSeconds,
            @Min(1) int retryDelaySeconds,
            @Min(1) int maxRetryDelaySeconds,
            @Min(1) int responseTimeoutSeconds) {

        private static final int KILOBYTE = 1024;
        private static final int MEGABYTE = KILOBYTE * KILOBYTE;

        public Duration tryTimeout() {
            return Duration.ofSeconds(tryTimeoutSeconds);
        }

        public Duration retryDelay() {
            return Duration.ofSeconds(retryDelaySeconds);
        }

        public Duration maxRetryDelay() {
            return Duration.ofSeconds(maxRetryDelaySeconds);
        }

        public Duration responseTimeout() {
            return Duration.ofSeconds(responseTimeoutSeconds);
        }

        public long uploadBlockSizeBytes() {
            return (long) uploadBlockSizeMb * MEGABYTE;
        }

        public int readBlockSizeBytes() {
            return readBlockSizeMb * MEGABYTE;
        }

        public int copyBufferBytes() {
            return copyBufferKb * KILOBYTE;
        }
    }

    public record FolderSettings(
            @NotBlank String inbox,
            @NotBlank String processed,
            @NotBlank String error) {
    }

    public record PartitionSettings(
            @Min(1) int count,
            @Min(1) int maxAttempts,
            @Min(1) int maxConcurrentTypes,
            @Min(1) int filesPerCycle,
            @Min(1) int threadsPerPartition,
            @Min(0) int progressIntervalSeconds,
            boolean serverSideCopy,
            @Min(1) int serverSideBlockSizeMb) {

        private static final int MEGABYTE = 1024 * 1024;

        public int serverSideBlockSizeBytes() {
            return serverSideBlockSizeMb * MEGABYTE;
        }
    }

    public record KafkaSettings(
            @NotBlank String topic,
            @Min(1) int sendTimeoutSeconds) {
    }
}
