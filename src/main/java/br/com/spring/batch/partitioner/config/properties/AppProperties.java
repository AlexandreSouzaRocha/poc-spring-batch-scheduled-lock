package br.com.spring.batch.partitioner.config.properties;

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
            @Min(1) int copyBufferKb) {

        private static final int KILOBYTE = 1024;
        private static final int MEGABYTE = KILOBYTE * KILOBYTE;

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
            @Min(1) int maxConcurrentFiles,
            @Min(1) int filesPerCycle) {
    }

    public record KafkaSettings(
            @NotBlank String topic,
            @Min(1) int topicPartitions,
            @Min(1) int sendTimeoutSeconds) {
    }
}
