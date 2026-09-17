package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.FolderSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.KafkaSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SettingsConfig {

    @Bean
    public BlobSettings blobSettings(AppProperties properties) {
        return properties.blob();
    }

    @Bean
    public FolderSettings folderSettings(AppProperties properties) {
        return properties.folders();
    }

    @Bean
    public PartitionSettings partitionSettings(AppProperties properties) {
        return properties.partition();
    }

    @Bean
    public KafkaSettings kafkaSettings(AppProperties properties) {
        return properties.kafka();
    }
}
