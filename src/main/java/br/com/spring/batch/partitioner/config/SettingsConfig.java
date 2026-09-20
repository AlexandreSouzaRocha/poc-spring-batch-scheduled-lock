package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.BlobSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.FolderSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.KafkaSettings;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;

import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.queue.MovementDependencies;
import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.service.dispatch.ConcurrentDispatch;
import br.com.spring.batch.partitioner.service.dispatch.QueueDispatch;
import br.com.spring.batch.partitioner.service.dispatch.SequentialDispatch;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

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

    @Bean
    public FileLayout fileLayout(AppProperties properties) {
        return properties.file().layout();
    }

    @Bean
    public MovementDependencies movementDependencies(AppProperties properties) {
        return properties.file().movementDependencies();
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "dispatch", havingValue = "CONCURRENT",
            matchIfMissing = true)
    public QueueDispatch concurrentDispatch(FilePartitionLauncher launcher, ProcessingLock processingLock,
            SchedulerProperties scheduler, AppProperties properties) {
        return new ConcurrentDispatch(launcher, processingLock, scheduler, properties.partition());
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.partition", name = "dispatch", havingValue = "SEQUENTIAL")
    public QueueDispatch sequentialDispatch(FilePartitionLauncher launcher, ProcessingLock processingLock,
            SchedulerProperties scheduler) {
        return new SequentialDispatch(launcher, processingLock, scheduler);
    }
}
