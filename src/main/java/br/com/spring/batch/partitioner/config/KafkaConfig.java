package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.AppProperties.KafkaSettings;
import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic partitionedFilesTopic(KafkaSettings settings) {
        return TopicBuilder.name(settings.topic())
                .partitions(settings.topicPartitions())
                .replicas(1)
                .build();
    }
}
