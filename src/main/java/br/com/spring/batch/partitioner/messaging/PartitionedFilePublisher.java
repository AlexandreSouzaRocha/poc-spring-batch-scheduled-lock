package br.com.spring.batch.partitioner.messaging;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import br.com.spring.batch.partitioner.config.properties.AppProperties.KafkaSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.event.PartitionedFileEvent;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class PartitionedFilePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final KafkaSettings settings;

    public PartitionedFilePublisher(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper,
                                    KafkaSettings settings) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.settings = settings;
    }

    public String topic() {
        return settings.topic();
    }

    public void publishAndAwait(List<ReceivedFileDocument> partitions) throws Exception {
        List<CompletableFuture<SendResult<String, String>>> acknowledgements = partitions.stream()
                .map(this::send)
                .toList();
        kafkaTemplate.flush();
        for (CompletableFuture<SendResult<String, String>> acknowledgement : acknowledgements) {
            await(acknowledgement);
        }
    }

    private CompletableFuture<SendResult<String, String>> send(ReceivedFileDocument partition) {
        PartitionedFileEvent event = new PartitionedFileEvent(partition.movementType().name(),
                partition.currentPath(), partition.movementDate());
        return kafkaTemplate.send(settings.topic(), partition.fileName(), jsonMapper.writeValueAsString(event));
    }

    private void await(CompletableFuture<SendResult<String, String>> acknowledgement) throws Exception {
        try {
            acknowledgement.get(settings.sendTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }
}
