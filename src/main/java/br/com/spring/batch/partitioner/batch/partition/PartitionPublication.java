package br.com.spring.batch.partitioner.batch.partition;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.messaging.PartitionedFilePublisher;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;

import org.springframework.stereotype.Component;

@Component
public class PartitionPublication {

    private final PartitionFileRepository repository;
    private final PartitionedFilePublisher publisher;

    public PartitionPublication(PartitionFileRepository repository, PartitionedFilePublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    public List<ReceivedFileDocument> publishPending(ReceivedFileDocument original) throws Exception {
        List<ReceivedFileDocument> pending = repository.findUnpublished(original.id());
        publisher.publishAndAwait(pending);
        repository.completePublished(pending.stream().map(ReceivedFileDocument::id).toList(), Instant.now());
        return pending;
    }

    public String topic() {
        return publisher.topic();
    }
}
