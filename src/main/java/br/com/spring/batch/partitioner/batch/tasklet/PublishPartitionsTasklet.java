package br.com.spring.batch.partitioner.batch.tasklet;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.messaging.PartitionedFilePublisher;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class PublishPartitionsTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(PublishPartitionsTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final PartitionFileRepository partitions;
    private final PartitionedFilePublisher publisher;

    public PublishPartitionsTasklet(FileStepSupport support, PartitionFileRepository partitions,
                                    PartitionedFilePublisher publisher) {
        this.support = support;
        this.partitions = partitions;
        this.publisher = publisher;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ReceivedFileDocument original = support.load(contribution, ChaosPoint.PUBLISH);
        List<ReceivedFileDocument> pending = partitions.findUnpublished(original.id());
        publisher.publishAndAwait(pending);
        partitions.completePublished(pending.stream().map(ReceivedFileDocument::id).toList(), Instant.now());
        log.info("partition.publish").field("fileId", original.id()).field("topic", publisher.topic())
                .field("published", pending.size()).field("partitions", original.partitionCount())
                .data("files", pending.stream().map(ReceivedFileDocument::currentPath).toList())
                .log("metadados das partições publicados no Kafka");
        return RepeatStatus.FINISHED;
    }
}
