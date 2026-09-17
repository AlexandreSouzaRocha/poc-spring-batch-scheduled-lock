package br.com.spring.batch.partitioner.batch.tasklet;

import java.util.List;

import br.com.spring.batch.partitioner.batch.partition.PartitionPublication;
import br.com.spring.batch.partitioner.batch.step.FileStep;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
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
    private final PartitionPublication publication;

    public PublishPartitionsTasklet(FileStepSupport support, PartitionPublication publication) {
        this.support = support;
        this.publication = publication;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        FileStep step = new FileStep(contribution);
        ReceivedFileDocument original = support.load(step, ChaosPoint.PUBLISH);
        List<ReceivedFileDocument> published = publication.publishPending(original);
        log.info("partition.publish").field("fileId", original.id()).field("topic", publication.topic())
                .field("published", published.size()).field("partitions", original.partitionCount())
                .data("files", published.stream().map(ReceivedFileDocument::currentPath).toList())
                .log("metadados das partições publicados no Kafka");
        return RepeatStatus.FINISHED;
    }
}
