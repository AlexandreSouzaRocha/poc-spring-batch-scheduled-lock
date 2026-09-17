package br.com.spring.batch.partitioner.batch.tasklet;

import java.util.List;

import br.com.spring.batch.partitioner.batch.partition.PartitionRegistration;
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
public class RegisterPartitionsTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(RegisterPartitionsTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final PartitionRegistration registration;

    public RegisterPartitionsTasklet(FileStepSupport support, PartitionRegistration registration) {
        this.support = support;
        this.registration = registration;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        ReceivedFileDocument original = support.load(step, ChaosPoint.REGISTER);
        List<ReceivedFileDocument> partitions = registration.registerUploaded(original);
        log.info("partition.register").field("fileId", original.id()).field("partitions", partitions.size())
                .field("status", "UPLOADED")
                .data("files", partitions.stream().map(ReceivedFileDocument::currentPath).toList())
                .log("partições enviadas ao blob registradas na received_file_management");
        return RepeatStatus.FINISHED;
    }
}
