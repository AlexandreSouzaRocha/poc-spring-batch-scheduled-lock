package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.OriginalFileArchiver;
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
public class MoveOriginalTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(MoveOriginalTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final OriginalFileArchiver archiver;

    public MoveOriginalTasklet(FileStepSupport support, OriginalFileArchiver archiver) {
        this.support = support;
        this.archiver = archiver;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        ReceivedFileDocument original = support.load(step, ChaosPoint.MOVE);
        String target = archiver.moveToProcessed(original);
        StepVolume.ofBytes(original.sizeBytes()).writeTo(step.executionContext());
        log.info("original.move").field("fileId", original.id()).field("source", original.currentPath())
                .field("target", target).data("sizeBytes", original.sizeBytes())
                .log("arquivo original movido para processados");
        return RepeatStatus.FINISHED;
    }
}
