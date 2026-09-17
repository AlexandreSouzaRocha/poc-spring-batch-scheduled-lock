package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.FileInspection;
import br.com.spring.batch.partitioner.batch.partition.FileInspector;
import br.com.spring.batch.partitioner.batch.step.FileStep;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class ValidateHeaderTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(ValidateHeaderTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final FileInspector inspector;
    private final OriginalFileRepository repository;

    public ValidateHeaderTasklet(FileStepSupport support, FileInspector inspector, OriginalFileRepository repository) {
        this.support = support;
        this.inspector = inspector;
        this.repository = repository;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        ReceivedFileDocument original = support.load(step, ChaosPoint.VALIDATE);
        FileInspection inspection = inspector.inspect(original);
        repository.recordInspection(original.id(), inspection.movement(), inspection.partitioning());
        StepVolume.ofLines(inspection.lineCount()).writeTo(step.executionContext());
        log.info("header.validate").field("fileId", original.id())
                .field("movementType", inspection.header().movementType())
                .field("movementDate", inspection.header().movementDateText())
                .field("lines", inspection.lineCount()).field("partitions", inspection.partitionCount())
                .data("sizeBytes", original.sizeBytes()).data("header", inspection.header().text())
                .log("header válido");
        return RepeatStatus.FINISHED;
    }
}
