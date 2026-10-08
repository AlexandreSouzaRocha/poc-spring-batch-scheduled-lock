package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobStorage;
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
    private final BlobStorage storage;
    private final OriginalFileRepository repository;
    private final FileLayout layout;
    private final PartitionSettings settings;

    public ValidateHeaderTasklet(FileStepSupport support, BlobStorage storage, OriginalFileRepository repository,
                                 FileLayout layout, PartitionSettings settings) {
        this.support = support;
        this.storage = storage;
        this.repository = repository;
        this.layout = layout;
        this.settings = settings;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        ReceivedFileDocument original = support.load(contribution, ChaosPoint.VALIDATE);
        FileHeader header = FileHeader.parse(storage.read(original.currentPath(), layout.headerRange()), layout);
        long lineCount = layout.detailLineCount(original.sizeBytes());
        int partitionCount = PartitionPlan.effectivePartitionCount(lineCount, settings.count());
        repository.recordInspection(original.id(), MovementInfo.from(header),
                PartitioningInfo.ofOriginal(lineCount, partitionCount));
        StepVolume.ofLines(lineCount).writeTo(contribution.getStepExecution().getExecutionContext());
        log.info("header.validate").field("fileId", original.id())
                .field("movementType", header.movementType())
                .field("movementDate", header.movementDateText())
                .field("lines", lineCount).field("partitions", partitionCount)
                .data("sizeBytes", original.sizeBytes()).data("header", header.text())
                .log("header válido");
        return RepeatStatus.FINISHED;
    }
}
