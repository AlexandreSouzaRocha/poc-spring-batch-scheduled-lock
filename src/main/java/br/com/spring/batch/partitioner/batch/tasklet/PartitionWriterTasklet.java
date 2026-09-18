package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.PartitionBlobWriter;
import br.com.spring.batch.partitioner.batch.partition.PartitionBlobWriter.UploadedPartition;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.batch.step.FileStep;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class PartitionWriterTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(PartitionWriterTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final PartitionBlobWriter blobWriter;
    private final PartitionProgressReporter progressReporter;

    public PartitionWriterTasklet(FileStepSupport support, PartitionBlobWriter blobWriter,
            PartitionProgressReporter progressReporter) {
        this.support = support;
        this.blobWriter = blobWriter;
        this.progressReporter = progressReporter;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        PartitionRange range = PartitionRange.from(step.executionContext());
        ReceivedFileDocument original = support.load(step.fileId());
        ProgressCounter progress = progressReporter.track(original.id(), step.jobExecutionId(), range.index(),
                range.bytes().length());
        UploadedPartition uploaded = blobWriter.upload(original, range, progress);
        support.checkPartition(original, range.index());
        step.recordVolume(new StepVolume(range.lineCount(), uploaded.sizeBytes()));
        log.info("partition.upload").field("fileId", original.id()).field("partitionIndex", range.index())
                .field("lines", range.lineCount()).field("bytes", uploaded.sizeBytes())
                .field("durationMs", uploaded.durationMs()).data("target", uploaded.path())
                .data("byteRange", range.bytes()).log("partição enviada ao blob");
        return RepeatStatus.FINISHED;
    }
}
