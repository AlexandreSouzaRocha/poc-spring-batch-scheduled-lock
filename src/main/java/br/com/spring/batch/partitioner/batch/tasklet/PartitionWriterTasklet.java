package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.PartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class PartitionWriterTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(PartitionWriterTasklet.class, "partition-job");
    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final FileStepSupport support;
    private final PartitionCopy partitionCopy;
    private final PartitionProgressReporter progressReporter;
    private final BlobPaths paths;
    private final FileLayout layout;

    public PartitionWriterTasklet(FileStepSupport support, PartitionCopy partitionCopy,
                                  PartitionProgressReporter progressReporter, BlobPaths paths, FileLayout layout) {
        this.support = support;
        this.partitionCopy = partitionCopy;
        this.progressReporter = progressReporter;
        this.paths = paths;
        this.layout = layout;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        StepExecution step = contribution.getStepExecution();
        PartitionRange range = PartitionRange.from(step.getExecutionContext());
        ReceivedFileDocument original = support.load(contribution);
        ProgressCounter progress = progressReporter.track(original.id(), step.getJobExecutionId(), range.index(),
                range.bytes().length());
        String target = paths.partitionPath(original, range.index());
        long startNanos = System.nanoTime();
        long copiedBytes = partitionCopy.copy(original.currentPath(), range.bytes(), target,
                original.headerLineBytes(layout), progress);
        requireComplete(range, copiedBytes);
        long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
        support.checkPartition(original, range.index());
        long sizeBytes = range.fileSizeBytes(layout);
        new StepVolume(range.lineCount(), sizeBytes).writeTo(step.getExecutionContext());
        contribution.incrementWriteCount(range.lineCount());
        log.info("partition.upload").field("fileId", original.id()).field("partitionIndex", range.index())
                .field("lines", range.lineCount()).field("bytes", sizeBytes)
                .field("durationMs", durationMs).data("target", target)
                .data("byteRange", range.bytes()).log("partição enviada ao blob");
        return RepeatStatus.FINISHED;
    }

    private static void requireComplete(PartitionRange range, long copiedBytes) {
        if (!range.bytes().isFullyCopied(copiedBytes)) {
            throw new IllegalStateException("partição " + range.index() + " copiou " + copiedBytes
                    + " bytes; esperado " + range.bytes().length());
        }
    }
}
