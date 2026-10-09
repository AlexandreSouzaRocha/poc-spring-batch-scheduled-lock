package br.com.spring.batch.partitioner.batch.tasklet;

import java.time.LocalDate;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.PartitionCopy;
import br.com.spring.batch.partitioner.batch.partition.PartitionCopy.CopyResult;
import br.com.spring.batch.partitioner.batch.partition.PartitionTarget;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ExecutionContext;
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
    private final PartitionSettings settings;

    public PartitionWriterTasklet(FileStepSupport support, PartitionCopy partitionCopy,
                                  PartitionProgressReporter progressReporter, BlobPaths paths, FileLayout layout,
                                  PartitionSettings settings) {
        this.support = support;
        this.partitionCopy = partitionCopy;
        this.progressReporter = progressReporter;
        this.paths = paths;
        this.layout = layout;
        this.settings = settings;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        StepExecution step = contribution.getStepExecution();
        PartitionRange range = PartitionRange.from(step.getExecutionContext());
        ReceivedFileDocument original = support.load(contribution);
        ProgressCounter progress = progressReporter.track(original.id(), step.getJobExecutionId(), range.index(),
                range.bytes().length());
        PartitionTarget target = targetOf(original, range);
        long startNanos = System.nanoTime();
        CopyResult copied = partitionCopy.copy(original.currentPath(), range.bytes(), target, progress);
        requireComplete(range, copied.copiedBytes());
        long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
        support.checkPartition(original, range.index());
        long sizeBytes = range.fileSizeBytes(layout, target.hasTrailer());
        long linesRead = copied.verifiedLines().orElse(range.lineCount());
        recordVolume(contribution, range, sizeBytes, linesRead, copied.verifiedLines().isPresent());
        log.info("partition.upload").field("fileId", original.id()).field("partitionIndex", range.index())
                .field("lines", range.lineCount()).field("linesRead", linesRead)
                .field("lineCheck", copied.verifiedLines().isPresent() ? "verified" : "skipped")
                .field("trailer", target.hasTrailer()).field("bytes", sizeBytes)
                .field("durationMs", durationMs).data("target", target.path())
                .data("byteRange", range.bytes()).log("partição enviada ao blob");
        return RepeatStatus.FINISHED;
    }

    private PartitionTarget targetOf(ReceivedFileDocument original, PartitionRange range) {
        String path = paths.partitionPath(original, range.index());
        byte[] header = original.headerLineBytes(layout);
        if (!settings.writeTrailer()) {
            return PartitionTarget.withoutTrailer(path, header);
        }
        LocalDate processingDate = LocalDate.parse(original.trailer().processingDate());
        byte[] trailer = FileTrailer.of(range.lineCount(), processingDate, layout).lineBytes(layout);
        return new PartitionTarget(path, header, trailer);
    }

    private static void recordVolume(StepContribution contribution, PartitionRange range, long sizeBytes,
                                     long linesRead, boolean verified) {
        ExecutionContext context = contribution.getStepExecution().getExecutionContext();
        new StepVolume(range.lineCount(), sizeBytes).writeTo(context);
        context.putLong(FilePartitionJob.CONTEXT_PARTITION_LINES_READ, linesRead);
        context.put(FilePartitionJob.CONTEXT_PARTITION_LINES_VERIFIED, verified);
        contribution.incrementWriteCount(linesRead);
    }

    private static void requireComplete(PartitionRange range, long copiedBytes) {
        if (!range.bytes().isFullyCopied(copiedBytes)) {
            throw new IllegalStateException("partição " + range.index() + " copiou " + copiedBytes
                    + " bytes; esperado " + range.bytes().length());
        }
    }
}
