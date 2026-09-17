package br.com.spring.batch.partitioner.batch.tasklet;

import java.time.Instant;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.partition.PartitionBlobWriter;
import br.com.spring.batch.partitioner.batch.step.FileStep;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument.WrittenBlob;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
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
    private final PartitionFileRepository repository;

    public PartitionWriterTasklet(FileStepSupport support, PartitionBlobWriter blobWriter,
                                  PartitionFileRepository repository) {
        this.support = support;
        this.blobWriter = blobWriter;
        this.repository = repository;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        PartitionRange range = PartitionRange.from(step.executionContext());
        ReceivedFileDocument original = support.load(step.fileId());
        WrittenBlob written = blobWriter.write(original, range);
        support.checkPartition(original, range.index());
        repository.save(original.partition(range, written, Instant.now()));
        step.recordVolume(new StepVolume(range.lineCount(), written.sizeBytes()));
        log.info("partition.write").field("fileId", original.id()).field("partitionIndex", range.index())
                .field("lines", range.lineCount()).field("bytes", written.sizeBytes())
                .field("durationMs", written.durationMs()).data("target", written.path())
                .data("byteRange", range.bytes()).log("partição gravada no blob");
        return RepeatStatus.FINISHED;
    }
}
