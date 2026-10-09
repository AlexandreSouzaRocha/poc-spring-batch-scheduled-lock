package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.TrailerInfo;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class ValidateFileTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(ValidateFileTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final BlobStorage storage;
    private final OriginalFileRepository repository;
    private final FileLayout layout;
    private final PartitionSettings settings;

    public ValidateFileTasklet(FileStepSupport support, BlobStorage storage, OriginalFileRepository repository,
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
        String path = original.currentPath();
        FileHeader header = FileHeader.parse(storage.read(path, layout.headerRange()), layout);
        long detailLines = layout.detailLineCount(original.sizeBytes());
        FileTrailer trailer = FileTrailer.parse(storage.read(path, layout.trailerRange(detailLines)), layout);
        int partitionCount = PartitionPlan.effectivePartitionCount(detailLines, settings.count());
        repository.recordInspection(original.id(), MovementInfo.from(header),
                PartitioningInfo.ofOriginal(detailLines, partitionCount), TrailerInfo.from(trailer));
        recordSummary(contribution, detailLines, trailer);
        warnIfTrailerCountDiffers(original, trailer, detailLines);
        log.info("file.validate").field("fileId", original.id())
                .field("movementType", header.movementType())
                .field("movementDate", header.movementDateText())
                .field("lines", detailLines).field("partitions", partitionCount)
                .field("trailerRecordCount", trailer.recordCount())
                .field("processingDate", trailer.processingDateText())
                .data("sizeBytes", original.sizeBytes()).data("header", header.text())
                .data("trailer", trailer.text())
                .log("header e trailer válidos");
        return RepeatStatus.FINISHED;
    }

    private static void recordSummary(StepContribution contribution, long detailLines, FileTrailer trailer) {
        StepVolume.ofLines(detailLines).writeTo(contribution.getStepExecution().getExecutionContext());
        ExecutionContext job = contribution.getStepExecution().getJobExecution().getExecutionContext();
        job.putLong(FilePartitionJob.CONTEXT_DETAIL_LINES, detailLines);
        job.putLong(FilePartitionJob.CONTEXT_TRAILER_RECORD_COUNT, trailer.recordCount());
        job.putString(FilePartitionJob.CONTEXT_TRAILER_PROCESSING_DATE, trailer.processingDateText());
    }

    private static void warnIfTrailerCountDiffers(ReceivedFileDocument original, FileTrailer trailer,
                                                  long detailLines) {
        if (trailer.recordCount() == detailLines) {
            return;
        }
        log.warn("file.trailer.mismatch").field("fileId", original.id())
                .field("trailerRecordCount", trailer.recordCount()).field("lines", detailLines)
                .field("difference", trailer.recordCount() - detailLines)
                .data("fileName", original.fileName())
                .log("quantidade do trailer diferente das linhas de detalhe; o processamento segue");
    }
}
