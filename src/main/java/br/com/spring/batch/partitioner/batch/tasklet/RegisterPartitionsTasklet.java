package br.com.spring.batch.partitioner.batch.tasklet;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;
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
public class RegisterPartitionsTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(RegisterPartitionsTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final BlobStorage storage;
    private final BlobPaths paths;
    private final PartitionFileRepository partitions;
    private final FileLayout layout;
    private final PartitionSettings settings;

    public RegisterPartitionsTasklet(FileStepSupport support, BlobStorage storage, BlobPaths paths,
                                     PartitionFileRepository partitions, FileLayout layout,
                                     PartitionSettings settings) {
        this.support = support;
        this.storage = storage;
        this.paths = paths;
        this.partitions = partitions;
        this.layout = layout;
        this.settings = settings;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        ReceivedFileDocument original = support.load(contribution, ChaosPoint.REGISTER);
        requireAllLinesRead(contribution.getStepExecution().getJobExecution().getExecutionContext(), original);
        Map<String, BlobFile> uploaded = storage.list(paths.partitionPrefix(original)).stream()
                .collect(Collectors.toMap(BlobFile::path, Function.identity()));
        Instant now = Instant.now();
        List<ReceivedFileDocument> registered = PartitionPlan
                .split(original.lineCount(), original.partitionCount(), layout).ranges().stream()
                .map(range -> toDocument(original, range, uploaded, now))
                .toList();
        partitions.replaceAll(original.id(), registered);
        log.info("partition.register").field("fileId", original.id()).field("partitions", registered.size())
                .field("status", "UPLOADED")
                .data("files", registered.stream().map(ReceivedFileDocument::currentPath).toList())
                .log("partições enviadas ao blob registradas na received_file_management");
        return RepeatStatus.FINISHED;
    }

    private static void requireAllLinesRead(ExecutionContext job, ReceivedFileDocument original) {
        long linesRead = job.getLong(FilePartitionJob.CONTEXT_DETAIL_LINES_READ, -1L);
        if (linesRead != original.lineCount()) {
            throw new IllegalStateException("partições leram " + linesRead + " linhas de detalhe; esperado "
                    + original.lineCount());
        }
    }

    private ReceivedFileDocument toDocument(ReceivedFileDocument original, PartitionRange range,
                                            Map<String, BlobFile> uploaded, Instant now) {
        String path = paths.partitionPath(original, range.index());
        long expectedSize = range.fileSizeBytes(layout, settings.writeTrailer());
        BlobFile blob = uploaded.get(path);
        if (blob == null || blob.sizeBytes() != expectedSize) {
            throw new IllegalStateException("partição " + path + " ausente ou incompleta no blob; esperado "
                    + expectedSize + " bytes, encontrado " + (blob == null ? 0 : blob.sizeBytes()));
        }
        return original.uploadedPartition(range, paths.partitionFileName(original, range.index()), path,
                expectedSize, now);
    }
}
