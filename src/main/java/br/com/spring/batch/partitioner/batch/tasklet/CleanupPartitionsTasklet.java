package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.FileJobExecutions;
import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import org.slf4j.event.Level;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class CleanupPartitionsTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(CleanupPartitionsTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final FileJobExecutions executions;
    private final BlobStorage storage;
    private final BlobPaths paths;
    private final PartitionFileRepository partitions;

    public CleanupPartitionsTasklet(FileStepSupport support, FileJobExecutions executions, BlobStorage storage,
                                    BlobPaths paths, PartitionFileRepository partitions) {
        this.support = support;
        this.executions = executions;
        this.storage = storage;
        this.paths = paths;
        this.partitions = partitions;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        ReceivedFileDocument original = support.load(contribution);
        if (executions.partitioningCompleted(contribution.getStepExecution().getJobExecution().getJobInstance())) {
            log.info("partition.cleanup").field("fileId", original.id()).field("attempt", original.attempts())
                    .field("action", "skip").log("particionamento concluído em tentativa anterior; nada a limpar");
            return RepeatStatus.FINISHED;
        }
        String prefix = paths.partitionPrefix(original);
        int deletedBlobs = storage.deleteByPrefix(prefix);
        long deletedDocuments = partitions.deleteByParentFileId(original.id());
        boolean rollback = deletedBlobs > 0 || deletedDocuments > 0;
        log.at(rollback ? Level.WARN : Level.INFO, "partition.cleanup")
                .field("fileId", original.id()).field("attempt", original.attempts())
                .field("action", rollback ? "rollback" : "clean").field("deletedBlobs", deletedBlobs)
                .field("deletedDocuments", deletedDocuments).data("prefix", prefix)
                .log(rollback ? "partições da tentativa anterior removidas (rollback)"
                        : "nenhuma partição anterior encontrada");
        return RepeatStatus.FINISHED;
    }
}
