package br.com.spring.batch.partitioner.batch.tasklet;

import java.util.Optional;

import br.com.spring.batch.partitioner.batch.job.BatchNames;
import br.com.spring.batch.partitioner.batch.partition.PartitionCleaner;
import br.com.spring.batch.partitioner.batch.partition.PartitionCleaner.CleanupResult;
import br.com.spring.batch.partitioner.batch.step.FileStep;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import org.slf4j.event.Level;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class CleanupPartitionsTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(CleanupPartitionsTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final PartitionCleaner cleaner;
    private final JobRepository jobRepository;

    public CleanupPartitionsTasklet(FileStepSupport support, PartitionCleaner cleaner, JobRepository jobRepository) {
        this.support = support;
        this.cleaner = cleaner;
        this.jobRepository = jobRepository;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        FileStep step = new FileStep(contribution);
        ReceivedFileDocument original = support.load(step, ChaosPoint.CLEANUP);
        if (partitioningAlreadyCompleted(step)) {
            log.info("partition.cleanup").field("fileId", original.id()).field("attempt", original.attempts())
                    .field("action", "skip").log("particionamento concluído em tentativa anterior; nada a limpar");
            return RepeatStatus.FINISHED;
        }
        logCleanup(original, cleaner.clean(original));
        return RepeatStatus.FINISHED;
    }

    private boolean partitioningAlreadyCompleted(FileStep step) {
        return Optional.ofNullable(jobRepository.getLastStepExecution(step.jobInstance(),
                        BatchNames.PARTITION_MASTER_STEP))
                .map(StepExecution::getStatus)
                .filter(status -> status == BatchStatus.COMPLETED)
                .isPresent();
    }

    private static void logCleanup(ReceivedFileDocument original, CleanupResult result) {
        boolean rollback = result.removedSomething();
        String message = rollback ? "partições da tentativa anterior removidas (rollback)"
                : "nenhuma partição anterior encontrada";
        log.at(rollback ? Level.WARN : Level.INFO, "partition.cleanup")
                .field("fileId", original.id()).field("attempt", original.attempts())
                .field("action", rollback ? "rollback" : "clean").field("deletedBlobs", result.deletedBlobs())
                .field("deletedDocuments", result.deletedDocuments()).data("prefix", result.prefix())
                .log(message);
    }
}
