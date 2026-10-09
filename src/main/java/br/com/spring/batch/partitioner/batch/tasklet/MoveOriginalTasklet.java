package br.com.spring.batch.partitioner.batch.tasklet;

import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class MoveOriginalTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(MoveOriginalTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final BlobStorage storage;
    private final BlobPaths paths;
    private final OriginalFileRepository repository;

    public MoveOriginalTasklet(FileStepSupport support, BlobStorage storage, BlobPaths paths,
                               OriginalFileRepository repository) {
        this.support = support;
        this.storage = storage;
        this.paths = paths;
        this.repository = repository;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        ReceivedFileDocument original = support.load(contribution);
        String target = paths.processedPath(original);
        if (!target.equals(original.currentPath())) {
            storage.move(original.currentPath(), target);
            repository.relocate(original.id(), target);
        }
        StepVolume.ofBytes(original.sizeBytes()).writeTo(contribution.getStepExecution().getExecutionContext());
        log.info("original.move").field("fileId", original.id()).field("source", original.currentPath())
                .field("target", target).data("sizeBytes", original.sizeBytes())
                .log("arquivo original movido para processados");
        return RepeatStatus.FINISHED;
    }
}
