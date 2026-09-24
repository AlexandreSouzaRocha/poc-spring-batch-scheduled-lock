package br.com.spring.batch.partitioner.batch.step;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.chaos.ChaosTarget;
import br.com.spring.batch.partitioner.support.chaos.FailureInjector;

import org.springframework.stereotype.Component;

@Component
public class FileStepSupport {

    private final OriginalFileRepository repository;
    private final FailureInjector failureInjector;

    public FileStepSupport(OriginalFileRepository repository, FailureInjector failureInjector) {
        this.repository = repository;
        this.failureInjector = failureInjector;
    }

    public ReceivedFileDocument load(FileStep step, ChaosPoint point) {
        ReceivedFileDocument original = load(step);
        failureInjector.check(ChaosTarget.file(point, original.id(), original.attempts()));
        return original;
    }

    public ReceivedFileDocument load(FileStep step) {
        ReceivedFileDocument original = repository.getById(step.fileId());
        requireOwnership(original, step.jobExecutionId());
        return original;
    }

    public ReceivedFileDocument load(String fileId) {
        return repository.getById(fileId);
    }

    public void checkPartition(ReceivedFileDocument original, int partitionIndex) {
        failureInjector.check(ChaosTarget.partition(original.id(), original.attempts(), partitionIndex));
    }

    private static void requireOwnership(ReceivedFileDocument original, long jobExecutionId) {
        if (original.isOwnedBy(jobExecutionId)) {
            return;
        }
        throw new FileOwnershipLostException(original.id(), jobExecutionId, original.owner());
    }
}
