package br.com.spring.batch.partitioner.batch;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.chaos.FailureInjector;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class FileStepSupport {

    private final OriginalFileRepository repository;
    private final FailureInjector failureInjector;
    private final FileJobExecutions executions;

    public FileStepSupport(OriginalFileRepository repository, FailureInjector failureInjector,
                           FileJobExecutions executions) {
        this.repository = repository;
        this.failureInjector = failureInjector;
        this.executions = executions;
    }

    public ReceivedFileDocument load(StepContribution contribution, ChaosPoint point) {
        ReceivedFileDocument original = load(contribution);
        failureInjector.check(point, original.id(), original.attempts());
        return original;
    }

    public ReceivedFileDocument load(StepContribution contribution) {
        StepExecution step = contribution.getStepExecution();
        requireCurrentExecution(FilePartitionJob.fileNameOf(step.getJobExecution()), step.getJobExecutionId());
        return repository.getById(FilePartitionJob.fileIdOf(step));
    }

    public void checkPartition(ReceivedFileDocument original, int partitionIndex) {
        failureInjector.checkPartition(original.id(), original.attempts(), partitionIndex);
    }

    private void requireCurrentExecution(String fileName, long jobExecutionId) {
        if (!executions.isCurrent(fileName, jobExecutionId)) {
            throw new FileOwnershipLostException(fileName, jobExecutionId);
        }
    }
}
