package br.com.spring.batch.partitioner.batch;

import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class FileStepSupport {

    private final OriginalFileRepository repository;
    private final FileJobExecutions executions;

    public FileStepSupport(OriginalFileRepository repository, FileJobExecutions executions) {
        this.repository = repository;
        this.executions = executions;
    }

    public ReceivedFileDocument load(StepContribution contribution) {
        return repository.getById(currentFileId(contribution));
    }

    public Optional<ReceivedFileDocument> find(StepContribution contribution) {
        return repository.findById(currentFileId(contribution));
    }

    private String currentFileId(StepContribution contribution) {
        StepExecution step = contribution.getStepExecution();
        requireCurrentExecution(FilePartitionJob.fileNameOf(step.getJobExecution()), step.getJobExecutionId());
        return FilePartitionJob.fileIdOf(step);
    }

    private void requireCurrentExecution(String fileName, long jobExecutionId) {
        if (!executions.isCurrent(fileName, jobExecutionId)) {
            throw new FileOwnershipLostException(fileName, jobExecutionId);
        }
    }
}
