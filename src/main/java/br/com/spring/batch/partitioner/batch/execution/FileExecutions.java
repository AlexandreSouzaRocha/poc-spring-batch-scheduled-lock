package br.com.spring.batch.partitioner.batch.execution;

import java.util.Optional;

import br.com.spring.batch.partitioner.batch.job.BatchNames;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.stereotype.Component;

@Component
public class FileExecutions {

    private final JobRepository jobRepository;

    public FileExecutions(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public Optional<JobExecution> last(String fileName) {
        return Optional.ofNullable(jobRepository.getLastJobExecution(BatchNames.JOB_NAME,
                FileJobParameters.forFile(fileName)));
    }

    public boolean isCurrent(String fileName, long jobExecutionId) {
        return last(fileName).map(JobExecution::getId).filter(id -> id == jobExecutionId).isPresent();
    }

    public Optional<ExecutionSummary> summary(String fileName) {
        return last(fileName).map(ExecutionSummary::of);
    }
}
