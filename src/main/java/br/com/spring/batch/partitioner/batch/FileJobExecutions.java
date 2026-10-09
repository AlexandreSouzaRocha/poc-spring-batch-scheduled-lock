package br.com.spring.batch.partitioner.batch;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class FileJobExecutions {

    private final JobRepository jobRepository;

    public FileJobExecutions(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public Optional<JobExecution> last(String fileName) {
        return Optional.ofNullable(jobRepository.getLastJobExecution(FilePartitionJob.NAME,
                FilePartitionJob.parametersFor(fileName)));
    }

    public boolean isCurrent(String fileName, long jobExecutionId) {
        return last(fileName).map(JobExecution::getId).filter(id -> id == jobExecutionId).isPresent();
    }

    public boolean isCompleted(String fileName) {
        return last(fileName).map(JobExecution::getStatus).filter(status -> status == BatchStatus.COMPLETED)
                .isPresent();
    }

    public int executionCount(JobInstance jobInstance) {
        return jobRepository.getJobExecutions(jobInstance).size();
    }

    public int attempts(String fileName) {
        return Optional.ofNullable(jobRepository.getJobInstance(FilePartitionJob.NAME,
                        FilePartitionJob.parametersFor(fileName)))
                .map(this::executionCount)
                .orElse(0);
    }

    public Optional<ExecutionSummary> summary(String fileName) {
        return last(fileName).map(ExecutionSummary::of);
    }

    public boolean partitioningCompleted(JobInstance jobInstance) {
        return Optional.ofNullable(jobRepository.getLastStepExecution(jobInstance,
                        FilePartitionJob.PARTITION_MASTER_STEP))
                .map(StepExecution::getStatus)
                .filter(status -> status == BatchStatus.COMPLETED)
                .isPresent();
    }

    public record ExecutionSummary(long jobInstanceId, long jobExecutionId, String status, String exitCode,
                                   String exitDescription, LocalDateTime startTime, LocalDateTime endTime,
                                   Long durationMs) {

        static ExecutionSummary of(JobExecution execution) {
            return new ExecutionSummary(execution.getJobInstanceId(), execution.getId(),
                    execution.getStatus().name(), execution.getExitStatus().getExitCode(),
                    execution.getExitStatus().getExitDescription(), execution.getStartTime(),
                    execution.getEndTime(), durationOf(execution));
        }

        private static Long durationOf(JobExecution execution) {
            if (execution.getStartTime() == null || execution.getEndTime() == null) {
                return null;
            }
            return Duration.between(execution.getStartTime(), execution.getEndTime()).toMillis();
        }
    }
}
