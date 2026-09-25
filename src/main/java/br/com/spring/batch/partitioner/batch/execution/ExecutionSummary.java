package br.com.spring.batch.partitioner.batch.execution;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.batch.core.job.JobExecution;

public record ExecutionSummary(long jobInstanceId, long jobExecutionId, String status, String exitCode,
                               String exitDescription, LocalDateTime startTime, LocalDateTime endTime,
                               Long durationMs) {

    static ExecutionSummary of(JobExecution execution) {
        return new ExecutionSummary(execution.getJobInstanceId(), execution.getId(), execution.getStatus().name(),
                execution.getExitStatus().getExitCode(), execution.getExitStatus().getExitDescription(),
                execution.getStartTime(), execution.getEndTime(), durationOf(execution));
    }

    private static Long durationOf(JobExecution execution) {
        return Optional.ofNullable(execution.getStartTime())
                .flatMap(start -> Optional.ofNullable(execution.getEndTime())
                        .map(end -> Duration.between(start, end).toMillis()))
                .orElse(null);
    }
}
