package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;

public enum JobOutcome {
    COMPLETED,
    FAILED,
    INVALID_FILE,
    LAUNCH_ERROR,
    CONCURRENT_LAUNCH;

    public static JobOutcome of(JobExecution execution) {
        if (execution.getStatus() == BatchStatus.COMPLETED) {
            return COMPLETED;
        }
        return execution.getAllFailureExceptions().stream().anyMatch(JobOutcome::isInvalidFile) ? INVALID_FILE : FAILED;
    }

    private static boolean isInvalidFile(Throwable error) {
        return ErrorSummary.rootCause(error) instanceof InvalidFileException;
    }
}
