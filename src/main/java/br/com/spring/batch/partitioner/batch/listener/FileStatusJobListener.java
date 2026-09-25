package br.com.spring.batch.partitioner.batch.listener;

import br.com.spring.batch.partitioner.batch.execution.FileExecutions;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.service.FileStatusService;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FileStatusJobListener implements JobExecutionListener {

    private static final StructuredLogger log = StructuredLogger.of(FileStatusJobListener.class, "file-partitioning");

    private final FileStatusService statusService;
    private final PartitionProgressReporter progressReporter;
    private final FileExecutions executions;

    public FileStatusJobListener(FileStatusService statusService, PartitionProgressReporter progressReporter,
                                 FileExecutions executions) {
        this.statusService = statusService;
        this.progressReporter = progressReporter;
        this.executions = executions;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String fileId = FileJobParameters.fileIdOf(jobExecution);
        progressReporter.finish(fileId, jobExecution.getId());
        if (!executions.isCurrent(FileJobParameters.fileNameOf(jobExecution), jobExecution.getId())) {
            log.warn("file.ownership.lost").field("fileId", fileId).field("jobExecutionId", jobExecution.getId())
                    .field("status", jobExecution.getStatus())
                    .log("outra execução assumiu o arquivo; status não alterado por esta instância");
            return;
        }
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            statusService.completed(fileId);
            return;
        }
        statusService.failed(fileId, failureOf(jobExecution), isRetryable(jobExecution));
    }

    private static String failureOf(JobExecution jobExecution) {
        return jobExecution.getAllFailureExceptions().stream()
                .findFirst()
                .map(ErrorSummary::oneLine)
                .orElse("job terminou com status " + jobExecution.getStatus());
    }

    private static boolean isRetryable(JobExecution jobExecution) {
        return jobExecution.getAllFailureExceptions().stream()
                .noneMatch(error -> ErrorSummary.rootCause(error) instanceof InvalidFileException);
    }
}
