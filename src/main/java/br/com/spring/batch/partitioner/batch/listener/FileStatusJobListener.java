package br.com.spring.batch.partitioner.batch.listener;

import br.com.spring.batch.partitioner.batch.heartbeat.FileHeartbeat;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.service.FileStatusService;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FileStatusJobListener implements JobExecutionListener {

    private final FileStatusService statusService;
    private final PartitionProgressReporter progressReporter;
    private final FileHeartbeat heartbeat;

    public FileStatusJobListener(FileStatusService statusService, PartitionProgressReporter progressReporter,
                                 FileHeartbeat heartbeat) {
        this.statusService = statusService;
        this.progressReporter = progressReporter;
        this.heartbeat = heartbeat;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String fileId = FileJobParameters.fileIdOf(jobExecution);
        statusService.started(fileId, jobExecution.getJobInstanceId(), jobExecution.getId());
        heartbeat.start(fileId, jobExecution.getId());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String fileId = FileJobParameters.fileIdOf(jobExecution);
        heartbeat.stop(fileId);
        progressReporter.finish(fileId, jobExecution.getId());
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            statusService.completed(fileId, jobExecution.getId(), durationMs(jobExecution));
            return;
        }
        statusService.failed(fileId, jobExecution.getId(), failureOf(jobExecution), isRetryable(jobExecution));
    }

    private static long durationMs(JobExecution jobExecution) {
        return Throughput.between(jobExecution.getStartTime(), jobExecution.getEndTime(), StepVolume.empty())
                .durationMs();
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
